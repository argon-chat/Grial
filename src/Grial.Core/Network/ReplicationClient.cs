namespace Grial.Core.Network;

using Clocks;
using KV;
using WAL;
using System.Formats.Cbor;

public sealed class ReplicationClient(ReplicatedKvStore store, ChangeLog log)
{
    public async ValueTask ConnectAsync(IReplicationStream conn, CancellationToken ct = default)
    {
        await using var rpc = new ReplicationConnection(conn.Input, conn.Output, leaveOpen: true);

        var hello = new HelloMessage(
            ClusterId: "cluster-x",
            NodeId: "node-c1",
            RegionId: "ru",
            ProtocolVersion: 1,
            Options: null);

        await rpc.SendHelloAsync(hello, ct);

        var ack = await rpc.ReceiveHelloAckAsync(ct);

        var myRev = log.LastRevision;
        var serverRev = ack.LastRevision;

        await rpc.SendFollowAsync(
            new FollowMessage(myRev, "stream-1", null),
            ct);

        if (myRev < serverRev)
            await ReceiveSnapshotAsync(rpc, ct);

        await ReceiveWalAsync(rpc, ct);
    }

    async ValueTask ReceiveSnapshotAsync(ReplicationConnection rpc, CancellationToken ct)
    {
        var start = await rpc.ReceiveSnapshotStartAsync(ct).ConfigureAwait(false);

        if (start.TotalSizeBytes <= 0 || start.TotalSizeBytes > int.MaxValue)
            throw new InvalidDataException($"Invalid snapshot size: {start.TotalSizeBytes}");

        var totalSize = (int)start.TotalSizeBytes;
        var buffer = new byte[totalSize];
        var offset = 0;

        while (offset < totalSize)
        {
            var chunk = await rpc.ReceiveSnapshotChunkAsync(ct).ConfigureAwait(false);

            if (chunk.SnapshotId != start.SnapshotId)
                throw new InvalidDataException("SnapshotId mismatch in chunk");

            var span = chunk.Data.Span;
            if (offset + span.Length > totalSize)
                throw new InvalidDataException("Snapshot chunks exceed declared size");

            span.CopyTo(buffer.AsSpan(offset));
            offset += span.Length;
        }

        var end = await rpc.ReceiveSnapshotEndAsync(ct).ConfigureAwait(false);
        if (end.SnapshotId != start.SnapshotId)
            throw new InvalidDataException("SnapshotId mismatch in end");

        var reader = new CborReader(buffer, CborConformanceMode.Strict);

        var entriesLen = reader.ReadStartArray();

        for (var i = 0; i < entriesLen; i++)
        {
            var innerLen = reader.ReadStartArray();

            var key = new Utf8Key(reader.ReadByteString());
            ReadOnlyMemory<byte>? value;

            if (reader.PeekState() == CborReaderState.Null)
            {
                reader.ReadNull();
                value = null;
            }
            else
            {
                value = reader.ReadByteString();
            }

            var physical = reader.ReadInt64();
            var logical = reader.ReadInt32();
            var nodeId = reader.ReadTextString();

            reader.ReadEndArray(); // end of entry

            var ts = new HybridTimestamp(physical, logical, nodeId);
            var op = value.HasValue ? ChangeRecordOperation.SET : ChangeRecordOperation.DEL;

            var rec = new ChangeRecord(
                0,
                ts,
                op,
                key,
                value);

            store.Apply(rec);
        }

        reader.ReadEndArray();

        // TODO Do not forget to set the local revision to the snapshot state changeLog.InitializeLastRevision(start.LastRevision);
    }

    public async ValueTask ReceiveWalAsync(ReplicationConnection rpc, CancellationToken ct)
    {
        var frame = await ReplicationFrameIO.ReadFrameAsync(rpc.Input, ct)
            .ConfigureAwait(false);

        if (frame is null)
            return;

        var type = ReplicationCodec.PeekMessageType(frame);

        var reader = new CborReader(frame, CborConformanceMode.Strict);

        switch (type)
        {
            case ReplicationMessageType.WalBatch:
            {
                var batch = ReplicationCodec.DecodeWalBatch(ref reader);
                var records = batch.Records ?? [];

                foreach (var t in records) store.Apply(t);

                break;
            }

            case ReplicationMessageType.Heartbeat:
            {
                var hb = ReplicationCodec.DecodeHeartbeat(ref reader);
                break;
            }

            default:
                throw new InvalidDataException($"Unexpected replication message: {type}");
        }
    }
}