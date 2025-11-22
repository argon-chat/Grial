namespace Grial.Core.Network;

using KV;
using WAL;
using System.Formats.Cbor;

public sealed class ReplicationServer(ReplicatedKvStore store, ChangeLog log)
{
    public async ValueTask ServeAsync(IReplicationStream conn, CancellationToken ct = default)
    {
        await using var rpc = new ReplicationConnection(conn.Input, conn.Output);

        var hello = await rpc.ReceiveHelloAsync(ct);

        var ack = new HelloAckMessage(
            hello.ProtocolVersion,
            "srv-1",
            log.LastRevision,
            null);

        await rpc.SendHelloAckAsync(ack, ct);

        var follow = await rpc.ReceiveFollowAsync(ct);
        var clientRev = follow.FromRevision;

        if (clientRev < log.LastRevision)
            await SendSnapshotAsync(rpc, ct);

        await StreamWalAsync(rpc, clientRev, ct);
    }

    private async ValueTask SendSnapshotAsync(ReplicationConnection rpc, CancellationToken ct)
    {
        var items = store.ExportAll();
        var writer = new CborWriter();

        writer.WriteStartArray(items.Length);
        foreach (ref readonly var e in items.AsSpan())
        {
            // [ key, value/null, phys, logical, nodeId ]
            writer.WriteStartArray(5);

            writer.WriteByteString(e.Key.Span);

            if (e.Value.HasValue)
                writer.WriteByteString(e.Value.Value.Span);
            else
                writer.WriteNull();

            writer.WriteInt64(e.Timestamp.PhysicalMillis);
            writer.WriteInt32(e.Timestamp.LogicalCounter);
            writer.WriteTextString(e.Timestamp.NodeId);

            writer.WriteEndArray();
        }

        writer.WriteEndArray();

        var snapshotBytes = writer.Encode();
        var totalSize = snapshotBytes.Length;

        const int chunkSize = 64 * 1024;

        var snapshotId = 1L;
        var start = new SnapshotStartMessage(
            StreamId: "stream-1",
            SnapshotId: snapshotId,
            LastRevision: log.LastRevision,
            TotalSizeBytes: totalSize,
            ChunkSizeBytes: chunkSize);

        await rpc.SendSnapshotStartAsync(start, ct).ConfigureAwait(false);

        var offset = 0;
        var chunkIndex = 0;

        while (offset < totalSize)
        {
            var size = Math.Min(chunkSize, totalSize - offset);

            var chunk = new SnapshotChunkMessage(
                StreamId: "stream-1",
                SnapshotId: snapshotId,
                ChunkIndex: chunkIndex++,
                Data: snapshotBytes.AsMemory(offset, size));

            await rpc.SendSnapshotChunkAsync(chunk, ct).ConfigureAwait(false);

            offset += size;
        }

        var end = new SnapshotEndMessage(
            StreamId: "stream-1",
            SnapshotId: snapshotId);

        await rpc.SendSnapshotEndAsync(end, ct).ConfigureAwait(false);
    }

    private async ValueTask StreamWalAsync(ReplicationConnection rpc, long fromRev, CancellationToken ct)
    {
        var lastSent = fromRev;

        while (!ct.IsCancellationRequested)
        {
            var batch = CollectWalBatch(ref lastSent);

            if (batch.Records.Length > 0)
            {
                await rpc.SendWalBatchAsync(batch, ct);
            }

            await Task.Delay(10, ct);
        }
    }

    private WalBatchMessage CollectWalBatch(ref long lastRev)
    {
        var records = log.ReadAfter(lastRev);
        if (records.Length == 0)
            return new WalBatchMessage("stream-1", lastRev, lastRev, []);

        var newLast = records[^1].Revision;
        var batch = new WalBatchMessage("stream-1", lastRev, newLast, records);
        lastRev = newLast;
        return batch;
    }
}