namespace Grial.Test;

using Core.Clocks;
using Core.Network;
using Core.WAL;
using System.Formats.Cbor;

public class ReplicationCodecTests
{
    private static byte[] Encode(Action<CborWriter> encode)
    {
        var writer = new CborWriter();
        encode(writer);
        return writer.Encode();
    }

    private static T Decode<T>(byte[] bytes, Func<CborReader, T> decode)
    {
        var reader = new CborReader(bytes, CborConformanceMode.Strict);
        return decode(reader);
    }

    [Test]
    public void Hello_Roundtrip_Works()
    {
        var msg = new HelloMessage(
            ClusterId: "clusterA",
            NodeId: "node-1",
            RegionId: "ru-east",
            ProtocolVersion: 1,
            Options: new Dictionary<string, string>
            {
                ["k1"] = "v1",
                ["k2"] = "v2"
            });

        var bytes = Encode(w => ReplicationCodec.EncodeHello(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeHello(ref r));

        Assert.That(parsed.ClusterId, Is.EqualTo("clusterA"));
        Assert.That(parsed.NodeId, Is.EqualTo("node-1"));
        Assert.That(parsed.RegionId, Is.EqualTo("ru-east"));
        Assert.That(parsed.ProtocolVersion, Is.EqualTo(1));
        Assert.That(parsed.Options!.Count, Is.EqualTo(2));
        Assert.That(parsed.Options["k1"], Is.EqualTo("v1"));
    }

    [Test]
    public void Follow_Roundtrip_Works()
    {
        var msg = new FollowMessage(
            FromRevision: 999,
            StreamId: "stream-x",
            Options: new Dictionary<string, string>
            {
                ["sync"] = "fast"
            });

        var bytes = Encode(w => ReplicationCodec.EncodeFollow(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeFollow(ref r));

        Assert.That(parsed.FromRevision, Is.EqualTo(999));
        Assert.That(parsed.StreamId, Is.EqualTo("stream-x"));
        Assert.That(parsed.Options!["sync"], Is.EqualTo("fast"));
    }


    [Test]
    public void FollowAccept_Roundtrip_Works()
    {
        var msg = new FollowAcceptMessage(
            StreamId: "st1",
            EffectiveFromRevision: 100,
            RequireSnapshot: true);

        var bytes = Encode(w => ReplicationCodec.EncodeFollowAccept(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeFollowAccept(ref r));

        Assert.That(parsed.StreamId, Is.EqualTo("st1"));
        Assert.That(parsed.EffectiveFromRevision, Is.EqualTo(100));
        Assert.That(parsed.RequireSnapshot, Is.True);
    }

    [Test]
    public void SnapshotStart_Roundtrip_Works()
    {
        var msg = new SnapshotStartMessage(
            StreamId: "s1",
            SnapshotId: 7,
            LastRevision: 99,
            TotalSizeBytes: 4096,
            ChunkSizeBytes: 512);

        var bytes = Encode(w => ReplicationCodec.EncodeSnapshotStart(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeSnapshotStart(ref r));

        Assert.That(parsed.SnapshotId, Is.EqualTo(7));
        Assert.That(parsed.TotalSizeBytes, Is.EqualTo(4096));
    }

    [Test]
    public void SnapshotChunk_Roundtrip_Works()
    {
        var chunk = new byte[] { 1, 2, 3, 4 };

        var msg = new SnapshotChunkMessage(
            StreamId: "s1",
            SnapshotId: 10,
            ChunkIndex: 3,
            Data: chunk);

        var bytes = Encode(w => ReplicationCodec.EncodeSnapshotChunk(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeSnapshotChunk(ref r));

        Assert.That(parsed.SnapshotId, Is.EqualTo(10));
        Assert.That(parsed.ChunkIndex, Is.EqualTo(3));
        Assert.That(parsed.Data.ToArray(), Is.EqualTo(chunk));
    }

    [Test]
    public void SnapshotEnd_Roundtrip_Works()
    {
        var msg = new SnapshotEndMessage("s1", 11);

        var bytes = Encode(w => ReplicationCodec.EncodeSnapshotEnd(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeSnapshotEnd(ref r));

        Assert.That(parsed.SnapshotId, Is.EqualTo(11));
    }

    [Test]
    public void WalBatch_Roundtrip_Works()
    {
        var ts = new HybridTimestamp(1000, 0, "n1");

        var rec1 = new ChangeRecord(
            1,
            ts,
            ChangeRecordOperation.SET,
            "alpha"u8.ToArray(),
            "111"u8.ToArray());

        var rec2 = new ChangeRecord(
            2,
            ts,
            ChangeRecordOperation.DEL,
            "beta"u8.ToArray(),
            null);

        var msg = new WalBatchMessage(
            StreamId: "st",
            BaseRevision: 1,
            LastRevision: 2,
            Records: [rec1, rec2]);

        var bytes = Encode(w => ReplicationCodec.EncodeWalBatch(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeWalBatch(ref r));

        Assert.That(parsed.Records.Length, Is.EqualTo(2));
        Assert.That(parsed.Records[0].Revision, Is.EqualTo(1));
        Assert.That(parsed.Records[1].Op, Is.EqualTo(ChangeRecordOperation.DEL));
    }


    [Test]
    public void Heartbeat_Roundtrip_Works()
    {
        var msg = new HeartbeatMessage("s2", 900);

        var bytes = Encode(w => ReplicationCodec.EncodeHeartbeat(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeHeartbeat(ref r));

        Assert.That(parsed.LastRevision, Is.EqualTo(900));
    }

    [Test]
    public void Ack_Roundtrip_Works()
    {
        var msg = new AckMessage("s1", AppliedRevision: 50, MaxInFlight: 2);

        var bytes = Encode(w => ReplicationCodec.EncodeAck(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeAck(ref r));

        Assert.That(parsed.AppliedRevision, Is.EqualTo(50));
        Assert.That(parsed.MaxInFlight, Is.EqualTo(2));
    }

    [Test]
    public void Error_Roundtrip_Works()
    {
        var msg = new ErrorMessage("stream-x", ReplicationErrorCode.WalGapTooLarge, "gap!");

        var bytes = Encode(w => ReplicationCodec.EncodeError(ref w, msg));
        var parsed = Decode(bytes, r => ReplicationCodec.DecodeError(ref r));

        Assert.That(parsed.Code, Is.EqualTo(ReplicationErrorCode.WalGapTooLarge));
        Assert.That(parsed.Message, Is.EqualTo("gap!"));
    }

    [Test]
    public void PeekMessageType_Works()
    {
        var msg = new HeartbeatMessage("s1", 123);

        var bytes = Encode(w => ReplicationCodec.EncodeHeartbeat(ref w, msg));
        var type = ReplicationCodec.PeekMessageType(bytes);

        Assert.That(type, Is.EqualTo(ReplicationMessageType.Heartbeat));
    }
}