namespace Grial.Core.Network;

using WAL;
using System.Formats.Cbor;

public class ReplicationCodec
{
    public static void EncodeHello(ref CborWriter writer, in HelloMessage msg)
    {
        writer.WriteStartArray(6);
        writer.WriteInt32((int)ReplicationMessageType.Ping);
        writer.WriteTextString(msg.ClusterId);
        writer.WriteTextString(msg.NodeId);
        writer.WriteTextString(msg.RegionId);
        writer.WriteInt32(msg.ProtocolVersion);

        if (msg.Options is null || msg.Options.Count == 0)
        {
            writer.WriteStartMap(0);
            writer.WriteEndMap();
        }
        else
        {
            writer.WriteStartMap(msg.Options.Count);
            foreach (var kv in msg.Options)
            {
                writer.WriteTextString(kv.Key);
                writer.WriteTextString(kv.Value);
            }

            writer.WriteEndMap();
        }

        writer.WriteEndArray();
    }

    public static HelloMessage DecodeHello(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 6)
            throw new InvalidOperationException($"Hello array length {len} != 6");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.Ping)
            throw new InvalidOperationException($"Invalid msg type {type} for Hello");

        var clusterId = reader.ReadTextString();
        var nodeId = reader.ReadTextString();
        var regionId = reader.ReadTextString();
        var proto = reader.ReadInt32();

        var mapLen = reader.ReadStartMap();
        var dict = new Dictionary<string, string>();
        for (int i = 0; i < mapLen; i++)
        {
            var k = reader.ReadTextString();
            var v = reader.ReadTextString();
            dict[k] = v;
        }

        reader.ReadEndMap();

        reader.ReadEndArray();

        return new HelloMessage(clusterId, nodeId, regionId, proto, dict);
    }

    public static void EncodeHelloAck(ref CborWriter writer, in HelloAckMessage msg)
    {
        writer.WriteStartArray(5);
        writer.WriteInt32((int)ReplicationMessageType.Pong);
        writer.WriteInt32(msg.ProtocolVersion);
        writer.WriteTextString(msg.NodeId);
        writer.WriteInt64(msg.LastRevision);

        if (msg.Info is null)
        {
            writer.WriteNull();
        }
        else
        {
            writer.WriteStartMap(msg.Info.Count);
            foreach (var kv in msg.Info)
            {
                writer.WriteTextString(kv.Key);
                writer.WriteTextString(kv.Value);
            }

            writer.WriteEndMap();
        }

        writer.WriteEndArray();
    }

    public static HelloAckMessage DecodeHelloAck(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 5)
            throw new InvalidOperationException($"HelloAck length={len}, expected 5");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.Pong)
            throw new InvalidOperationException($"Invalid message type {type}");

        var proto = reader.ReadInt32();
        var nodeId = reader.ReadTextString();
        var lastRev = reader.ReadInt64();

        Dictionary<string, string>? info = null;

        if (reader.PeekState() == CborReaderState.Null)
            reader.ReadNull();
        else
        {
            var mapLen = reader.ReadStartMap();
            info = new Dictionary<string, string>();

            for (var i = 0; i < mapLen; i++)
            {
                var k = reader.ReadTextString();
                var v = reader.ReadTextString();
                info[k] = v;
            }

            reader.ReadEndMap();
        }

        reader.ReadEndArray();

        return new HelloAckMessage(proto, nodeId, lastRev, info);
    }

    public static void EncodeFollow(ref CborWriter writer, in FollowMessage msg)
    {
        writer.WriteStartArray(4);

        writer.WriteInt32((int)ReplicationMessageType.Follow);
        writer.WriteInt64(msg.FromRevision);
        writer.WriteTextString(msg.StreamId);

        if (msg.Options is null)
        {
            writer.WriteStartMap(0);
            writer.WriteEndMap();
        }
        else
        {
            writer.WriteStartMap(msg.Options.Count);
            foreach (var kv in msg.Options)
            {
                writer.WriteTextString(kv.Key);
                writer.WriteTextString(kv.Value);
            }

            writer.WriteEndMap();
        }

        writer.WriteEndArray();
    }

    public static FollowMessage DecodeFollow(ref CborReader reader)
    {
        var len = reader.ReadStartArray();

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.Follow)
            throw new InvalidOperationException("Not Follow");

        var from = reader.ReadInt64();
        var streamId = reader.ReadTextString();

        var mapLen = reader.ReadStartMap();
        var dict = new Dictionary<string, string>();
        for (var i = 0; i < mapLen; i++)
        {
            var k = reader.ReadTextString();
            var v = reader.ReadTextString();
            dict[k] = v;
        }

        reader.ReadEndMap();

        reader.ReadEndArray();

        return new FollowMessage(from, streamId, dict);
    }

    public static void EncodeFollowAccept(ref CborWriter writer, in FollowAcceptMessage msg)
    {
        writer.WriteStartArray(4);
        writer.WriteInt32((int)ReplicationMessageType.FollowAccept);
        writer.WriteTextString(msg.StreamId);
        writer.WriteInt64(msg.EffectiveFromRevision);
        writer.WriteBoolean(msg.RequireSnapshot);
        writer.WriteEndArray();
    }

    public static FollowAcceptMessage DecodeFollowAccept(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 4)
            throw new InvalidOperationException($"FollowAccept array length {len} != 4");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.FollowAccept)
            throw new InvalidOperationException($"Invalid msg type {type} for FollowAccept");

        var streamId = reader.ReadTextString();
        var effFrom = reader.ReadInt64();
        var reqSnap = reader.ReadBoolean();
        reader.ReadEndArray();

        return new FollowAcceptMessage(streamId, effFrom, reqSnap);
    }

    public static void EncodeSnapshotStart(ref CborWriter writer, in SnapshotStartMessage msg)
    {
        writer.WriteStartArray(6);
        writer.WriteInt32((int)ReplicationMessageType.SnapshotStart);
        writer.WriteTextString(msg.StreamId);
        writer.WriteInt64(msg.SnapshotId);
        writer.WriteInt64(msg.LastRevision);
        writer.WriteInt64(msg.TotalSizeBytes);
        writer.WriteInt32(msg.ChunkSizeBytes);
        writer.WriteEndArray();
    }

    public static SnapshotStartMessage DecodeSnapshotStart(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 6)
            throw new InvalidOperationException($"SnapshotStart array length {len} != 6");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.SnapshotStart)
            throw new InvalidOperationException($"Invalid msg type {type} for SnapshotStart");

        var streamId = reader.ReadTextString();
        var snapId = reader.ReadInt64();
        var lastRev = reader.ReadInt64();
        var total = reader.ReadInt64();
        var chunk = reader.ReadInt32();

        reader.ReadEndArray();

        return new SnapshotStartMessage(streamId, snapId, lastRev, total, chunk);
    }
    public static void EncodeSnapshotChunk(ref CborWriter writer, in SnapshotChunkMessage msg)
    {
        writer.WriteStartArray(5);
        writer.WriteInt32((int)ReplicationMessageType.SnapshotChunk);
        writer.WriteTextString(msg.StreamId);
        writer.WriteInt64(msg.SnapshotId);
        writer.WriteInt32(msg.ChunkIndex);
        writer.WriteByteString(msg.Data.Span);
        writer.WriteEndArray();
    }

    public static SnapshotChunkMessage DecodeSnapshotChunk(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 5)
            throw new InvalidOperationException($"SnapshotChunk array length {len} != 5");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.SnapshotChunk)
            throw new InvalidOperationException($"Invalid msg type {type} for SnapshotChunk");

        var streamId = reader.ReadTextString();
        var snapId = reader.ReadInt64();
        var chunkIndex = reader.ReadInt32();
        var data = reader.ReadByteString();

        reader.ReadEndArray();

        return new SnapshotChunkMessage(streamId, snapId, chunkIndex, data);
    }

    public static void EncodeSnapshotEnd(ref CborWriter writer, in SnapshotEndMessage msg)
    {
        writer.WriteStartArray(3);
        writer.WriteInt32((int)ReplicationMessageType.SnapshotEnd);
        writer.WriteTextString(msg.StreamId);
        writer.WriteInt64(msg.SnapshotId);
        writer.WriteEndArray();
    }

    public static SnapshotEndMessage DecodeSnapshotEnd(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 3)
            throw new InvalidOperationException($"SnapshotEnd array length {len} != 3");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.SnapshotEnd)
            throw new InvalidOperationException($"Invalid msg type {type} for SnapshotEnd");

        var streamId = reader.ReadTextString();
        var snapId = reader.ReadInt64();

        reader.ReadEndArray();

        return new SnapshotEndMessage(streamId, snapId);
    }

    public static void EncodeWalBatch(ref CborWriter writer, in WalBatchMessage msg)
    {
        writer.WriteStartArray(5);
        writer.WriteInt32((int)ReplicationMessageType.WalBatch);
        writer.WriteTextString(msg.StreamId);
        writer.WriteInt64(msg.BaseRevision);
        writer.WriteInt64(msg.LastRevision);

        // [ ChangeRecord* ]
        var records = msg.Records;
        writer.WriteStartArray(records.Length);
        foreach (var t in records)
        {
            ChangeRecordCbor.Write(ref writer, in t);
        }

        writer.WriteEndArray();

        writer.WriteEndArray();
    }

    public static WalBatchMessage DecodeWalBatch(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 5)
            throw new InvalidOperationException($"WalBatch array length {len} != 5");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.WalBatch)
            throw new InvalidOperationException($"Invalid msg type {type} for WalBatch");

        var streamId = reader.ReadTextString();
        var baseRev = reader.ReadInt64();
        var lastRev = reader.ReadInt64();

        var arrLen = reader.ReadStartArray();
        var records = arrLen == null
            ? Array.Empty<ChangeRecord>()
            : new ChangeRecord[arrLen.Value];

        if (arrLen is > 0)
        {
            for (int i = 0; i < arrLen.Value; i++)
            {
                var rec = ChangeRecordCbor.Read(ref reader);
                records[i] = rec;
            }
        }

        if (arrLen is not null)
            reader.ReadEndArray();

        reader.ReadEndArray();

        return new WalBatchMessage(streamId, baseRev, lastRev, records);
    }

    public static void EncodeHeartbeat(ref CborWriter writer, in HeartbeatMessage msg)
    {
        writer.WriteStartArray(3);
        writer.WriteInt32((int)ReplicationMessageType.Heartbeat);
        writer.WriteTextString(msg.StreamId);
        writer.WriteInt64(msg.LastRevision);
        writer.WriteEndArray();
    }

    public static HeartbeatMessage DecodeHeartbeat(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 3)
            throw new InvalidOperationException($"Heartbeat array length {len} != 3");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.Heartbeat)
            throw new InvalidOperationException($"Invalid msg type {type} for Heartbeat");

        var streamId = reader.ReadTextString();
        var lastRev = reader.ReadInt64();

        reader.ReadEndArray();
        return new HeartbeatMessage(streamId, lastRev);
    }

    public static void EncodeAck(ref CborWriter writer, in AckMessage msg)
    {
        writer.WriteStartArray(4);
        writer.WriteInt32((int)ReplicationMessageType.Ack);
        writer.WriteTextString(msg.StreamId);
        writer.WriteInt64(msg.AppliedRevision);
        writer.WriteInt32(msg.MaxInFlight);
        writer.WriteEndArray();
    }

    public static AckMessage DecodeAck(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 4)
            throw new InvalidOperationException($"Ack array length {len} != 4");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.Ack)
            throw new InvalidOperationException($"Invalid msg type {type} for Ack");

        var streamId = reader.ReadTextString();
        var applied = reader.ReadInt64();
        var maxInFlight = reader.ReadInt32();

        reader.ReadEndArray();
        return new AckMessage(streamId, applied, maxInFlight);
    }

    public static void EncodeError(ref CborWriter writer, in ErrorMessage msg)
    {
        writer.WriteStartArray(4);
        writer.WriteInt32((int)ReplicationMessageType.Error);
        writer.WriteTextString(msg.StreamId);
        writer.WriteInt32((int)msg.Code);
        writer.WriteTextString(msg.Message);
        writer.WriteEndArray();
    }

    public static ErrorMessage DecodeError(ref CborReader reader)
    {
        var len = reader.ReadStartArray();
        if (len != 4)
            throw new InvalidOperationException($"Error array length {len} != 4");

        var type = reader.ReadInt32();
        if (type != (int)ReplicationMessageType.Error)
            throw new InvalidOperationException($"Invalid msg type {type} for Error");

        var streamId = reader.ReadTextString();
        var code = (ReplicationErrorCode)reader.ReadInt32();
        var msg = reader.ReadTextString();

        reader.ReadEndArray();
        return new ErrorMessage(streamId, code, msg);
    }

    public static ReplicationMessageType PeekMessageType(ReadOnlyMemory<byte> cborPayload)
    {
        var reader = new CborReader(cborPayload);
        var len = reader.ReadStartArray();
        if (len <= 0)
            throw new InvalidOperationException("Empty CBOR array for replication message");

        var type = reader.ReadInt32();
        return (ReplicationMessageType)type;
    }

    public static unsafe ReplicationMessageType PeekMessageType(ReadOnlySpan<byte> cborPayload)
    {
        fixed (byte* pointer = &cborPayload.GetPinnableReference())
        {
            using var memoryManager = new PointerMemoryManager(pointer, cborPayload.Length);
            return PeekMessageType(memoryManager.Memory);
        }
    }
}