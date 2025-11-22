namespace Grial.Core.WAL;

using Clocks;
using Grial.Core.KV;
using Storage;
using System.Formats.Cbor;

public readonly struct ChangeRecord(
    long rv,
    HybridTimestamp ts,
    ChangeRecordOperation op,
    Utf8Key key,
    ReadOnlyMemory<byte>? value)
{
    public readonly long Revision = rv;
    public readonly HybridTimestamp Timestamp = ts;
    public readonly ChangeRecordOperation Op = op;
    public readonly Utf8Key Key = key;
    public readonly ReadOnlyMemory<byte>? Value = value;
}

public enum ChangeRecordOperation
{
    SET = 1,
    DEL = 2,
}

public readonly struct ApplyHandler(ReplicatedKvStore store) : ILogEntryHandler
{
    public void OnEntry(long seq, ReadOnlyMemory<byte> payload)
    {
        var reader = new CborReader(payload);
        var record = ChangeRecordCbor.Read(ref reader);

        store.Apply(record);
    }
}
public static class ChangeRecordCbor
{
    public static void Write(ref CborWriter writer, in ChangeRecord rec)
    {
        // [ revision, timestamp.phys, timestamp.logical, timestamp.nodeId, op, key, value ]
        writer.WriteStartArray(7);

        writer.WriteInt64(rec.Revision);
        writer.WriteInt64(rec.Timestamp.PhysicalMillis);
        writer.WriteInt32(rec.Timestamp.LogicalCounter);
        writer.WriteTextString(rec.Timestamp.NodeId);

        writer.WriteInt32((int)rec.Op);

        writer.WriteByteString(rec.Key.Span);

        if (rec.Value.HasValue)
            writer.WriteByteString(rec.Value.Value.Span);
        else
            writer.WriteNull();

        writer.WriteEndArray();
    }

    public static ChangeRecord Read(ref CborReader reader)
    {
        var len = reader.ReadStartArray();

        var revision = reader.ReadInt64();

        var phys = reader.ReadInt64();
        var logical = reader.ReadInt32();
        var node = reader.ReadTextString();

        var op = (ChangeRecordOperation)reader.ReadInt32();

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

        reader.ReadEndArray();

        return new ChangeRecord(
            revision,
            new HybridTimestamp(phys, logical, node),
            op,
            key,
            value
        );
    }
}