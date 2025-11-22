namespace Grial.Core.Clocks;

using System.Formats.Cbor;

public static class HybridTimestampCodec
{
    public static void Write(ref CborWriter writer, in HybridTimestamp ts)
    {
        writer.WriteStartArray(3);

        writer.WriteInt64(ts.PhysicalMillis);
        writer.WriteInt32(ts.LogicalCounter);
        writer.WriteTextString(ts.NodeId);

        writer.WriteEndArray();
    }

    public static HybridTimestamp Read(ref CborReader reader)
    {
        var len = reader.ReadStartArray();

        var physical = reader.ReadInt64();
        var logical = reader.ReadInt32();
        var nodeId = reader.ReadTextString();

        reader.ReadEndArray();

        return new HybridTimestamp(physical, logical, nodeId);
    }
}