namespace Grial.Core.Clocks;

public static class HybridTimestampConverter
{
    internal static HybridTimestampBinary ToBinary(in HybridTimestamp src)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(src.NodeId);
        return new HybridTimestampBinary(src.PhysicalMillis, src.LogicalCounter, bytes);
    }

    internal static HybridTimestamp FromBinary(in HybridTimestampBinary bin)
    {
        var nodeId = System.Text.Encoding.UTF8.GetString(
            bin.NodeIdBytes.Span);

        return new HybridTimestamp(
            bin.PhysicalMillis,
            bin.LogicalCounter,
            nodeId);
    }
}