namespace Grial.Core.Clocks;

internal readonly struct HybridTimestampBinary(
    long physicalMillis,
    int logicalCounter,
    ReadOnlyMemory<byte> nodeIdBytes)
{
    public readonly long PhysicalMillis = physicalMillis;
    public readonly int LogicalCounter = logicalCounter;
    public readonly ushort NodeIdLength = (ushort)nodeIdBytes.Length;
    public readonly ReadOnlyMemory<byte> NodeIdBytes = nodeIdBytes;
}