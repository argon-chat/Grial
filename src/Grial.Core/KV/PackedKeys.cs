namespace Grial.Core.KV;

using System.Buffers.Binary;

public readonly struct PackedKeys(ReadOnlyMemory<byte> buffer)
{
    public ReadOnlyMemory<byte> Buffer => buffer;

    public int Count
    {
        get
        {
            var span = buffer.Span;
            return BinaryPrimitives.ReadInt32LittleEndian(span);
        }
    }

    public ReadOnlySpan<byte> GetKey(int index)
    {
        var span = buffer.Span;

        var count = BinaryPrimitives.ReadInt32LittleEndian(span);
        if ((uint)index >= (uint)count) throw new ArgumentOutOfRangeException(nameof(index));

        const int offsetTableStart = 4;
        var keyStartOffset = offsetTableStart + index * 4;

        var start = BinaryPrimitives.ReadInt32LittleEndian(span[keyStartOffset..]);

        var end = index + 1 == count
            ? span.Length
            : BinaryPrimitives.ReadInt32LittleEndian(span[(keyStartOffset + 4)..]);

        return span.Slice(start, end - start);
    }

    public static PackedKeys Pack(IReadOnlyList<ReadOnlyMemory<byte>> keys)
    {
        var count = keys.Count;
        var totalBytes = 4 + 4 * count + keys.Sum(k => k.Length); // count + offsets

        var buffer = new byte[totalBytes];
        var span = buffer.AsSpan();

        BinaryPrimitives.WriteInt32LittleEndian(span, count);

        var offsetTableStart = 4;
        var dataStart = offsetTableStart + count * 4;
        var current = dataStart;

        for (var i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(
                span[(offsetTableStart + i * 4)..],
                current);

            var keyBytes = keys[i].Span;
            keyBytes.CopyTo(span[current..]);
            current += keyBytes.Length;
        }

        return new PackedKeys(buffer);
    }
}