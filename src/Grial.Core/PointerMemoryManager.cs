namespace Grial.Core;

using System.Buffers;

public sealed unsafe class PointerMemoryManager(byte* pointer, int length) : MemoryManager<byte>
{
    private bool _disposed;

    public override Span<byte> GetSpan()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PointerMemoryManager));

        return new Span<byte>(pointer, length);
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PointerMemoryManager));

        if ((uint)elementIndex >= length)
            throw new ArgumentOutOfRangeException(nameof(elementIndex));

        return new MemoryHandle(pointer + elementIndex);
    }

    public override void Unpin()
    {
    }

    protected override void Dispose(bool disposing) => _disposed = true;
}