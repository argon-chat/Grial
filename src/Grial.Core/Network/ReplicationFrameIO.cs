namespace Grial.Core.Network;

using System.Buffers;
using System.Buffers.Binary;
using System.Formats.Cbor;

public static class ReplicationFrameIO
{
    public static async ValueTask WriteFrameAsync(
        Stream stream,
        Action<CborWriter> encode,
        CancellationToken cancellationToken = default)
    {
        var writer = new CborWriter();
        encode(writer);
        var payload = writer.Encode();

        Span<byte> prefix = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(prefix, payload.Length);

        // TODO ToArray
        await stream.WriteAsync(prefix.ToArray(), cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async ValueTask<byte[]?> ReadFrameAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        var prefixBuf = new byte[4];
        var read = 0;
        while (read < 4)
        {
            var r = await stream.ReadAsync(prefixBuf.AsMemory(read, 4 - read), cancellationToken)
                .ConfigureAwait(false);
            if (r == 0)
                return read == 0 ? null : throw new EndOfStreamException("Unexpected EOF while reading frame length");

            read += r;
        }

        var length = BinaryPrimitives.ReadInt32BigEndian(prefixBuf);
        if (length <= 0)
            throw new InvalidDataException($"Invalid frame length {length}");

        var payload = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var offset = 0;
            while (offset < length)
            {
                var r = await stream.ReadAsync(payload.AsMemory(offset, length - offset), cancellationToken)
                    .ConfigureAwait(false);
                if (r == 0)
                    throw new EndOfStreamException("Unexpected EOF while reading frame payload");
                offset += r;
            }

            var result = new byte[length];
            Buffer.BlockCopy(payload, 0, result, 0, length);
            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payload);
        }
    }
}