namespace Grial.Core.KV;

using System.Buffers;
using System.Formats.Cbor;
using Storage;
using WAL;

public struct PrefixReplayHandler(ReadOnlySpan<byte> prefix, long fromRevision) : IWalEntryHandler, ILogEntryHandler
{
    private readonly byte[] prefix = prefix.ToArray();

    private ArrayBufferWriter<KvWatchEvent>? buffer = null;

    public void OnEntry(long seq, ReadOnlyMemory<byte> payload)
    {
        if (seq <= fromRevision)
            return;

        var reader = new CborReader(payload, CborConformanceMode.Strict);
        var rec = ChangeRecordCbor.Read(ref reader);

        if (!rec.Key.Span.StartsWith(prefix))
            return;

        buffer ??= new ArrayBufferWriter<KvWatchEvent>(8);

        var kind = rec.Op == ChangeRecordOperation.SET
            ? KvWatchEventKind.Set
            : KvWatchEventKind.Delete;

        var span = buffer.GetSpan(1);
        span[0] = new KvWatchEvent(
            kind,
            rec.Key,
            rec.Value,
            rec.Timestamp,
            rec.Revision);

        buffer.Advance(1);
    }

    public ReadOnlySpan<KvWatchEvent> Events
        => buffer is null ? ReadOnlySpan<KvWatchEvent>.Empty : buffer.WrittenSpan;
}