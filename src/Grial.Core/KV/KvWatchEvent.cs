namespace Grial.Core.KV;

using Clocks;

public enum KvWatchEventKind
{
    Set,
    Delete
}

public readonly struct KvWatchEvent(
    KvWatchEventKind kind,
    Utf8Key key,
    ReadOnlyMemory<byte>? value,
    HybridTimestamp timestamp,
    long rv)
{
    public readonly KvWatchEventKind Kind = kind;
    public readonly Utf8Key Key = key;
    public readonly ReadOnlyMemory<byte>? Value = value;
    public readonly HybridTimestamp Timestamp = timestamp;
    public readonly long Revision = rv;

    public bool IsDelete => Kind == KvWatchEventKind.Delete;
}