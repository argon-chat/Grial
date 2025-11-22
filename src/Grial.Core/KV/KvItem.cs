namespace Grial.Core.KV;

using Clocks;

public readonly struct KvItem(
    Utf8Key key,
    ReadOnlyMemory<byte>? value,
    HybridTimestamp timestamp,
    long revision
)
{
    /// <summary>
    /// The key of the item.
    /// </summary>
    public readonly Utf8Key Key = key;


    /// <summary>
    /// The value (null => tombstone).
    /// </summary>
    public readonly ReadOnlyMemory<byte>? Value = value;


    /// <summary>
    /// Hybrid timestamp used for conflict resolution.
    /// </summary>
    public readonly HybridTimestamp Timestamp = timestamp;


    /// <summary>
    /// Local revision (version).
    /// </summary>
    public readonly long Revision = revision;


    /// <summary>
    /// Indicates that the value represents a tombstone.
    /// </summary>
    public bool IsTombstone => !Value.HasValue;
}