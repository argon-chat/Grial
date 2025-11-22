namespace Grial.Core.KV;

using Clocks;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using WAL;

public class ReplicatedKvStore(
    HybridLogicalClock clock,
    ChangeLog log,
    KvWatchManager watchManager,
    ILogger<ReplicatedKvStore>? logger = null)
{
    private readonly ConcurrentDictionary<Utf8Key, KvItem> map = new();
    private readonly RadixKeyIndex prefixIndex = new();
    private readonly Lock gate = new();

    public void Apply(in ChangeRecord rec)
    {
        var changed = rec.Op == ChangeRecordOperation.SET
            ? ApplySet(rec, rec.Key)
            : ApplyDelete(rec, rec.Key);

        if (changed)
            watchManager.Publish(in rec);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ApplySet(in ChangeRecord rec, Utf8Key key)
    {
        var changed = false;

        if (!map.TryGetValue(key, out var existing) ||
            rec.Timestamp > existing.Timestamp)
        {
            map[key] = new KvItem(key, rec.Value, rec.Timestamp, rec.Revision);
            changed = true;
        }

        prefixIndex.Add(key);

        logger?.LogDebug(
            "Apply SET: rev={Revision}, ts={Timestamp:o}, keyLen={KeyLen}",
            rec.Revision,
            rec.Timestamp,
            key.Length);

        return changed;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ApplyDelete(in ChangeRecord rec, Utf8Key key)
    {
        var changed = false;

        if (!map.TryGetValue(key, out var existing) ||
            rec.Timestamp > existing.Timestamp)
        {
            map[key] = new KvItem(key, null, rec.Timestamp, rec.Revision);
            changed = true;
        }

        prefixIndex.Remove(key);

        logger?.LogDebug(
            "Apply DELETE: rev={Revision}, ts={Timestamp:o}, keyLen={KeyLen}",
            rec.Revision,
            rec.Timestamp,
            key.Length);

        return changed;
    }


    public bool TryGet(Utf8Key key, out ReadOnlyMemory<byte> value)
    {
        if (map.TryGetValue(key, out var entry) && entry.Value.HasValue)
        {
            value = entry.Value.Value;
            return true;
        }

        value = default;
        return false;
    }

    public bool TryGetKvItem(Utf8Key key, out KvItem value)
    {
        if (map.TryGetValue(key, out var entry) && entry.Value.HasValue)
        {
            value = entry;
            return true;
        }

        value = default;
        return false;
    }

    public bool TryGetItem(Utf8Key key, out KvItem item)
    {
        if (map.TryGetValue(key, out var entry))
        {
            item = entry;
            return true;
        }

        item = default;
        return false;
    }

    public KvItem[] ExportAll()
    {
        var result = new List<KvItem>(map.Count);

        foreach (var kv in map) result.Add(kv.Value);

        return result.ToArray();
    }

    /// <summary>
    /// A prefix query 
    /// </summary>
    public KvItem[] GetByPrefix(Utf8Key prefix)
    {
        logger?.LogDebug("PrefixQuery: prefixLen={Len}", prefix.Length);

        if (prefix.Length == 0)
            return ExportAll();

        var collector = new PrefixCollector(this);
        prefixIndex.VisitByPrefix(prefix, collector);
        return collector.Build();
    }

    public bool TryCompareAndSet(
        Utf8Key key,
        long expectedRevision,
        ReadOnlySpan<byte> newValue,
        out long appliedRevision)
    {
        appliedRevision = 0;

        lock (gate)
        {
            var has = map.TryGetValue(key, out var current);

            if (!has)
            {
                if (expectedRevision != 0)
                {
                    logger?.LogDebug(
                        "CAS SET rejected: expected={Expected}, actual=missing",
                        expectedRevision);
                    return false;
                }
            }
            else if (current.Revision != expectedRevision)
            {
                logger?.LogDebug(
                    "CAS SET rejected: expected={Expected}, actual={Actual}",
                    expectedRevision,
                    current.Revision);
                return false;
            }

            var ts = clock.NextLocal();

            var rec = log.Append(new ChangeRecord(
                0,
                ts,
                ChangeRecordOperation.SET,
                key.Clone(),
                newValue.ToArray()));

            map[key] = new KvItem(rec.Key, rec.Value, rec.Timestamp, rec.Revision);
            appliedRevision = rec.Revision;

            logger?.LogInformation(
                "CAS SET applied: expected={Expected}, newRev={NewRev}",
                expectedRevision,
                rec.Revision);

            watchManager?.Publish(in rec);

            return true;
        }
    }

    /// <summary>
    /// CAS-delete by revision.
    /// expectedRevision:
    /// 0 → we expect that there is no key (or it is tombstone) and we set tombstone anyway
    /// > 0 → we expect that the key exists with this revision
    ///
    /// Returns true if delete was applied, and out applicedrevision = new revision (from WAL).
    /// Returns false if the condition was not met (nothing was changed).
    /// </summary>
    public bool TryCompareAndDelete(
        Utf8Key key,
        long expectedRevision,
        out long appliedRevision)
    {
        appliedRevision = 0;

        lock (gate)
        {
            var has = map.TryGetValue(key, out var current);

            if (!has)
            {
                if (expectedRevision != 0)
                {
                    logger?.LogDebug(
                        "CAS DELETE rejected: expected={Expected}, actual=missing",
                        expectedRevision);
                    return false;
                }
            }
            else if (current.Revision != expectedRevision)
            {
                logger?.LogDebug(
                    "CAS DELETE rejected: expected={Expected}, actual={Actual}",
                    expectedRevision,
                    current.Revision);
                return false;
            }

            var ts = clock.NextLocal();

            var rec = log.Append(new ChangeRecord(
                0,
                ts,
                ChangeRecordOperation.DEL,
                key.Clone(),
                null));

            Apply(rec);

            appliedRevision = rec.Revision;

            logger?.LogInformation(
                "CAS DELETE applied: expected={Expected}, newRev={NewRev}",
                expectedRevision,
                rec.Revision);

            return true;
        }
    }

    private sealed class PrefixCollector(ReplicatedKvStore store) : IKeyVisitor
    {
        private readonly List<KvItem> items = new();

        public void OnKey(Utf8Key key)
        {
            if (!store.map.TryGetValue(key, out var entry)) return;
            if (!entry.IsTombstone) items.Add(entry);
        }

        public KvItem[] Build()
            => items.ToArray();
    }

    private readonly record struct KeyWrapper
    {
        private readonly byte[] keyBytes;

        public KeyWrapper(ReadOnlyMemory<byte> mem) => keyBytes = mem.ToArray();
        public KeyWrapper(ReadOnlySpan<byte> span) => keyBytes = span.ToArray();

        public ReadOnlyMemory<byte> AsMemory() => keyBytes;

        public bool Equals(KeyWrapper other)
            => keyBytes.AsSpan().SequenceEqual(other.keyBytes);

        public override int GetHashCode()
            => ComputeHash(keyBytes);

        private static int ComputeHash(byte[] data)
        {
            const uint fnvOffset = 2166136261;
            const uint fnvPrime = 16777619;

            var hash = fnvOffset;
            foreach (var t in data)
            {
                hash ^= t;
                hash *= fnvPrime;
            }

            return (int)hash;
        }
    }
}