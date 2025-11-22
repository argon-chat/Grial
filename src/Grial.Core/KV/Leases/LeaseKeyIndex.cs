namespace Grial.Core.KV.Leases;

public sealed class LeaseKeyIndex
{
    private readonly Lock sync = new();
    private readonly Dictionary<long, HashSet<Utf8Key>> map = new();

    /// <summary>
    /// Attaches a key to a lease and stores it in the index.
    /// </summary>
    public void AttachKey(LeaseId leaseId, Utf8Key key)
    {
        lock (sync)
        {
            if (!map.TryGetValue(leaseId.Value, out var set))
            {
                set = [];
                map[leaseId.Value] = set;
            }

            set.Add(key);
        }
    }

    /// <summary>
    /// Detaches a key from a lease. If the lease has no remaining keys, it is removed.
    /// </summary>
    public void DetachKey(LeaseId leaseId, Utf8Key key)
    {
        lock (sync)
        {
            if (!map.TryGetValue(leaseId.Value, out var set))
                return;

            set.Remove(key);

            if (set.Count == 0)
                map.Remove(leaseId.Value);
        }
    }

    /// <summary>
    /// Returns all keys assigned to the lease and removes the lease from the index.
    /// </summary>
    public Utf8Key[] TakeKeysForLease(LeaseId leaseId)
    {
        lock (sync)
        {
            if (!map.TryGetValue(leaseId.Value, out var set) || set.Count == 0)
                return [];

            map.Remove(leaseId.Value);

            var result = new Utf8Key[set.Count];
            var i = 0;
            foreach (var k in set)
            {
                result[i++] = k;
            }

            return result;
        }
    }

    /// <summary>
    /// Exports the entire lease-to-keys mapping as a snapshot.
    /// Used for persistence or replication.
    /// </summary>
    public LeaseSnapshotEntry[] ExportSnapshot()
    {
        lock (sync)
        {
            if (map.Count == 0)
                return [];

            var result = new LeaseSnapshotEntry[map.Count];
            var i = 0;

            foreach (var (leaseIdValue, keysSet) in map)
            {
                var keyList = new List<ReadOnlyMemory<byte>>(keysSet.Count);
                foreach (var keyWrapper in keysSet)
                    keyList.Add(keyWrapper.Memory);

                var packed = PackedKeys.Pack(keyList);

                var leaseId = new LeaseId(leaseIdValue);
                var dummyLease = new LeaseEntry(leaseId, 0, TimeSpan.Zero);

                result[i++] = new LeaseSnapshotEntry(dummyLease, packed);
            }

            if (i == result.Length)
                return result;

            Array.Resize(ref result, i);
            return result;
        }
    }

    /// <summary>
    /// Restores keys for leases from a previously exported snapshot.
    /// Clears any existing in-memory index.
    /// </summary>
    public void RestoreSnapshotKeys(ReadOnlySpan<LeaseSnapshotEntry> entries)
    {
        lock (sync)
        {
            map.Clear();

            foreach (var entry in entries)
            {
                if (!map.TryGetValue(entry.Lease.Id.Value, out var set))
                {
                    set = [];
                    map[entry.Lease.Id.Value] = set;
                }

                var packed = entry.Keys;

                for (var i = 0; i < packed.Count; i++)
                {
                    var keySpan = packed.GetKey(i);
                    set.Add(new Utf8Key(keySpan));
                }
            }
        }
    }
}