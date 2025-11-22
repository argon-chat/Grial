namespace Grial.Core.KV.Leases;

public sealed class LeaseSnapshotBackend(LeaseManager leaseManager, LeaseKeyIndex leaseKeyIndex) : ILeaseSnapshotBackend
{
    public LeaseSnapshotEntry[] ExportLeasesForSnapshot()
    {
        var now = TimeProvider.System.GetUtcNow().ToUnixTimeMilliseconds();
        var activeLeases = leaseManager.ExportAllActive(now);
        if (activeLeases is { Length: 0 })
            return [];

        var keyEntries = leaseKeyIndex.ExportSnapshot();
        if (keyEntries is { Length: 0 })
            return [];

        var dict = new Dictionary<long, PackedKeys>();

        foreach (var ke in keyEntries)
            dict[ke.Lease.Id.Value] = ke.Keys;

        var result = new List<LeaseSnapshotEntry>(activeLeases.Length);

        foreach (ref readonly var lease in activeLeases.AsSpan())
        {
            if (!dict.TryGetValue(lease.Id.Value, out var packed) || packed is { Count: 0 })
                continue;

            result.Add(new(lease, packed));
        }

        return result.ToArray();
    }

    public void RestoreLeasesFromSnapshot(ReadOnlySpan<LeaseSnapshotEntry> entries)
    {
        if (entries.IsEmpty)
            return;

        var leases = new LeaseEntry[entries.Length];
        for (var i = 0; i < entries.Length; i++)
            leases[i] = entries[i].Lease;

        leaseManager.RestoreFromSnapshot(leases);

        leaseKeyIndex.RestoreSnapshotKeys(entries);
    }
}