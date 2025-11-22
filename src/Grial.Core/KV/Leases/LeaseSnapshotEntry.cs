namespace Grial.Core.KV.Leases;

using Grial.Core.KV;

public readonly struct LeaseSnapshotEntry(LeaseEntry lease, PackedKeys keys)
{
    public LeaseEntry Lease { get; } = lease;
    public PackedKeys Keys { get; } = keys;
}

public interface ILeaseSnapshotBackend
{
    LeaseSnapshotEntry[] ExportLeasesForSnapshot();
    void RestoreLeasesFromSnapshot(ReadOnlySpan<LeaseSnapshotEntry> entries);
}