namespace Grial.Test;

using Core.KV.Leases;
using NUnit.Framework.Legacy;

[TestFixture]
public sealed class LeaseSnapshotBackendTests
{
    [Test]
    public void Export_And_Restore_Leases()
    {
        var lm = new LeaseManager(TimeProvider.System);
        var idx = new LeaseKeyIndex();

        var backend = new LeaseSnapshotBackend(lm, idx);

        var now = TimeProvider.System.GetUtcNow().ToUnixTimeMilliseconds();

        var lease1 = lm.CreateLeaseAt(now, TimeSpan.FromSeconds(1));
        var lease2 = lm.CreateLeaseAt(now, TimeSpan.FromSeconds(2));

        idx.AttachKey(lease1.Id, "a1"u8);
        idx.AttachKey(lease1.Id, "a2"u8);
        idx.AttachKey(lease2.Id, "b"u8);

        var exported = backend.ExportLeasesForSnapshot();
        Assert.That(exported.Length, Is.EqualTo(2));

        var lm2 = new LeaseManager(TimeProvider.System);
        var idx2 = new LeaseKeyIndex();
        var backend2 = new LeaseSnapshotBackend(lm2, idx2);

        backend2.RestoreLeasesFromSnapshot(exported);

        // lease1 restored?
        Assert.That(lm2.TryGet(lease1.Id, out var restored1), Is.True);
        Assert.That(restored1.Ttl, Is.EqualTo(lease1.Ttl));

        // key binding restored?
        var keys1 = idx2.TakeKeysForLease(lease1.Id);
        CollectionAssert.AreEquivalent(
            new[] { "a1", "a2" },
            keys1.Select(x => (string)x));
    }
}