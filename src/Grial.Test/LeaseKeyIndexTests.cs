namespace Grial.Test;

using Core.KV.Leases;
using NUnit.Framework.Legacy;

[TestFixture]
public sealed class LeaseKeyIndexTests
{
    [Test]
    public void Attach_And_Detach_Works()
    {
        var index = new LeaseKeyIndex();
        var lid = new LeaseId(1);

        var key1 = new byte[] { 1, 2, 3 };
        var key2 = new byte[] { 4, 5, 6 };

        index.AttachKey(lid, key1);
        index.AttachKey(lid, key2);

        var arr = index.TakeKeysForLease(lid);
        Assert.That(arr.Length, Is.EqualTo(2));

        CollectionAssert.AreEquivalent(
            new[] { key1, key2 },
            arr.Select(x => (string)x));
    }

    [Test]
    public void Snapshot_And_Restore_Works()
    {
        var index = new LeaseKeyIndex();
        var lid1 = new LeaseId(1);
        var lid2 = new LeaseId(2);

        index.AttachKey(lid1, "foo"u8);
        index.AttachKey(lid1, "bar"u8);
        index.AttachKey(lid2, "baz"u8);

        var snap = index.ExportSnapshot();
        Assert.That(snap.Length, Is.EqualTo(2));

        var index2 = new LeaseKeyIndex();
        index2.RestoreSnapshotKeys(snap);

        var keys1 = index2.TakeKeysForLease(lid1);
        var keys2 = index2.TakeKeysForLease(lid2);

        CollectionAssert.AreEquivalent(
            new[] { "foo", "bar" },
            keys1.Select(x => (string)x));

        CollectionAssert.AreEquivalent(
            new[] { "baz" },
            keys2.Select(x => (string)x));
    }
}