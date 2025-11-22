namespace Grial.Test;

using Core.Clocks;
using Core.KV;
using Core.Storage;
using Core.WAL;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using System;
using System.IO;
using System.Text;
using Core.KV.Leases;

[TestFixture]
public sealed class SnapshotManagerWithLeasesTests
{
    sealed class TempDir : IDisposable
    {
        public string Path { get; }

        public TempDir()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "snap-lease-test-" + Guid.NewGuid());
            Directory.CreateDirectory(Path);
        }

        public void Dispose()
        {
            try { Directory.Delete(Path, true); } catch { }
        }
    }

    [Test]
    public void Snapshot_Save_And_Load_With_Leases()
    {
        using var dir = new TempDir();

        var wal = new SegmentedLogStorage(dir.Path, 1024);
        var clock = new HybridLogicalClock("nodeA");
        var changeLog = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, changeLog, new KvWatchManager());

        var lm = new LeaseManager(TimeProvider.System);
        var idx = new LeaseKeyIndex();
        var backend = new LeaseSnapshotBackend(lm, idx);

        var snapManager = new SnapshotManager(kv, changeLog, wal, backend);

        var lease = lm.CreateLease(TimeSpan.FromSeconds(5));
        idx.AttachKey(lease.Id, "alpha"u8);
        idx.AttachKey(lease.Id, "beta"u8);

        var rec1 = MakeSet(clock, "alpha", "111");
        var rec2 = MakeSet(clock, "beta", "222");

        changeLog.Append(rec1);
        changeLog.Append(rec2);

        kv.Apply(rec1);
        kv.Apply(rec2);

        snapManager.CreateSnapshotAsync().AsTask().Wait();

        var kv2 = new ReplicatedKvStore(clock, changeLog, new KvWatchManager());
        var lm2 = new LeaseManager(TimeProvider.System);
        var idx2 = new LeaseKeyIndex();
        var backend2 = new LeaseSnapshotBackend(lm2, idx2);

        var snapManager2 = new SnapshotManager(kv2, changeLog, wal, backend2);

        var loaded = snapManager2.TryLoad();
        Assert.That(loaded, Is.True);

        Assert.That(kv2.TryGet("alpha"u8, out var v1), Is.True);
        Assert.That(v1.ToArray(), Is.EqualTo("111"u8.ToArray()));

        Assert.That(kv2.TryGet("beta"u8, out var v2), Is.True);
        Assert.That(v2.ToArray(), Is.EqualTo("222"u8.ToArray()));

        Assert.That(lm2.LeaseCount, Is.EqualTo(1));
        Assert.That(lm2.TryGet(lease.Id, out var restoredLease), Is.True);

        var keys = idx2.TakeKeysForLease(lease.Id);
        CollectionAssert.AreEquivalent(
            new[] { "alpha", "beta" },
            keys.Select(x => (string)x));
    }

    static ChangeRecord MakeSet(HybridLogicalClock clock, string key, string value)
    {
        var ts = clock.NextLocal();
        return new ChangeRecord(0,
            ts,
            ChangeRecordOperation.SET,
            Encoding.UTF8.GetBytes(key),
            Encoding.UTF8.GetBytes(value));
    }
}