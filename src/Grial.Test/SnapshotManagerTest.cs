namespace Grial.Test;

using Core.Clocks;
using Core.Storage;
using Core.WAL;
using System.Text;
using Core.KV;
using Core.KV.Leases;

public class SnapshotManagerTest
{
    [Test]
    public async Task Snapshot_Create_Then_Load_Restores_All_Records()
    {
        using var dir = new TempDir();

        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("nodeA");
        var changeLog = new ChangeLog(wal, clock);

        var store = new ReplicatedKvStore(clock, changeLog, new KvWatchManager());

        var rec1 = new ChangeRecord(0,
            new HybridTimestamp(1000, 0, "nodeA"),
            ChangeRecordOperation.SET,
            "alpha"u8.ToArray(),
            "111"u8.ToArray());

        var rec2 = new ChangeRecord(0,
            new HybridTimestamp(2000, 2, "nodeA"),
            ChangeRecordOperation.SET,
            "beta"u8.ToArray(),
            "222"u8.ToArray());

        store.Apply(rec1);
        store.Apply(rec2);
        changeLog.Append(rec1);
        changeLog.Append(rec2);

        var snapshot = new SnapshotManager(store, changeLog, wal, new LeaseSnapshotBackend(new LeaseManager(TimeProvider.System), new LeaseKeyIndex()));
        await snapshot.CreateSnapshotAsync();

        var store2 = new ReplicatedKvStore(clock, changeLog, new KvWatchManager());
        var snapshot2 = new SnapshotManager(store2, changeLog, wal, new LeaseSnapshotBackend(new LeaseManager(TimeProvider.System), new LeaseKeyIndex()));

        var loaded = snapshot2.TryLoad();
        Assert.That(loaded, Is.True);

        var items = store2.ExportAll();
        Assert.That(items.Length, Is.EqualTo(2));

        var alpha = items.First(x => Encoding.UTF8.GetString(x.Key.Span) == "alpha");
        Assert.That(Encoding.UTF8.GetString(alpha.Value!.Value.Span),
            Is.EqualTo("111"));
        Assert.That(alpha.Timestamp.PhysicalMillis, Is.EqualTo(1000));

        var beta = items.First(x => Encoding.UTF8.GetString(x.Key.Span) == "beta");
        Assert.That(Encoding.UTF8.GetString(beta.Value!.Value.Span),
            Is.EqualTo("222"));
        Assert.That(beta.Timestamp.PhysicalMillis, Is.EqualTo(2000));
    }

    [Test]
    public void Snapshot_TryLoad_Fails_When_No_Files()
    {
        using var dir = new TempDir();

        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("nodeA");
        var changeLog = new ChangeLog(wal, clock);

        var store = new ReplicatedKvStore(clock, changeLog, new KvWatchManager());
        var snapshot = new SnapshotManager(store, changeLog, wal, new LeaseSnapshotBackend(new LeaseManager(TimeProvider.System), new LeaseKeyIndex()));

        Assert.That(snapshot.TryLoad(), Is.False);
    }

    [Test]
    public void Snapshot_Fails_On_Broken_Meta()
    {
        using var dir = new TempDir();

        File.WriteAllText(Path.Combine(dir.Path, "snapshot.meta.json"), "{broken");
        File.WriteAllBytes(Path.Combine(dir.Path, "snapshot.cbor"), new byte[] { 0x82, 0x01, 0x02 });

        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("nodeA");
        var changeLog = new ChangeLog(wal, clock);

        var store = new ReplicatedKvStore(clock, changeLog, new KvWatchManager());
        var snapshot = new SnapshotManager(store, changeLog, wal, new LeaseSnapshotBackend(new LeaseManager(TimeProvider.System), new LeaseKeyIndex()));

        Assert.That(snapshot.TryLoad(), Is.False);
    }
}