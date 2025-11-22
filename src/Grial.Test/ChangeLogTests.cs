namespace Grial.Test;

using Core.Clocks;
using Core.KV;
using Core.Storage;
using Core.WAL;
using Newtonsoft.Json.Linq;
using NUnit.Framework.Legacy;
using System.Text;
using Core.KV.Leases;

[TestFixture]
public class ChangeLogTests
{
    [Test]
    public void Revisions_Are_Monotonic()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        var r1 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "a", "1"u8.ToArray()));
        var r2 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "b", "2"u8.ToArray()));
        var r3 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "c", "3"u8.ToArray()));

        Assert.That(r1.Revision, Is.EqualTo(1));
        Assert.That(r2.Revision, Is.EqualTo(2));
        Assert.That(r3.Revision, Is.EqualTo(3));
    }

    [Test]
    public void ScanFromRevision_EmptyLog_ReturnsNothing()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        var handler = new PrefixReplayHandler("a"u8, fromRevision: 0);
        log.ScanFromRevision(0, ref handler);

        Assert.That(handler.Events.Length, Is.EqualTo(0));
    }


    [Test]
    public void ScanFromRevision_FutureRevision_YieldsNothing()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x", "1"u8.ToArray()));

        var handler = new PrefixReplayHandler("x"u8, fromRevision: 1000);
        log.ScanFromRevision(1000, ref handler);

        Assert.That(handler.Events.Length, Is.EqualTo(0));
    }

    [Test]
    public void ScanFromRevision_Zero_ReturnsAll()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        var r1 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x1", "A"u8.ToArray()));
        var r2 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x2", "B"u8.ToArray()));
        var r3 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x3", "C"u8.ToArray()));

        var handler = new PrefixReplayHandler("x"u8, fromRevision: 0);
        log.ScanFromRevision(0, ref handler);

        var evs = handler.Events;
        Assert.That(evs.Length, Is.EqualTo(3));
        Assert.That(evs[0].Revision, Is.EqualTo(r1.Revision));
        Assert.That(evs[1].Revision, Is.EqualTo(r2.Revision));
        Assert.That(evs[2].Revision, Is.EqualTo(r3.Revision));
    }

    [Test]
    public void ScanFromRevision_Prefix_FiltersCorrectly()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "foo1", "A"u8.ToArray()));
        var start = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "bar1",
            "B"u8.ToArray()));
        log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "foo2", "C"u8.ToArray()));

        var handler = new PrefixReplayHandler("foo"u8, start.Revision);
        log.ScanFromRevision(start.Revision, ref handler);

        var evs = handler.Events;
        Assert.That(evs.Length, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(evs[0].Key.Span), Is.EqualTo("foo2"));
    }

    [Test]
    public void ScanFromRevision_Includes_Deletes()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        var r1 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x", "1"u8.ToArray()));
        var r2 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.DEL, "x", null));

        var handler = new PrefixReplayHandler("x"u8, fromRevision: r1.Revision);
        log.ScanFromRevision(r1.Revision, ref handler);

        var evs = handler.Events;
        Assert.That(evs.Length, Is.EqualTo(1));
        Assert.That(evs[0].Kind, Is.EqualTo(KvWatchEventKind.Delete));
    }

    [Test]
    public void ScanFromRevision_MultipleUpdatesOnSameKey()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        var r1 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "k", "1"u8.ToArray()));
        var r2 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "k", "2"u8.ToArray()));
        var r3 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.DEL, "k", null));

        var handler = new PrefixReplayHandler("k"u8, fromRevision: r1.Revision);
        log.ScanFromRevision(r1.Revision, ref handler);

        var evs = handler.Events;

        Assert.That(evs.Length, Is.EqualTo(2));
        Assert.That(evs[0].Revision, Is.EqualTo(r2.Revision));
        Assert.That(evs[1].Revision, Is.EqualTo(r3.Revision));
    }

    [Test]
    public void ScanFromRevision_SkipsOlder_AndReadsNewer()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x1", "A"u8.ToArray()));
        var middle =
            log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x2", "B"u8.ToArray()));
        log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x3", "C"u8.ToArray()));

        var handler = new PrefixReplayHandler("x"u8, fromRevision: middle.Revision);
        log.ScanFromRevision(middle.Revision, ref handler);

        Assert.That(handler.Events.Length, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(handler.Events[0].Key.Span), Is.EqualTo("x3"));
    }

    [Test]
    public void ScanFromRevision_DifferentPrefixes()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);

        log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "foo1", "A"u8.ToArray()));
        log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "foa", "B"u8.ToArray()));
        var r = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "fax", "C"u8.ToArray()));

        var handler = new PrefixReplayHandler("fo"u8, fromRevision: r.Revision - 3);
        log.ScanFromRevision(r.Revision - 3, ref handler);

        var keys = handler.Events.ToArray().Select(x => Encoding.UTF8.GetString(x.Key.Span)).ToArray();
        CollectionAssert.Contains(keys, "foo1");
        CollectionAssert.Contains(keys, "foa");
        CollectionAssert.DoesNotContain(keys, "fax");
    }

    [Test]
    public async Task Replay_Works_After_Snapshot()
    {
        using var dir = new TempDir();

        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var wch = new KvWatchManager();
        var store = new ReplicatedKvStore(clock, log, wch);
        var snapshot = new SnapshotManager(store, log, wal,
            new LeaseSnapshotBackend(new LeaseManager(TimeProvider.System), new LeaseKeyIndex()));

        var r1 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x1", "A"u8.ToArray()));
        var r2 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x2", "B"u8.ToArray()));

        await snapshot.CreateSnapshotAsync();

        var r3 = log.Append(new ChangeRecord(0, clock.NextLocal(), ChangeRecordOperation.SET, "x3", "C"u8.ToArray()));

        var handler = new PrefixReplayHandler("x"u8, fromRevision: r2.Revision);
        log.ScanFromRevision(r2.Revision, ref handler);

        Assert.That(handler.Events.Length, Is.EqualTo(1));
        Assert.That(handler.Events[0].Revision, Is.EqualTo(r3.Revision));
    }

    [Test]
    public void ChangeLog_Revisions_Increase_Monotonically()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("nodeA");
        var log = new ChangeLog(wal, clock);

        var rec1 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "a",
            "1"u8.ToArray());

        var rec2 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "b",
            "2"u8.ToArray());

        var rec3 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "c",
            "3"u8.ToArray());

        var r1 = log.Append(rec1);
        var r2 = log.Append(rec2);
        var r3 = log.Append(rec3);

        Assert.That(r1.Revision, Is.EqualTo(1));
        Assert.That(r2.Revision, Is.EqualTo(2));
        Assert.That(r3.Revision, Is.EqualTo(3));
        Assert.That(log.LastRevision, Is.EqualTo(3));
    }

    [Test]
    public void ScanFromRevision_Returns_Only_Records_After_Given_Revision()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("nodeA");
        var log = new ChangeLog(wal, clock);

        var base1 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "x1",
            "1"u8.ToArray());

        var base2 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "x2",
            "2"u8.ToArray());

        var base3 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "x3",
            "3"u8.ToArray());

        var r1 = log.Append(in base1);
        var r2 = log.Append(in base2);
        var r3 = log.Append(in base3);

        var handler = new PrefixReplayHandler("x"u8, fromRevision: r1.Revision);
        log.ScanFromRevision(r1.Revision, ref handler);

        var evs = handler.Events;
        Assert.That(evs.Length, Is.EqualTo(2));

        Assert.That(evs[0].Revision, Is.EqualTo(r2.Revision));
        Assert.That(evs[1].Revision, Is.EqualTo(r3.Revision));
    }

    [Test]
    public void ScanFromRevision_WithPrefix_Filters_Keys_Correctly()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("nodeA");
        var log = new ChangeLog(wal, clock);

        // foo1
        var recFoo1 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "foo1",
            "A"u8.ToArray());
        log.Append(in recFoo1);

        // bar1
        var recBar1 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "bar1",
            "B"u8.ToArray());
        var rBar1 = log.Append(in recBar1);

        // foo2
        var recFoo2 = new ChangeRecord(
            0,
            clock.NextLocal(),
            ChangeRecordOperation.SET,
            "foo2",
            "C"u8.ToArray());
        log.Append(in recFoo2);

        var handler = new PrefixReplayHandler("foo"u8, fromRevision: rBar1.Revision);
        log.ScanFromRevision(rBar1.Revision, ref handler);

        var evs = handler.Events;
        Assert.That(evs.Length, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(evs[0].Key.Span), Is.EqualTo("foo2"));
    }
}