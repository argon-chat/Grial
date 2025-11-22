namespace Grial.Test;

using Core.Clocks;
using Core.KV;
using Core.Storage;
using Core.WAL;
using NUnit.Framework.Legacy;

[TestFixture]
public class ReplicatedKvStoreRadixIntegrationTests
{
    [Test]
    public void Kv_PrefixScan_Returns_Only_Matching_Keys()
    {
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(10, 0, "n"),
            ChangeRecordOperation.SET,
            "user:1"u8.ToArray(),
            "A"u8.ToArray()
        ));

        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(11, 0, "n"),
            ChangeRecordOperation.SET,
            "user:2"u8.ToArray(),
            "B"u8.ToArray()
        ));

        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(12, 0, "n"),
            ChangeRecordOperation.SET,
            "order:1"u8.ToArray(),
            "X"u8.ToArray()
        ));

        var result = kv.GetByPrefix("user:"u8);

        var keys = result.Select(x => System.Text.Encoding.UTF8.GetString(x.Key.Span)).ToArray();

        CollectionAssert.Contains(keys, "user:1");
        CollectionAssert.Contains(keys, "user:2");
        CollectionAssert.DoesNotContain(keys, "order:1");
    }

    [Test]
    public void Kv_Delete_Removes_From_PrefixScan()
    {
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(10, 0, "n"),
            ChangeRecordOperation.SET,
            "key:1"u8.ToArray(),
            "AAA"u8.ToArray()
        ));

        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(11, 0, "n"),
            ChangeRecordOperation.SET,
            "key:2"u8.ToArray(),
            "BBB"u8.ToArray()
        ));

        // delete key:1
        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(12, 0, "n"),
            ChangeRecordOperation.DEL,
            "key:1"u8.ToArray(),
            null
        ));

        var result = kv.GetByPrefix("key:"u8);
        var keys = result.Select(x => System.Text.Encoding.UTF8.GetString(x.Key.Span)).ToArray();

        CollectionAssert.DoesNotContain(keys, "key:1");
        CollectionAssert.Contains(keys, "key:2");
    }

    [Test]
    public void Kv_Lww_Conflict_Uses_Newer_And_Index_Stays_Intact()
    {
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        // older write
        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(1000, 0, "n"),
            ChangeRecordOperation.SET,
            "x"u8.ToArray(),
            "OLD"u8.ToArray()
        ));

        // newer write
        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(2000, 0, "n"),
            ChangeRecordOperation.SET,
            "x"u8.ToArray(),
            "NEW"u8.ToArray()
        ));

        var result = kv.GetByPrefix("x"u8);

        Assert.That(result.Length, Is.EqualTo(1));
        Assert.That(System.Text.Encoding.UTF8.GetString(result[0].Value!.Value.Span), Is.EqualTo("NEW"));
    }

    [Test]
    public void Kv_PrefixScan_Returns_Zero_For_Unknown_Prefix()
    {
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        kv.Apply(new ChangeRecord(0,
            new HybridTimestamp(10, 0, "n"),
            ChangeRecordOperation.SET,
            "alpha"u8.ToArray(),
            "A"u8.ToArray()
        ));

        var result = kv.GetByPrefix("beta"u8);
        Assert.That(result.Length, Is.EqualTo(0));
    }

    [Test]
    public void Kv_PrefixScan_Deep_Keys()
    {
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        kv.Apply(new ChangeRecord(0, new HybridTimestamp(1, 0, "n"), ChangeRecordOperation.SET, "a/b/c"u8.ToArray(),
            "1"u8.ToArray()));
        kv.Apply(new ChangeRecord(0, new HybridTimestamp(2, 0, "n"), ChangeRecordOperation.SET, "a/b/d"u8.ToArray(),
            "2"u8.ToArray()));
        kv.Apply(new ChangeRecord(0, new HybridTimestamp(3, 0, "n"), ChangeRecordOperation.SET, "a/x"u8.ToArray(),
            "3"u8.ToArray()));

        var result = kv.GetByPrefix("a/b"u8);

        var keys = result.Select(x => System.Text.Encoding.UTF8.GetString(x.Key.Span)).ToHashSet();
        Assert.That(keys.SetEquals(["a/b/c", "a/b/d"]));
    }

    [Test]
    public void Cas_Works_For_Initial_Create()
    {
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        var ok = kv.TryCompareAndSet("a"u8, expectedRevision: 0, "1"u8, out var rev1);
        Assert.That(ok, Is.True);
        Assert.That(rev1, Is.GreaterThan(0));

        var ok2 = kv.TryCompareAndSet("a"u8, expectedRevision: 0, "2"u8, out _);
        Assert.That(ok2, Is.False); // wrong expected revision
    }

    [Test]
    public void Cas_Rejects_Stale_Writes()
    {
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        var ok = kv.TryCompareAndSet("k"u8, 0, "v1"u8, out var rev1);
        Assert.That(ok, Is.True);

        // revision mismatch
        var bad = kv.TryCompareAndSet("k"u8, expectedRevision: rev1 - 1, "v2"u8, out _);
        Assert.That(bad, Is.False);

        // correct revision
        var ok2 = kv.TryCompareAndSet("k"u8, expectedRevision: rev1, "v3"u8, out var rev2);
        Assert.That(ok2, Is.True);
    }


    [Test]
    public void CasDelete_Create_Tombstone_When_Key_Missing_And_Expected_0()
    {
        var walDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        var wal = new SegmentedLogStorage(walDir, 1024 * 1024);
        var clock = new HybridLogicalClock("n1");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        var ok = kv.TryCompareAndDelete("foo"u8, expectedRevision: 0, out var rev);
        Assert.That(ok, Is.True);
        Assert.That(rev, Is.GreaterThan(0));

        var has = kv.TryGet("foo"u8, out var value);
        Assert.That(has, Is.False);
    }

    [Test]
    public void CasDelete_Rejects_Stale_Revision()
    {
        var walDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        var wal = new SegmentedLogStorage(walDir, 1024 * 1024);
        var clock = new HybridLogicalClock("n1");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, new KvWatchManager());

        var ts = clock.NextLocal();
        var putRec = log.Append(new ChangeRecord(
            0,
            ts,
            op: ChangeRecordOperation.SET,
            key: "foo"u8.ToArray(),
            value: "bar"u8.ToArray()));
        kv.Apply(putRec);

        var staleRev = putRec.Revision - 1;
        var okStale = kv.TryCompareAndDelete("foo"u8, expectedRevision: staleRev, out _);
        Assert.That(okStale, Is.False);

        var okReal = kv.TryCompareAndDelete("foo"u8, expectedRevision: putRec.Revision, out var delRev);
        Assert.That(okReal, Is.True);
        Assert.That(delRev, Is.GreaterThan(putRec.Revision));
    }
}