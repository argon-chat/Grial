namespace Grial.Test;

using Core.Clocks;
using Core.Storage;
using Core.WAL;
using System.Text;

[TestFixture]
public sealed unsafe class SegmentGcTests
{
    sealed class TempDir : IDisposable
    {
        public string Path { get; }

        public TempDir()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "wal-gc-test-" + Guid.NewGuid());

            Directory.CreateDirectory(Path);
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(Path, true);
            }
            catch
            {
                /* ignore */
            }
        }
    }

    [Test]
    public void Gc_Removes_Old_Segments_And_Keeps_Active_One()
    {
        using var dir = new TempDir();

        // Tiny segment size so we produce many segments quickly
        var wal = new SegmentedLogStorage(dir.Path, segmentSizeBytes: 512);

        var clock = new HybridLogicalClock("nodeA");
        var changeLog = new ChangeLog(wal, clock);

        // Append 100 records → definitely enough to create multiple segments
        for (var i = 0; i < 100; i++)
        {
            var rec = new ChangeRecord(0,
                new HybridTimestamp(1000 + i, 0, "nodeA"),
                ChangeRecordOperation.SET,
                Encoding.UTF8.GetBytes("k" + i),
                Encoding.UTF8.GetBytes("v" + i));

            changeLog.Append(rec);
        }

        var seqBefore = changeLog.LastSeq;

        var initialSegments = wal.SegmentCount;
        Assert.That(initialSegments, Is.GreaterThan(1));

        var keepSeq = seqBefore - 10;
        var removed = changeLog.CollectSegments(keepSeq);

        Assert.That(removed, Is.GreaterThan(0));
        Assert.That(wal.SegmentCount, Is.LessThan(initialSegments));

        Assert.That(wal.SegmentCount, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public void Gc_Does_Not_Delete_Active_Segment()
    {
        using var dir = new TempDir();

        var wal = new SegmentedLogStorage(dir.Path, 256);
        var clock = new HybridLogicalClock("nodeA");
        var changeLog = new ChangeLog(wal, clock);

        for (var i = 0; i < 40; i++)
        {
            var rec = new ChangeRecord(0,
                new HybridTimestamp(2000 + i, 0, "nodeA"),
                ChangeRecordOperation.SET,
                Encoding.UTF8.GetBytes("a" + i),
                Encoding.UTF8.GetBytes("b" + i));

            changeLog.Append(rec);
        }

        var before = wal.SegmentCount;

        var keepSeq = changeLog.LastSeq;
        changeLog.CollectSegments(keepSeq);

        var after = wal.SegmentCount;

        Assert.That(after, Is.GreaterThanOrEqualTo(1));

        Assert.That(after, Is.LessThan(before));
    }

    [Test]
    public void Segments_Removed_By_Gc_Do_Not_Affect_ScanFrom()
    {
        using var dir = new TempDir();

        var wal = new SegmentedLogStorage(dir.Path, 256);
        var clock = new HybridLogicalClock("nodeA");
        var changeLog = new ChangeLog(wal, clock);

        for (var i = 0; i < 50; i++)
        {
            var rec = new ChangeRecord(0,
                new HybridTimestamp(3000 + i, 0, "nodeA"),
                ChangeRecordOperation.SET,
                Encoding.UTF8.GetBytes("k" + i),
                Encoding.UTF8.GetBytes("v" + i));

            changeLog.Append(rec);
        }

        var last = changeLog.LastSeq;

        changeLog.CollectSegments(minSeqToKeep: 25);

        var count = 0;

        var handler = new CountingHandler(&count);

        changeLog.ScanFrom(25, 1000, ref handler);

        Assert.That(count, Is.EqualTo(50 - 25), "ScanFrom returned wrong number of entries");
    }

    readonly struct CountingHandler(int* ptr) : ILogEntryHandler
    {
        public void OnEntry(long seq, ReadOnlyMemory<byte> payload) => (*ptr)++;
    }
}