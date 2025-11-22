namespace Grial.Core.WAL;

using Clocks;
using Microsoft.Extensions.Logging;
using Storage;
using System.Diagnostics;
using System.Formats.Cbor;
using System.Runtime.InteropServices;

public sealed class ChangeLog(SegmentedLogStorage wal, HybridLogicalClock clock, ILogger<ChangeLog>? logger = null)
{
    private readonly HybridLogicalClock clock = clock;

    private long lastSeq = 0;
    private long lastRevision = 0;

    public long LastSeq => Volatile.Read(ref lastSeq);
    public long LastRevision => Volatile.Read(ref lastRevision);

    public ChangeRecord Append(in ChangeRecord record)
    {
        var start = Stopwatch.GetTimestamp();

        var newRevision = Interlocked.Increment(ref lastRevision);

        var actual = new ChangeRecord(
            newRevision,
            record.Timestamp,
            record.Op,
            record.Key,
            record.Value);

        var writer = new CborWriter();
        ChangeRecordCbor.Write(ref writer, in actual);
        var payload = writer.Encode();

        var seq = wal.Append(payload);
        Volatile.Write(ref lastSeq, seq);

        logger?.LogDebug(
            "Append: ts={Timestamp:o} seq={Seq} rev={Rev} op={Op} keyLen={KeyLen} valueLen={ValueLen} elapsed={Elapsed}µs",
            actual.Timestamp,
            seq,
            actual.Revision,
            actual.Op,
            actual.Key.Length,
            actual.Value?.Length ?? 0,
            ElapsedMicro(start));

        return actual;
    }

    public void ScanFrom<THandler>(long afterSeq, int max, ref THandler handler)
        where THandler : struct, ILogEntryHandler
    {
        logger?.LogDebug("ScanFrom: afterSeq={AfterSeq}, max={Max}", afterSeq, max);
        wal.ScanFrom(afterSeq, max, ref handler);
    }

    public int CollectSegments(long minSeqToKeep)
    {
        logger?.LogInformation("CollectSegments: keeping from seq {MinSeq}", minSeqToKeep);

        var removed = wal.CollectSegments(minSeqToKeep);

        logger?.LogInformation("CollectSegments finished: removed={Removed}", removed);

        return removed;
    }

    /// <summary>
    /// WAL scan, starting with fromRevision.
    /// Now we believe that seq ~ revision.
    /// </summary>
    public void ScanFromRevision<THandler>(long fromRevision, ref THandler handler)
        where THandler : struct, ILogEntryHandler
    {
        logger?.LogDebug("ScanFromRevision: fromRevision={FromRev}", fromRevision);
        wal.ScanFrom(fromRevision, int.MaxValue, ref handler);
    }

    /// <summary>
    /// Reads all WAL entries with Revision > fromRevision.
    /// </summary>
    public ChangeRecord[] ReadAfter(long fromRevision)
    {
        logger?.LogDebug("ReadAfter: fromRevision={FromRev}", fromRevision);

        var start = Stopwatch.GetTimestamp();

        var result = new List<ChangeRecord>(64);
        var handler = new RevisionCollector(fromRevision, result);
        wal.ScanFrom(0, int.MaxValue, ref handler);

        var arr = CollectionsMarshal.AsSpan(result).ToArray();

        logger?.LogInformation(
            "ReadAfter finished: fromRev={FromRev}, count={Count}, elapsed={Elapsed}ms",
            fromRevision,
            arr.Length,
            ElapsedMs(start));

        return arr;
    }

    private static double ElapsedMicro(long start) =>
        (Stopwatch.GetTimestamp() - start) * 1_000_000.0 / Stopwatch.Frequency;

    private static double ElapsedMs(long start) =>
        (Stopwatch.GetTimestamp() - start) * 1000.0 / Stopwatch.Frequency;

    private readonly struct RevisionCollector(long fromRev, List<ChangeRecord> dest) : ILogEntryHandler
    {
        public void OnEntry(long seq, ReadOnlyMemory<byte> payload)
        {
            var reader = new CborReader(payload);
            var rec = ChangeRecordCbor.Read(ref reader);

            if (rec.Revision > fromRev)
                dest.Add(rec);
        }
    }
}