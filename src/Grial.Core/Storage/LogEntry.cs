namespace Grial.Core.Storage;

public readonly struct LogEntry(long seq, ReadOnlyMemory<byte> payload)
{
    public long Seq { get; } = seq;
    public ReadOnlyMemory<byte> Payload { get; } = payload;
}

public interface ILogEntryHandler
{
    void OnEntry(long seq, ReadOnlyMemory<byte> payload);
}

internal sealed class LogManifest
{
    public long CurrentSegmentId { get; set; }
    public long LastSeq { get; set; }
    public int SegmentSizeBytes { get; set; }
}

internal readonly struct SegmentMeta(long segmentId, FileInfo file, long firstSeq, long lastSeq)
{
    public long SegmentId { get; } = segmentId;
    public FileInfo File { get; } = file;
    public long FirstSeq { get; } = firstSeq;
    public long LastSeq { get; } = lastSeq;
}