namespace Grial.Core.Storage;

using System.Buffers;
using System.IO.Hashing;
using System.Text.Json;

public class SegmentedLogStorage
{
    private const string ManifestFileName = "manifest.json";
    private const int DefaultSegmentSize = 32 * 1024 * 1024;
    private const int MaxRecordSize = 100 * 1024 * 1024;

    private readonly DirectoryInfo directory;
    private readonly FileInfo manifestFile;
    private readonly Lock syncLock = new();

    public DirectoryInfo WalDirectory => directory;

    private readonly int segmentSizeBytes;

    private FileStream? currentSegmentStream;
    private long currentSegmentId;
    private long currentSegmentLength;
    private long lastSeq;
    private bool disposed;

    private SegmentMeta[] segments = [];
    private int segmentCount;

    public int SegmentCount
    {
        get
        {
            lock (syncLock)
            {
                return segmentCount;
            }
        }
    }

    public SegmentedLogStorage(string directoryPath, int segmentSizeBytes = DefaultSegmentSize)
    {
        directory = new DirectoryInfo(directoryPath);
        if (!directory.Exists) directory.Create();

        manifestFile = new FileInfo(Path.Combine(directory.FullName, ManifestFileName));
        this.segmentSizeBytes = segmentSizeBytes > 0 ? segmentSizeBytes : DefaultSegmentSize;

        lock (syncLock)
        {
            LoadOrInit();
        }
    }

    public long LastSeq
    {
        get
        {
            lock (syncLock)
            {
                return lastSeq;
            }
        }
    }

    public long Append(ReadOnlySpan<byte> payload)
    {
        ThrowIfDisposed();

        lock (syncLock)
        {
            EnsureCurrentSegment();

            // seq(8) + len(4) + payload + crc64(8)
            var estimatedSize = 8 + 4 + payload.Length + 8;

            if (currentSegmentLength + estimatedSize > segmentSizeBytes)
            {
                RotateSegment();
            }

            var seq = lastSeq + 1;

            const int headerLength = 8 + 4;
            var header = ArrayPool<byte>.Shared.Rent(headerLength);
            try
            {
                BitConverter.TryWriteBytes(header.AsSpan(0, 8), seq);
                BitConverter.TryWriteBytes(header.AsSpan(8, 4), payload.Length);

                var crc64 = new Crc64();
                crc64.Append(header.AsSpan(0, headerLength));
                crc64.Append(payload);
                var crc = crc64.GetCurrentHashAsUInt64();

                var crcBytes = ArrayPool<byte>.Shared.Rent(8);
                try
                {
                    BitConverter.TryWriteBytes(crcBytes.AsSpan(0, 8), crc);

                    currentSegmentStream!.Write(header, 0, headerLength);
                    currentSegmentStream.Write(payload);
                    currentSegmentStream.Write(crcBytes, 0, 8);
                    currentSegmentStream.Flush();

                    currentSegmentLength += estimatedSize;

                    lastSeq = seq;
                    UpdateSegmentsMetaOnAppend(seq);
                    SaveManifest();

                    return seq;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(crcBytes);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(header);
            }
        }
    }

    public void ScanFrom<THandler>(long afterSeq, int maxCount, ref THandler handler)
        where THandler : struct, ILogEntryHandler
    {
        ThrowIfDisposed();
        if (maxCount <= 0) return;

        SegmentMeta[] snapshot;
        int snapshotCount;

        lock (syncLock)
        {
            if (segmentCount == 0)
                return;

            snapshot = new SegmentMeta[segmentCount];
            Array.Copy(segments, snapshot, segmentCount);
            snapshotCount = segmentCount;
        }

        var remaining = maxCount;

        for (var i = 0; i < snapshotCount && remaining > 0; i++)
        {
            var meta = snapshot[i];

            if (meta.LastSeq <= afterSeq)
                continue;

            using var stream = new FileStream(
                meta.File.FullName, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);

            const int headerLength = 8 + 4;
            var header = ArrayPool<byte>.Shared.Rent(headerLength);
            var crcBytes = ArrayPool<byte>.Shared.Rent(8);
            var payloadBuffer = ArrayPool<byte>.Shared.Rent(4096);

            try
            {
                while (remaining > 0 && stream.Position < stream.Length)
                {
                    var recordStart = stream.Position;

                    if (!ReadExact(stream, header, headerLength))
                    {
                        Truncate(stream, recordStart);
                        break;
                    }

                    var seq = BitConverter.ToInt64(header, 0);
                    var len = BitConverter.ToInt32(header, 8);

                    if (len is < 0 or > MaxRecordSize)
                    {
                        Truncate(stream, recordStart);
                        break;
                    }

                    if (payloadBuffer.Length < len)
                    {
                        ArrayPool<byte>.Shared.Return(payloadBuffer);
                        payloadBuffer = ArrayPool<byte>.Shared.Rent(len);
                    }

                    if (!ReadExact(stream, payloadBuffer, len))
                    {
                        Truncate(stream, recordStart);
                        break;
                    }

                    if (!ReadExact(stream, crcBytes, 8))
                    {
                        Truncate(stream, recordStart);
                        break;
                    }

                    var storedCrc = BitConverter.ToUInt64(crcBytes, 0);

                    var crc64 = new Crc64();
                    crc64.Append(header.AsSpan(0, headerLength));
                    crc64.Append(payloadBuffer.AsSpan(0, len));
                    var computed = crc64.GetCurrentHashAsUInt64();

                    if (computed != storedCrc)
                    {
                        Truncate(stream, recordStart);
                        break;
                    }

                    if (seq > afterSeq)
                    {
                        handler.OnEntry(seq, new ReadOnlyMemory<byte>(payloadBuffer, 0, len));
                        remaining--;
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(header);
                ArrayPool<byte>.Shared.Return(crcBytes);
                ArrayPool<byte>.Shared.Return(payloadBuffer);
            }
        }
    }

    public int CollectSegments(long minSeqToKeep)
    {
        ThrowIfDisposed();

        lock (syncLock)
        {
            if (segmentCount == 0)
                return 0;

            var removed = 0;
            var write = 0;

            for (var read = 0; read < segmentCount; read++)
            {
                var meta = segments[read];

                if (meta.LastSeq == 0)
                {
                    segments[write++] = meta;
                    continue;
                }

                if (meta.SegmentId == currentSegmentId)
                {
                    segments[write++] = meta;
                    continue;
                }

                if (meta.LastSeq <= minSeqToKeep)
                {
                    try
                    {
                        if (meta.File.Exists)
                            meta.File.Delete();
                    }
                    catch
                    {
                        // ignore
                    }

                    removed++;
                    continue;
                }

                segments[write++] = meta;
            }

            for (var i = write; i < segmentCount; i++)
                segments[i] = default;

            segmentCount = write;

            return removed;
        }
    }

    public void Dispose()
    {
        if (disposed) return;
        lock (syncLock)
        {
            if (disposed) return;
            disposed = true;
            currentSegmentStream?.Dispose();
            currentSegmentStream = null;
        }
    }

    public ValueTask DisposeAsync()
    {
        if (disposed) return ValueTask.CompletedTask;

        FileStream? stream;
        lock (syncLock)
        {
            if (disposed) return ValueTask.CompletedTask;
            disposed = true;
            stream = currentSegmentStream;
            currentSegmentStream = null;
        }

        return stream is not null ? ValueTask.CompletedTask : stream.DisposeAsync();
    }

    private void ThrowIfDisposed()
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(SegmentedLogStorage));
    }

    private void LoadOrInit()
    {
        if (manifestFile.Exists)
        {
            var json = File.ReadAllText(manifestFile.FullName);
            var manifest = JsonSerializer.Deserialize<LogManifest>(json) ?? new LogManifest();

            currentSegmentId = manifest.CurrentSegmentId == 0 ? 1 : manifest.CurrentSegmentId;
            lastSeq = manifest.LastSeq;

            LoadSegmentsMeta();
            OpenCurrentSegmentForAppend();
        }
        else
        {
            currentSegmentId = 1;
            lastSeq = 0;
            segments = [];
            segmentCount = 0;

            SaveManifest();
            OpenCurrentSegmentForAppend();
        }
    }

    private void LoadSegmentsMeta()
    {
        segments = [];
        segmentCount = 0;

        var files = directory.GetFiles("*.log");
        Array.Sort(files, static (a, b) =>
            string.CompareOrdinal(a.Name, b.Name));

        var metas = new List<SegmentMeta>(files.Length);

        foreach (var file in files)
        {
            if (!long.TryParse(Path.GetFileNameWithoutExtension(file.Name), out var id))
                continue;

            var meta = ScanSegment(file, id);
            if (meta.HasValue)
            {
                metas.Add(meta.Value);
            }
        }

        metas.Sort((a, b) => a.SegmentId.CompareTo(b.SegmentId));

        if (metas.Count > 0)
        {
            segments = metas.ToArray();
            segmentCount = segments.Length;

            var lastMeta = segments[segmentCount - 1];
            currentSegmentId = lastMeta.SegmentId;
            lastSeq = lastMeta.LastSeq;
        }
        else
        {
            segments = [];
            segmentCount = 0;
            currentSegmentId = 1;
            lastSeq = 0;
        }
    }

    private static SegmentMeta? ScanSegment(FileInfo file, long segmentId)
    {
        using var stream = new FileStream(
            file.FullName, FileMode.Open, FileAccess.ReadWrite,
            FileShare.ReadWrite | FileShare.Delete);

        const int headerLength = 8 + 4;
        var header = ArrayPool<byte>.Shared.Rent(headerLength);
        var crcBytes = ArrayPool<byte>.Shared.Rent(8);
        var payloadBuffer = ArrayPool<byte>.Shared.Rent(4096);

        long firstSeq = 0;
        long lastSeqInSegment = 0;

        try
        {
            while (stream.Position < stream.Length)
            {
                var recordStart = stream.Position;

                if (!ReadExact(stream, header, headerLength))
                {
                    Truncate(stream, recordStart);
                    break;
                }

                var seq = BitConverter.ToInt64(header, 0);
                var len = BitConverter.ToInt32(header, 8);

                if (len is < 0 or > MaxRecordSize)
                {
                    Truncate(stream, recordStart);
                    break;
                }

                if (payloadBuffer.Length < len)
                {
                    ArrayPool<byte>.Shared.Return(payloadBuffer);
                    payloadBuffer = ArrayPool<byte>.Shared.Rent(len);
                }

                if (!ReadExact(stream, payloadBuffer, len))
                {
                    Truncate(stream, recordStart);
                    break;
                }

                if (!ReadExact(stream, crcBytes, 8))
                {
                    Truncate(stream, recordStart);
                    break;
                }

                var storedCrc = BitConverter.ToUInt64(crcBytes, 0);

                var crc64 = new Crc64();
                crc64.Append(header.AsSpan(0, headerLength));
                crc64.Append(payloadBuffer.AsSpan(0, len));
                var computed = crc64.GetCurrentHashAsUInt64();

                if (computed != storedCrc)
                {
                    Truncate(stream, recordStart);
                    break;
                }

                if (firstSeq == 0)
                    firstSeq = seq;
                lastSeqInSegment = seq;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(header);
            ArrayPool<byte>.Shared.Return(crcBytes);
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }

        if (stream.Length == 0)
        {
            return new SegmentMeta(segmentId, file, 0, 0);
        }

        if (firstSeq == 0 && lastSeqInSegment == 0)
        {
            stream.SetLength(0);
            return new SegmentMeta(segmentId, file, 0, 0);
        }

        return new SegmentMeta(segmentId, file, firstSeq, lastSeqInSegment);
    }

    private void OpenCurrentSegmentForAppend()
    {
        var file = GetSegmentFile(currentSegmentId);
        currentSegmentStream?.Dispose();
        currentSegmentStream = new FileStream(
            file.FullName, FileMode.OpenOrCreate, FileAccess.ReadWrite,
            FileShare.ReadWrite | FileShare.Delete);

        currentSegmentStream.Seek(0, SeekOrigin.End);
        currentSegmentLength = currentSegmentStream.Length;

        var found = false;
        for (var i = 0; i < segmentCount; i++)
        {
            if (segments[i].SegmentId == currentSegmentId)
            {
                found = true;
                break;
            }
        }

        if (!found)
        {
            var meta = ScanSegment(file, currentSegmentId) ??
                       new SegmentMeta(currentSegmentId, file, 0, 0);
            AddOrUpdateSegmentMeta(meta);
        }
    }

    private void EnsureCurrentSegment()
    {
        if (currentSegmentStream == null)
        {
            OpenCurrentSegmentForAppend();
        }
    }

    private void RotateSegment()
    {
        currentSegmentStream?.Dispose();
        currentSegmentStream = null;
        currentSegmentId++;
        OpenCurrentSegmentForAppend();
    }

    private void SaveManifest()
    {
        var manifest = new LogManifest
        {
            CurrentSegmentId = currentSegmentId,
            LastSeq = lastSeq,
            SegmentSizeBytes = segmentSizeBytes
        };

        var json = JsonSerializer.Serialize(
            manifest,
            new JsonSerializerOptions { WriteIndented = true });

        File.WriteAllText(manifestFile.FullName, json);
    }

    private void UpdateSegmentsMetaOnAppend(long seq)
    {
        var file = GetSegmentFile(currentSegmentId);
        var updated = false;

        for (var i = 0; i < segmentCount; i++)
        {
            if (segments[i].SegmentId == currentSegmentId)
            {
                var existing = segments[i];
                var first = existing.FirstSeq == 0 ? seq : existing.FirstSeq;
                segments[i] = new SegmentMeta(currentSegmentId, file, first, seq);
                updated = true;
                break;
            }
        }

        if (!updated)
        {
            AddOrUpdateSegmentMeta(new SegmentMeta(currentSegmentId, file, seq, seq));
        }
    }

    private void AddOrUpdateSegmentMeta(in SegmentMeta meta)
    {
        if (segments.Length == segmentCount)
        {
            var newSize = segmentCount == 0 ? 4 : segmentCount * 2;
            var newArr = new SegmentMeta[newSize];
            if (segmentCount > 0)
                Array.Copy(segments, newArr, segmentCount);
            segments = newArr;
        }

        segments[segmentCount++] = meta;
        Array.Sort(segments, 0, segmentCount, SegmentMetaComparer.Instance);
    }

    private FileInfo GetSegmentFile(long segmentId)
    {
        var name = segmentId.ToString("D8") + ".log";
        return new FileInfo(Path.Combine(directory.FullName, name));
    }

    private static bool ReadExact(Stream stream, byte[] buffer, int count)
    {
        var offset = 0;
        while (offset < count)
        {
            var read = stream.Read(buffer, offset, count - offset);
            if (read == 0)
                return false;
            offset += read;
        }

        return true;
    }

    private static void Truncate(FileStream stream, long newLength)
    {
        stream.SetLength(newLength);
    }

    private sealed class SegmentMetaComparer : IComparer<SegmentMeta>
    {
        public static readonly SegmentMetaComparer Instance = new();

        public int Compare(SegmentMeta x, SegmentMeta y)
            => x.SegmentId.CompareTo(y.SegmentId);
    }
}