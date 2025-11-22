namespace Grial.Core.WAL;

using Clocks;
using KV.Leases;
using Storage;
using KV;
using System.Formats.Cbor;
using System.Runtime.InteropServices;

public class SnapshotManager
{
    private readonly ReplicatedKvStore store;
    private readonly ChangeLog log;
    private readonly DirectoryInfo dir;
    private readonly Lock sync = new();
    private readonly ILeaseSnapshotBackend? leaseBackend;

    private const string SnapshotFileName = "snapshot.cbor";
    private const string MetaFileName = "snapshot.meta.json";

    public SnapshotManager(
        ReplicatedKvStore store,
        ChangeLog log,
        SegmentedLogStorage segmentStorage,
        ILeaseSnapshotBackend leaseBackend)
    {
        this.store = store;
        this.log = log;
        dir = segmentStorage.WalDirectory;
        if (!dir.Exists) dir.Create();
        this.leaseBackend = leaseBackend;
    }

    public bool TryLoad()
    {
        lock (sync)
        {
            var metaFile = new FileInfo(Path.Combine(dir.FullName, MetaFileName));
            var snapFile = new FileInfo(Path.Combine(dir.FullName, SnapshotFileName));

            if (!metaFile.Exists || !snapFile.Exists)
                return false;

            SnapshotMeta meta;
            try
            {
                var json = File.ReadAllText(metaFile.FullName);
                meta = System.Text.Json.JsonSerializer.Deserialize<SnapshotMeta>(json)
                       ?? throw new Exception("Meta parse failed");
            }
            catch
            {
                return false;
            }

            byte[] bytes;
            try
            {
                bytes = File.ReadAllBytes(snapFile.FullName);
            }
            catch
            {
                return false;
            }

            var reader = new CborReader(bytes);

            var arrLen = reader.ReadStartArray();
            var lastWalOffsetFromSnapshot = reader.ReadInt64();

            var entriesLen = reader.ReadStartArray();

            for (var i = 0; i < entriesLen; i++)
            {
                var innerLen = reader.ReadStartArray();

                var key = new Utf8Key(reader.ReadByteString());
                ReadOnlyMemory<byte>? value;

                if (reader.PeekState() == CborReaderState.Null)
                {
                    reader.ReadNull();
                    value = null;
                }
                else
                {
                    value = reader.ReadByteString();
                }

                var physical = reader.ReadInt64();
                var logical = reader.ReadInt32();
                var nodeId = reader.ReadTextString();

                reader.ReadEndArray();

                // In the snapshot, we do not restore the "historical" revision
                // — ReplicatedKvStore keeps its own counter.
                store.Apply(new ChangeRecord(
                    0,
                    new HybridTimestamp(physical, logical, nodeId),
                    value.HasValue ? ChangeRecordOperation.SET : ChangeRecordOperation.DEL,
                    key: key,
                    value: value
                ));
            }

            reader.ReadEndArray(); // kvEntries

            List<LeaseSnapshotEntry>? leaseSnapshotEntries = null;

            if (arrLen == 3)
            {
                var leasesLen = reader.ReadStartArray() ?? throw new InvalidOperationException();
                leaseSnapshotEntries = new List<LeaseSnapshotEntry>(leasesLen);

                for (var i = 0; i < leasesLen; i++)
                {
                    var innerLen = reader.ReadStartArray(); // 4

                    var leaseIdValue = reader.ReadInt64();
                    var expireAt = reader.ReadInt64();
                    var ttlMs = reader.ReadInt64();

                    var raw = reader.ReadByteString();
                    var keys = new PackedKeys(raw);

                    reader.ReadEndArray(); // lease entry

                    var leaseId = new LeaseId(leaseIdValue);
                    var lease = new LeaseEntry(leaseId, expireAt, TimeSpan.FromMilliseconds(ttlMs));
                    leaseSnapshotEntries.Add(new LeaseSnapshotEntry(lease, keys));
                }

                reader.ReadEndArray(); // leaseEntries
            }

            reader.ReadEndArray(); // root

            if (leaseBackend is not null && leaseSnapshotEntries is { Count: > 0 })
                leaseBackend.RestoreLeasesFromSnapshot(CollectionsMarshal.AsSpan(leaseSnapshotEntries));

            return true;
        }
    }

    public async ValueTask CreateSnapshotAsync()
    {
        byte[] buffer = [];
        var metaTmp = Path.Combine(dir.FullName, MetaFileName + ".tmp");
        var snapTmp = Path.Combine(dir.FullName, SnapshotFileName + ".tmp");

        long lastWalOffset;
        LeaseSnapshotEntry[]? leaseItems = null;

        lock (sync)
        {
            var kvItems = store.ExportAll();
            lastWalOffset = log.LastSeq; // WAL offset

            if (leaseBackend is not null)
            {
                leaseItems = leaseBackend.ExportLeasesForSnapshot();
            }

            var writer = new CborWriter();

            if (leaseItems is null || leaseItems.Length == 0)
            {
                writer.WriteStartArray(2);
                writer.WriteInt64(lastWalOffset);

                writer.WriteStartArray(kvItems.Length);
                foreach (ref readonly var e in kvItems.AsSpan())
                {
                    writer.WriteStartArray(5);
                    writer.WriteByteString(e.Key.Span);

                    if (e.Value.HasValue)
                        writer.WriteByteString(e.Value.Value.Span);
                    else
                        writer.WriteNull();

                    writer.WriteInt64(e.Timestamp.PhysicalMillis);
                    writer.WriteInt32(e.Timestamp.LogicalCounter);
                    writer.WriteTextString(e.Timestamp.NodeId);
                    writer.WriteEndArray();
                }

                writer.WriteEndArray(); // kvEntries
                writer.WriteEndArray(); // root
            }
            else
            {
                // [lastWalOffset, kvEntries[], leaseEntries[]]
                writer.WriteStartArray(3);
                writer.WriteInt64(lastWalOffset);

                // kv entries
                writer.WriteStartArray(kvItems.Length);
                foreach (ref readonly var e in kvItems.AsSpan())
                {
                    writer.WriteStartArray(5);
                    writer.WriteByteString(e.Key.Span);

                    if (e.Value.HasValue)
                        writer.WriteByteString(e.Value.Value.Span);
                    else
                        writer.WriteNull();

                    writer.WriteInt64(e.Timestamp.PhysicalMillis);
                    writer.WriteInt32(e.Timestamp.LogicalCounter);
                    writer.WriteTextString(e.Timestamp.NodeId);
                    writer.WriteEndArray();
                }

                writer.WriteEndArray(); // kvEntries

                // lease entries
                writer.WriteStartArray(leaseItems.Length);
                foreach (ref readonly var le in leaseItems.AsSpan())
                {
                    writer.WriteStartArray(4);

                    writer.WriteInt64(le.Lease.Id.Value);
                    writer.WriteInt64(le.Lease.ExpireAtMillis);
                    writer.WriteInt64((long)le.Lease.Ttl.TotalMilliseconds);

                    writer.WriteByteString(le.Keys.Buffer.Span);

                    writer.WriteEndArray(); // lease entry
                }

                writer.WriteEndArray(); // leaseEntries

                writer.WriteEndArray(); // root
            }

            buffer = writer.Encode();
            File.WriteAllBytes(snapTmp, buffer);

            var meta = new SnapshotMeta(
                LastRevision: 0,
                LastWalOffset: lastWalOffset,
                CreatedByNode: Environment.MachineName);

            File.WriteAllText(
                metaTmp,
                System.Text.Json.JsonSerializer.Serialize(
                    value: meta,
                    options: new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
        }

        await FsyncAndReplaceAsync(snapTmp, Path.Combine(dir.FullName, SnapshotFileName));
        await FsyncAndReplaceAsync(metaTmp, Path.Combine(dir.FullName, MetaFileName));

        log.CollectSegments(minSeqToKeep: lastWalOffset);
    }

    private static async Task FsyncAndReplaceAsync(string tmp, string final)
    {
        await using (var fs = new FileStream(tmp, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
        {
            await fs.FlushAsync();
            fs.Flush(true);
        }

        if (File.Exists(final))
            File.Delete(final);

        File.Move(tmp, final);
    }

    public sealed record SnapshotMeta(
        long LastRevision,
        long LastWalOffset,
        string CreatedByNode);
}