namespace Grial.Test;

using Core.Clocks;
using Core.KV;
using Core.WAL;
using Core.Storage;
using System.Text;

[TestFixture]
public class KvWatchTests
{
    [Test]
    public async Task WatchPrefix_Receives_Set_And_Delete()
    {
        var manager = new KvWatchManager();
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, manager);

        var events = new List<KvWatchEvent>();

        await using var sub = manager.WatchPrefix("user:"u8, evt => {
            events.Add(evt);
        });

        kv.Apply(new ChangeRecord(
            0,
            new HybridTimestamp(1000, 0, "n"),
            ChangeRecordOperation.SET,
            "user:1"u8.ToArray(),
            "A"u8.ToArray()
        ));

        kv.Apply(new ChangeRecord(
            0,
            new HybridTimestamp(2000, 0, "n"),
            ChangeRecordOperation.DEL,
            "user:1"u8.ToArray(),
            null
        ));

        Assert.That(events.Count, Is.EqualTo(2));

        Assert.That(events[0].Kind, Is.EqualTo(KvWatchEventKind.Set));
        Assert.That(Encoding.UTF8.GetString(events[0].Key.Span), Is.EqualTo("user:1"));
        Assert.That(Encoding.UTF8.GetString(events[0].Value!.Value.Span), Is.EqualTo("A"));

        Assert.That(events[1].Kind, Is.EqualTo(KvWatchEventKind.Delete));
        Assert.That(Encoding.UTF8.GetString(events[1].Key.Span), Is.EqualTo("user:1"));
        Assert.That(events[1].Value.HasValue, Is.False);
    }

    [Test]
    public async Task WatchPrefix_Does_Not_Receive_Other_Prefixes()
    {
        var manager = new KvWatchManager();
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, manager);

        var events = new List<KvWatchEvent>();

        await using var sub = manager.WatchPrefix("user:"u8, evt => {
            events.Add(evt);
        });

        kv.Apply(new ChangeRecord(
            0,
            new HybridTimestamp(1000, 0, "n"),
            ChangeRecordOperation.SET,
            "order:1"u8.ToArray(),
            "X"u8.ToArray()
        ));

        Assert.That(events.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task Unsubscribe_Stops_Events()
    {
        var manager = new KvWatchManager();
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, manager);

        var events = new List<KvWatchEvent>();

        var sub = manager.WatchPrefix("user:"u8, evt => {
            events.Add(evt);
        });

        kv.Apply(new ChangeRecord(
            0,
            new HybridTimestamp(1000, 0, "n"),
            ChangeRecordOperation.SET,
            "user:1"u8.ToArray(),
            "A"u8.ToArray()
        ));

        Assert.That(events.Count, Is.EqualTo(1));

        await sub.DisposeAsync();

        kv.Apply(new ChangeRecord(
            0,
            new HybridTimestamp(2000, 0, "n"),
            ChangeRecordOperation.SET,
            "user:2"u8.ToArray(),
            "B"u8.ToArray()
        ));

        Assert.That(events.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task WatchPrefixAsync_Receives_Events()
    {
        var manager = new KvWatchManager();
        var wal = new SegmentedLogStorage(Path.GetTempPath(), 1024 * 1024);
        var clock = new HybridLogicalClock("n");
        var log = new ChangeLog(wal, clock);
        var kv = new ReplicatedKvStore(clock, log, manager);

        await using var sub = manager.WatchPrefixAsync("user:"u8, bufferSize: 16);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var events = new List<KvWatchEvent>();

        var readerTask = Task.Run(async () =>
        {
            await foreach (var evt in sub.WithCancellation(cts.Token))
            {
                events.Add(evt);
                if (events.Count >= 2)
                    break;
            }
        });

        kv.Apply(new ChangeRecord(
            0,
            new HybridTimestamp(1000, 0, "n"),
            ChangeRecordOperation.SET,
            "user:1"u8.ToArray(),
            "A"u8.ToArray()
        ));

        kv.Apply(new ChangeRecord(
            0,
            new HybridTimestamp(2000, 0, "n"),
            ChangeRecordOperation.DEL,
            "user:1"u8.ToArray(),
            null
        ));

        await readerTask;

        Assert.That(events.Count, Is.EqualTo(2));
        Assert.That(events[0].Kind, Is.EqualTo(KvWatchEventKind.Set));
        Assert.That(events[1].Kind, Is.EqualTo(KvWatchEventKind.Delete));
    }
}