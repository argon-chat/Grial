namespace Grial.Core.KV;

using DnsClient.Internal;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using WAL;

public sealed class KvWatchManager(ILogger<KvWatchManager>? logger = null)
{
    private readonly struct WatchRegistration(long id, byte[] prefix, Action<KvWatchEvent> callback)
    {
        public readonly long Id = id;
        public readonly byte[] Prefix = prefix;
        public readonly Action<KvWatchEvent> Callback = callback;
    }

    private readonly Lock gate = new();
    private readonly List<WatchRegistration> watches = new();
    private long nextId = 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnterLock()
    {
        if (gate.TryEnter()) 
            return;
        var sw = new SpinWait();
        while (!gate.TryEnter())
            sw.SpinOnce();
    }

    public KvWatchSubscription WatchPrefix(ReadOnlySpan<byte> prefix, Action<KvWatchEvent> callback)
    {
        if (callback is null) throw new ArgumentNullException(nameof(callback));
        if (prefix.Length == 0) throw new ArgumentException("Prefix must be non-empty", nameof(prefix));

        var prefixCopy = prefix.ToArray();

        long id;
        EnterLock();
        try
        {
            id = nextId++;
            watches.Add(new WatchRegistration(id, prefixCopy, callback));
        }
        finally
        {
            gate.Exit();
        }

        logger?.LogDebug("WatchPrefix: registered id={Id} prefixLen={Len}", id, prefixCopy.Length);

        return new KvWatchSubscription(this, id);
    }

    internal void Unregister(long id)
    {
        EnterLock();
        try
        {
            for (var i = 0; i < watches.Count; i++)
            {
                if (watches[i].Id != id)
                    continue;
                watches.RemoveAt(i);
                logger?.LogDebug("WatchPrefix: unregistered id={Id}", id);
                break;
            }
        }
        finally
        {
            gate.Exit();
        }
    }

    /// <summary>
    /// Called from the ReplicatedKvStore.Apply when the actual state has changed.
    /// </summary>
    public void Publish(in ChangeRecord rec)
    {
        if (watches.Count == 0)
            return;

        List<WatchRegistration>? targets = null;

        EnterLock();
        try
        {
            var keySpan = rec.Key.Span;

            foreach (var w in watches)
            {
                if (!keySpan.StartsWith(w.Prefix)) continue;
                targets ??= [];
                targets.Add(w);
            }
        }
        finally
        {
            gate.Exit();
        }

        if (targets is null)
            return;

        logger?.LogDebug("Publish: {Count} watchers match keyLen={Key}",
            targets.Count, rec.Key.Length);

        var evt = new KvWatchEvent(
            rec.Op == ChangeRecordOperation.SET ? KvWatchEventKind.Set : KvWatchEventKind.Delete,
            rec.Key,
            rec.Value,
            rec.Timestamp,
            rec.Revision);

        for (var i = 0; i < targets.Count; i++)
        {
            try
            {
                targets[i].Callback(evt);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Publish: watcher callback threw (id={Id})",
                    targets[i].Id);
            }
        }
    }

    /// <summary>
    /// Creates an asynchronous subscription to the prefix.
    /// Returns IAsyncEnumerable events + IAsyncDisposable for unsubscribing.
    /// </summary>
    public KvWatchAsyncSubscription WatchPrefixAsync(
        ReadOnlySpan<byte> prefix,
        int? bufferSize = null)
    {
        if (prefix.Length == 0) throw new ArgumentException("Prefix must be non-empty", nameof(prefix));

        Channel<KvWatchEvent> channel;
        if (bufferSize is { } capacity and > 0)
        {
            var options = new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.DropOldest
            };
            channel = Channel.CreateBounded<KvWatchEvent>(options);
        }
        else
        {
            var options = new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            };
            channel = Channel.CreateUnbounded<KvWatchEvent>(options);
        }

        var subscription = WatchPrefix(prefix, evt => channel.Writer.TryWrite(evt));

        return new KvWatchAsyncSubscription(subscription, channel);
    }
}