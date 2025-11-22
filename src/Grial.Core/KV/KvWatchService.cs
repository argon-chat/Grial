namespace Grial.Core.KV;

using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using WAL;

public sealed class KvWatchService(
    KvWatchManager watchManager,
    ChangeLog changeLog,
    ILogger<KvWatchService>? logger = null)
{
    public IAsyncEnumerable<KvWatchEvent> WatchPrefix(
        Utf8Key prefix,
        long fromRevision,
        CancellationToken ct = default)
    {
        return WatchPrefixCore(prefix, fromRevision, ct);
    }

    private async IAsyncEnumerable<KvWatchEvent> WatchPrefixCore(
        Utf8Key prefix,
        long fromRevision,
        [EnumeratorCancellation] CancellationToken ct)
    {
        logger?.LogDebug("WatchPrefix: prefixLen={Len}, fromRev={Rev}", prefix.Length, fromRevision);

        // 1. Replay history
        if (fromRevision < changeLog.LastRevision)
        {
            logger?.LogDebug("WatchPrefix: replay history from rev={Rev}", fromRevision);

            var handler = new PrefixReplayHandler(prefix.Span, fromRevision);

            changeLog.ScanFromRevision(fromRevision, ref handler);

            for (var index = 0; index < handler.Events.Length; index++)
            {
                var ev = handler.Events[index];
                yield return ev;
            }
        }

        // 2. Live subscription
        logger?.LogDebug("WatchPrefix: subscribing for live updates");

        await using var sub = watchManager.WatchPrefixAsync(prefix.Span);

        await foreach (var ev in sub.WithCancellation(ct))
        {
            logger?.LogTrace("WatchPrefix: live event rev={Rev}", ev.Revision);
            yield return ev;
        }
    }
}