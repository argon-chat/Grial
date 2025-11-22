namespace Grial.Core.KV;

using System.Threading.Channels;

public sealed class KvWatchAsyncSubscription(KvWatchSubscription inner, Channel<KvWatchEvent> channel)
    : IAsyncEnumerable<KvWatchEvent>, IAsyncDisposable
{
    private readonly CancellationTokenSource cts = new();

    public async IAsyncEnumerator<KvWatchEvent> GetAsyncEnumerator(
        CancellationToken cancellationToken = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
        var reader = channel.Reader;

        while (await reader.WaitToReadAsync(linkedCts.Token).ConfigureAwait(false))
        {
            while (reader.TryRead(out var evt))
                yield return evt;
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await cts.CancelAsync();
        }
        catch
        {
            // ignore
        }

        await inner.DisposeAsync().ConfigureAwait(false);

        channel.Writer.TryComplete();
        cts.Dispose();
    }
}