namespace Grial.Core.KV;

public sealed class KvWatchSubscription : IAsyncDisposable
{
    private readonly KvWatchManager manager;
    private readonly long id;

    internal KvWatchSubscription(KvWatchManager manager, long id)
    {
        this.manager = manager;
        this.id = id;
    }

    public ValueTask DisposeAsync()
    {
        manager.Unregister(id);
        return ValueTask.CompletedTask;
    }
}