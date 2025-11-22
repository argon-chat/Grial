namespace Grial.Core.WAL;

using KV.Leases;

public class WalSchedulerOptions
{
    public TimeSpan leaseCheckPeriod { get; set; } = TimeSpan.FromSeconds(12);
    public TimeSpan snapshotCheckPeriod { get; set; } = TimeSpan.FromSeconds(32);
}

public sealed class WalScheduler(
    SnapshotEffluentScheduler snapshotScheduler,
    LeaseManager leaseManager,
    LeaseExpirationHandler leaseExpirationHandler,
    WalSchedulerOptions options
)
{
    private long lastLeaseCheck = 0;
    private long lastSnapshotCheck = 0;

    private TimeSpan leaseCheckPeriod => options.leaseCheckPeriod;
    private TimeSpan snapshotCheckPeriod => options.snapshotCheckPeriod;

    public async ValueTask RunOnceAsync(CancellationToken ct = default)
    {
        var now = Environment.TickCount64;

        if (now - lastLeaseCheck >= leaseCheckPeriod.TotalMilliseconds)
        {
            lastLeaseCheck = now;
            ProcessExpiredLeases(now);
        }

        if (now - lastSnapshotCheck >= snapshotCheckPeriod.TotalMilliseconds)
        {
            lastSnapshotCheck = now;
            await snapshotScheduler.RunOnceAsync(ct);
        }
    }

    private void ProcessExpiredLeases(long nowTick) => leaseManager.CollectExpired(leaseExpirationHandler);
}

public sealed class SnapshotEffluentScheduler(
    SnapshotManager snapshotManager,
    ChangeLog changeLog,
    SnapshotEffluentSchedulerOptions options)
{
    private long lastSnapshotSeq = 0;

    public async ValueTask<bool> RunOnceAsync(CancellationToken cancellationToken = default)
    {
        var currentSeq = changeLog.LastSeq;

        if (currentSeq < options.MinSeqToStartSnapshots)
            return false;

        var delta = currentSeq - lastSnapshotSeq;

        if (delta < options.MinSeqDeltaForSnapshot)
            return false;

        await snapshotManager.CreateSnapshotAsync();

        lastSnapshotSeq = currentSeq;
        return true;
    }

    public async ValueTask ForceSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var currentSeq = changeLog.LastSeq;
        await snapshotManager.CreateSnapshotAsync();
        lastSnapshotSeq = currentSeq;
    }
}