namespace Grial.Service;

using Core.WAL;

public sealed class WalSchedulerRunner(WalScheduler impl) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await impl.RunOnceAsync(stoppingToken);
            await Task.Delay(100, stoppingToken);
        }
    }
}

public sealed class SnapshotEffluentSchedulerRunner(
    SnapshotEffluentScheduler engine,
    SnapshotEffluentSchedulerOptions options)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (options.InitialDelay > TimeSpan.Zero)
        {
            try
            {
                await Task.Delay(options.InitialDelay, stoppingToken);
            }
            catch
            {
                return;
            }
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await engine.RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
            }

            try
            {
                await Task.Delay(options.SnapshotInterval, stoppingToken);
            }
            catch
            {
                break;
            }
        }
    }
}