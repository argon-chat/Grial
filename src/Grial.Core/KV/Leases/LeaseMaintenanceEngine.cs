namespace Grial.Core.KV.Leases;

public sealed class LeaseMaintenanceEngine(
    LeaseManager leaseManager,
    ILeaseExpirationHandler handler)
{
    public async ValueTask RunAsync(CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromSeconds(1);

        while (!cancellationToken.IsCancellationRequested)
        {
            leaseManager.CollectExpired(handler);

            try
            {
                await Task.Delay(interval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}
