namespace Grial.Core.KV.Leases;

using Clocks;
using Microsoft.Extensions.Logging;
using WAL;

public interface ILeaseExpirationHandler
{
    void OnLeaseExpired(LeaseEntry lease);
}

public sealed class LeaseExpirationHandler(
    LeaseKeyIndex leaseKeyIndex,
    ReplicatedKvStore kvStore,
    HybridLogicalClock clock,
    ChangeLog? changeLog = null,
    ILogger<ILeaseExpirationHandler>? logger = null)
    : ILeaseExpirationHandler
{
    public void OnLeaseExpired(LeaseEntry lease)
    {
        var leaseId = lease.Id.Value;

        logger?.LogInformation(
            "LeaseExpired: leaseId={LeaseId}, expireAt={ExpireAt}, ttlMs={Ttl}",
            leaseId,
            lease.ExpireAtMillis,
            (long)lease.Ttl.TotalMilliseconds);

        var keys = leaseKeyIndex.TakeKeysForLease(lease.Id);

        if (keys.Length == 0)
        {
            logger?.LogDebug("LeaseExpired: leaseId={LeaseId} has no keys", leaseId);
            return;
        }

        logger?.LogDebug(
            "LeaseExpired: leaseId={LeaseId} removing {Count} keys",
            leaseId,
            keys.Length);

        foreach (var key in keys)
        {
            try
            {
                var ts = clock.NextLocal();

                var baseRecord = new ChangeRecord(
                    0,
                    ts,
                    ChangeRecordOperation.DEL,
                    key: key,
                    value: null);

                var finalRecord = changeLog?.Append(baseRecord) ?? baseRecord;

                logger?.LogDebug(
                    "LeaseExpired: deleting keyLen={KeyLen}, rev={Rev}, leaseId={LeaseId}",
                    key.Length,
                    finalRecord.Revision,
                    leaseId);

                kvStore.Apply(finalRecord);
            }
            catch (Exception ex)
            {
                logger?.LogError(
                    ex,
                    "LeaseExpired: failed to delete key (len={KeyLen}) for leaseId={LeaseId}",
                    key.Length,
                    leaseId);
            }
        }
    }
}