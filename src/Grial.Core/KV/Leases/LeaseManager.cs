namespace Grial.Core.KV.Leases;

using Microsoft.Extensions.Logging;
using System;
using System.Runtime.CompilerServices;

public sealed class LeaseManager(TimeProvider timeProvider, ILogger<LeaseManager>? logger = null)
{
    private readonly Lock sync = new();
    private readonly Dictionary<long, LeaseEntry> leases = new();
    private readonly PriorityQueue<long, long> expiryQueue = new(); // element = leaseId, priority = expireAtMillis
    readonly TimeProvider timeProvider = timeProvider ?? TimeProvider.System;
    private long nextId = 0;

    public int LeaseCount
    {
        get
        {
            lock (sync)
            {
                return leases.Count;
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static long NowMillis(TimeProvider provider)
        => provider.GetUtcNow().ToUnixTimeMilliseconds();

    /// <summary>
    /// Creates a new lease with the specified TTL.
    /// </summary>
    public LeaseEntry Acquire(TimeSpan ttl)
    {
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl));

        var now = NowMillis(timeProvider);
        var id = Interlocked.Increment(ref nextId);
        var leaseId = new LeaseId(id);
        var entry = new LeaseEntry(leaseId, now + (long)ttl.TotalMilliseconds, ttl);

        lock (sync)
        {
            leases[id] = entry;
        }

        logger?.LogDebug(
            "Acquire: id={Id}, ttlMs={TtlMs}, expireAt={Expire}",
            id,
            (long)ttl.TotalMilliseconds,
            entry.ExpireAtMillis);

        return entry;
    }

    /// <summary>
    /// Trying to get an active lease (not expired).
    /// </summary>
    public bool TryGetActive(LeaseId id, long nowMillis, out LeaseEntry entry)
    {
        lock (sync)
        {
            if (!leases.TryGetValue(id.Value, out entry))
            {
                logger?.LogTrace("TryGetActive: id={Id} not found", id.Value);
                return false;
            }

            if (entry.ExpireAtMillis > nowMillis)
                return true;

            logger?.LogTrace(
                "TryGetActive: id={Id} expired, expireAt={Expire}, now={Now}",
                id.Value,
                entry.ExpireAtMillis,
                nowMillis);

            return false;
        }
    }

    /// <summary>
    /// Updates the TTL (keep-alive). If the lease has already expired, it returns false.
    /// </summary>
    public bool TryKeepAlive(LeaseId id, TimeSpan? newTtl, out LeaseEntry updated)
    {
        var now = NowMillis(timeProvider);

        lock (sync)
        {
            if (!leases.TryGetValue(id.Value, out var current))
            {
                updated = default;
                logger?.LogDebug("KeepAlive: id={Id} not found", id.Value);
                return false;
            }

            if (current.ExpireAtMillis <= now)
            {
                leases.Remove(id.Value);
                updated = default;

                logger?.LogDebug(
                    "KeepAlive: id={Id} expired before keepalive (expireAt={Expire}, now={Now})",
                    id.Value,
                    current.ExpireAtMillis,
                    now);

                return false;
            }

            var ttl = newTtl ?? current.Ttl;

            if (ttl <= TimeSpan.Zero)
            {
                updated = current;
                logger?.LogWarning(
                    "KeepAlive: id={Id} ignored zero/negative TTL {TTL}",
                    id.Value,
                    ttl);
                return true;
            }

            var newExpire = now + (long)ttl.TotalMilliseconds;

            updated = new LeaseEntry(id, newExpire, ttl);
            leases[id.Value] = updated;

            logger?.LogDebug(
                "KeepAlive: id={Id}, newExpire={Expire}, ttlMs={TTL}",
                id.Value,
                newExpire,
                (long)ttl.TotalMilliseconds);

            return true;
        }
    }


    /// <summary>
    /// Explicit termination of lease (for example, when explicitly closing the session).
    /// Key eviction will be done via LeaseExpirationHandler on the next GC pass.
    /// </summary>
    public bool TryRelease(LeaseId id)
    {
        lock (sync)
        {
            var removed = leases.Remove(id.Value);

            logger?.LogDebug("Release: id={Id}, removed={Removed}", id.Value, removed);

            return removed;
        }
    }

    /// <summary>
    /// Create a new lease with TTL starting "now".
    /// </summary>
    public LeaseEntry CreateLease(TimeSpan ttl)
    {
        var now = NowMillis(timeProvider);
        return CreateLeaseAt(now, ttl);
    }

    /// <summary>
    /// Create a new lease, counting from the specified time (for tests/simulations).
    /// </summary>
    public LeaseEntry CreateLeaseAt(long nowMillis, TimeSpan ttl)
    {
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentException("TTL must be > 0", nameof(ttl));

        lock (sync)
        {
            var idValue = Interlocked.Increment(ref nextId);
            var leaseId = new LeaseId(idValue);
            var expireAt = nowMillis + (long)ttl.TotalMilliseconds;

            var entry = new LeaseEntry(leaseId, expireAt, ttl);

            leases[idValue] = entry;
            expiryQueue.Enqueue(idValue, expireAt);

            logger?.LogDebug(
                "CreateLeaseAt: id={Id}, ttlMs={TTL}, expireAt={Expire}",
                idValue,
                (long)ttl.TotalMilliseconds,
                expireAt);

            return entry;
        }
    }

    /// <summary>
    /// Try to extend lease (reset TTL from current time).
    /// Returns updated LeaseEntry, or false if lease no longer exists/expired.
    /// </summary>
    public bool TryRenew(LeaseId leaseId, TimeSpan ttl, out LeaseEntry renewed)
    {
        var now = NowMillis(timeProvider);
        return TryRenewAt(leaseId, now, ttl, out renewed);
    }

    public bool TryRenewAt(LeaseId leaseId, long nowMillis, TimeSpan ttl, out LeaseEntry renewed)
    {
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentException("TTL must be > 0", nameof(ttl));

        lock (sync)
        {
            if (!leases.TryGetValue(leaseId.Value, out var existing))
            {
                renewed = default;

                logger?.LogDebug("Renew: id={Id} not found", leaseId.Value);
                return false;
            }

            var expireAt = nowMillis + (long)ttl.TotalMilliseconds;

            renewed = new LeaseEntry(leaseId, expireAt, ttl);

            leases[leaseId.Value] = renewed;
            expiryQueue.Enqueue(leaseId.Value, expireAt);

            logger?.LogDebug(
                "Renew: id={Id}, ttlMs={TTL}, expireAt={Expire}",
                leaseId.Value,
                (long)ttl.TotalMilliseconds,
                expireAt);

            return true;
        }
    }

    /// <summary>
    /// Try to forcibly remove lease.
    /// It will disappear and will no longer expire.
    /// </summary>
    public bool TryRevoke(LeaseId leaseId)
    {
        lock (sync)
        {
            var removed = leases.Remove(leaseId.Value);

            logger?.LogInformation(
                "Revoke: id={Id}, removed={Removed}",
                leaseId.Value,
                removed);

            return removed;
        }
    }

    /// <summary>
    /// Get the current lease record if it is still alive (not expired or deleted).
    /// </summary>
    public bool TryGet(LeaseId leaseId, out LeaseEntry entry)
    {
        lock (sync)
        {
            if (!leases.TryGetValue(leaseId.Value, out var e))
            {
                entry = default;
                logger?.LogTrace("TryGet: id={Id} not found", leaseId.Value);
                return false;
            }

            var now = NowMillis(timeProvider);

            if (e.ExpireAtMillis <= now)
            {
                entry = default;

                logger?.LogTrace(
                    "TryGet: id={Id} expired (expireAt={Expire}, now={Now})",
                    leaseId.Value,
                    e.ExpireAtMillis,
                    now);

                return false;
            }

            entry = e;
            return true;
        }
    }

    /// <summary>
    /// Collect expired leases and call handler for each one.
    /// Returns the number of actually expired and processed ones.
    /// </summary>
    public int CollectExpired(ILeaseExpirationHandler handler)
    {
        var now = NowMillis(timeProvider);
        return CollectExpiredAt(now, handler);
    }

    public int CollectExpiredAt(long nowMillis, ILeaseExpirationHandler handler)
    {
        var expiredCount = 0;

        lock (sync)
        {
            while (expiryQueue.Count > 0)
            {
                expiryQueue.TryPeek(out var idValue, out var expireAt);

                if (expireAt > nowMillis)
                    break;

                expiryQueue.Dequeue();

                if (!leases.TryGetValue(idValue, out var entry))
                    continue;

                if (entry.ExpireAtMillis != expireAt)
                    continue;

                leases.Remove(idValue);
                expiredCount++;

                logger?.LogDebug(
                    "LeaseExpired: id={Id}, expireAt={Expire}, now={Now}",
                    idValue,
                    expireAt,
                    nowMillis);

                handler.OnLeaseExpired(entry);
            }

            if (expiredCount > 0)
            {
                logger?.LogInformation("CollectExpired: expired={Count}", expiredCount);
            }
        }

        return expiredCount;
    }


    public LeaseEntry[] ExportAllActive(long nowMillis)
    {
        lock (sync)
        {
            if (leases.Count == 0)
                return [];

            var result = new LeaseEntry[leases.Count];
            var i = 0;

            foreach (var entry in leases.Values.Where(entry => entry.ExpireAtMillis > nowMillis)) 
                result[i++] = entry;

            if (i != result.Length)
                Array.Resize(ref result, i);

            logger?.LogDebug("ExportAllActive: count={Count}", i);

            return result;
        }
    }


    public void RestoreFromSnapshot(ReadOnlySpan<LeaseEntry> entries)
    {
        lock (sync)
        {
            leases.Clear();
            expiryQueue.Clear();
            nextId = 0;

            var loaded = 0;
            var now = NowMillis(timeProvider);

            foreach (var entry in entries)
            {
                if (entry.ExpireAtMillis <= now)
                    continue;

                var idValue = entry.Id.Value;

                leases[idValue] = entry;
                expiryQueue.Enqueue(idValue, entry.ExpireAtMillis);
                loaded++;

                if (idValue > nextId)
                    nextId = idValue;
            }

            logger?.LogInformation(
                "RestoreFromSnapshot: loaded={Loaded}, maxId={MaxId}",
                loaded,
                nextId);
        }
    }
}