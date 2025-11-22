namespace Grial.Core.KV;

using Clocks;
using Leases;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using WAL;

public sealed class EphemeralSessionStore(
    ReplicatedKvStore kv,
    ChangeLog log,
    HybridLogicalClock clock,
    LeaseManager leaseManager,
    LeaseKeyIndex leaseKeyIndex,
    TimeProvider? timeProvider = null,
    ILogger<EphemeralSessionStore>? logger = null)
{
    readonly TimeProvider timeProvider = timeProvider ?? TimeProvider.System;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    long NowMillis() => timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    /// <summary>
    /// Create a new ephemeral session (lease) with TTL.
    /// </summary>
    public LeaseEntry OpenSession(TimeSpan ttl)
    {
        var entry = leaseManager.Acquire(ttl);

        logger?.LogInformation(
            "OpenSession: leaseId={LeaseId}, ttlMs={TTL}, expireAt={Expire}",
            entry.Id.Value,
            (long)ttl.TotalMilliseconds,
            entry.ExpireAtMillis);

        return entry;
    }

    /// <summary>
    /// Update the TTL of the session (keep-alive).
    /// </summary>
    public bool TryKeepAlive(LeaseId leaseId, TimeSpan? newTtl, out LeaseEntry updated)
    {
        var ok = leaseManager.TryKeepAlive(leaseId, newTtl, out updated);

        if (ok)
        {
            logger?.LogDebug(
                "KeepAlive: leaseId={LeaseId}, newExpireAt={Expire}, ttlMs={TTL}",
                leaseId.Value,
                updated.ExpireAtMillis,
                (long)(newTtl ?? updated.Ttl).TotalMilliseconds);
        }
        else
        {
            logger?.LogDebug(
                "KeepAlive failed: leaseId={LeaseId} not active",
                leaseId.Value);
        }

        return ok;
    }

    /// <summary>
    /// Explicitly terminate the session (lease).
    /// Keys will be deleted the next time GC passes through LeaseExpirationHandler.
    /// </summary>
    public bool TryCloseSession(LeaseId leaseId)
    {
        var ok = leaseManager.TryRelease(leaseId);

        logger?.LogInformation(
            "CloseSession: leaseId={LeaseId}, removed={Removed}",
            leaseId.Value,
            ok);

        return ok;
    }

    /// <summary>
    /// Creates an ephemeral key linked to lease.
    /// If lease is inactive (none / expired) — returns false.
    ///
    /// Inside:
    /// - check lease by time
    /// - write SET to WAL+KV
    /// - register key → lease in LeaseKeyIndex
    /// </summary>
    public bool PutEphemeral(
        LeaseId leaseId,
        Utf8Key key,
        ReadOnlySpan<byte> value,
        out long revision)
    {
        revision = 0;

        var now = NowMillis();

        // 1. check lease
        if (!leaseManager.TryGetActive(leaseId, now, out _))
        {
            logger?.LogDebug(
                "PutEphemeral rejected: leaseId={LeaseId} inactive",
                leaseId.Value);

            return false;
        }
        var valueBytes = value.ToArray();

        var ts = clock.NextLocal();

        try
        {
            // 2. Append в WAL
            var rec = log.Append(new ChangeRecord(
                0,
                ts,
                ChangeRecordOperation.SET,
                key: key,
                value: valueBytes));

            revision = rec.Revision;

            // 3. Apply в KV
            kv.Apply(rec);

            // 4. attach key → lease
            leaseKeyIndex.AttachKey(leaseId, key);

            logger?.LogDebug(
                "PutEphemeral: leaseId={LeaseId}, keyLen={KeyLen}, valueLen={ValueLen}, rev={Revision}",
                leaseId.Value,
                key.Length,
                valueBytes.Length,
                revision);

            return true;
        }
        catch (Exception ex)
        {
            logger?.LogError(
                ex,
                "PutEphemeral failed: leaseId={LeaseId}, keyLen={KeyLen}, valueLen={ValueLen}",
                leaseId.Value,
                key.Length,
                valueBytes.Length);

            return false;
        }
    }
}