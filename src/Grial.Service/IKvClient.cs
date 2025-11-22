namespace Grial.Service;

using Core.Clocks;
using Core.KV.Leases;
using Core.WAL;
using Grial.Core.KV;
using System;
using System.Security.Cryptography;
using System.Text;

public sealed class ArgonKvClient(
    ReplicatedKvStore store,
    ChangeLog log,
    HybridLogicalClock clock,
    LeaseManager leaseManager,
    LeaseKeyIndex leaseKeyIndex)
    
{
    public ValueTask<KvGetResult?> GetAsync(string key, CancellationToken ct = default)
    {
        if (!store.TryGetKvItem(key, out var val))
            return ValueTask.FromResult<KvGetResult?>(null);

        var result = new KvGetResult(
            Key: key,
            Value: val.Value!.Value,
            ValueTimestamp: val.Timestamp
        );
        return ValueTask.FromResult<KvGetResult?>(result);
    }

    public ValueTask<bool> SetAsync(string key, ReadOnlyMemory<byte> value, CancellationToken ct = default)
    {
        var ts = clock.NextLocal();
        var keyBytes = Encoding.UTF8.GetBytes(key);

        var rec = new ChangeRecord(
            0,
            ts,
            op: ChangeRecordOperation.SET,
            key: keyBytes,
            value: value);

        var actual = log.Append(rec);
        store.Apply(actual);

        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> DeleteAsync(string key, CancellationToken ct = default)
    {
        var ts = clock.NextLocal();
        var keyBytes = Encoding.UTF8.GetBytes(key);

        var rec = new ChangeRecord(
            0,
            ts,
            ChangeRecordOperation.DEL,
            keyBytes,
            null);

        var actual = log.Append(rec);
        store.Apply(actual);

        return ValueTask.FromResult(true);
    }

    public ValueTask<long> CreateLeaseAsync(TimeSpan ttl, CancellationToken ct = default)
    {
        var id = leaseManager.CreateLease(ttl);
        return ValueTask.FromResult(id.Id.Value);
    }

    public ValueTask AttachToLeaseAsync(long leaseId, string key, CancellationToken ct = default)
    {
        leaseKeyIndex.AttachKey(new LeaseId(), key);
        return ValueTask.CompletedTask;
    }
}


public readonly record struct KvGetResult(
    string Key,
    ReadOnlyMemory<byte> Value,
    HybridTimestamp ValueTimestamp);