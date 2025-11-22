namespace Grial.Discovery;

using Core.KV;
using Core.KV.Leases;

public sealed class ServiceRegistry(EphemeralSessionStore ephemeral, ReplicatedKvStore kv)
{
    /// <summary>
    /// Registers the service instance as ephemeral key:
    /// - creates lease with the specified TTL
    /// - writes an entry to KV (ephemeral.PutEphemeral)
    /// - returns handle with LeaseId and Revision.
    ///
    /// If necessary, then you can hang it on top of the heartbeat.
    /// </summary>
    public ServiceRegistrationHandle RegisterInstance(
        string serviceName,
        string instanceId,
        ServiceInstanceAddress address,
        TimeSpan ttl,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        if (string.IsNullOrWhiteSpace(serviceName))
            throw new ArgumentException("Service name required", nameof(serviceName));
        if (string.IsNullOrWhiteSpace(instanceId))
            throw new ArgumentException("Instance id required", nameof(instanceId));
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl));

        var lease = ephemeral.OpenSession(ttl);

        var payload = new ServiceInstancePayload(
            serviceName,
            instanceId,
            address,
            metadata,
            lease.Id.Value);

        var valueBytes = ServiceInstanceJson.Encode(payload);
        var keyBytes = ServiceRegistryKeys.BuildInstanceKey(serviceName, instanceId);

        if (!ephemeral.PutEphemeral(lease.Id, keyBytes, valueBytes, out var revision))
            throw new InvalidOperationException("Failed to register instance: lease not active");

        return new ServiceRegistrationHandle(serviceName, instanceId, lease.Id, revision);
    }

    public bool UnregisterInstance(ServiceRegistrationHandle handle)
    {
        var keyBytes = ServiceRegistryKeys.BuildInstanceKey(handle.ServiceName, handle.InstanceId);
        return kv.TryCompareAndDelete(keyBytes, handle.Revision, out _);
    }

    public bool KeepAlive(ServiceRegistrationHandle handle, TimeSpan? newTtl, out LeaseEntry updated)
    {
        return ephemeral.TryKeepAlive(handle.LeaseId, newTtl, out updated);
    }

    public ServiceInstancePayload[] GetInstances(string serviceName)
    {
        if (string.IsNullOrWhiteSpace(serviceName))
            throw new ArgumentException("Service name required", nameof(serviceName));

        var prefix = ServiceRegistryKeys.BuildServicePrefix(serviceName);
        var items = kv.GetByPrefix(prefix);

        if (items.Length == 0)
            return Array.Empty<ServiceInstancePayload>();

        var result = new List<ServiceInstancePayload>(items.Length);

        foreach (ref readonly var item in items.AsSpan())
        {
            if (!item.Value.HasValue)
                continue;

            try
            {
                var payload = ServiceInstanceJson.Decode(item.Value.Value.Span);
                result.Add(payload);
            }
            catch
            {
            }
        }

        return result.ToArray();
    }
}