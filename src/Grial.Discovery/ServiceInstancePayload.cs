namespace Grial.Discovery;

using Core.KV.Leases;

public readonly record struct ServiceInstanceAddress(string Host, int Port);

public sealed record ServiceInstancePayload(
    string ServiceName,
    string InstanceId,
    ServiceInstanceAddress Address,
    IReadOnlyDictionary<string, string>? Metadata,
    long LeaseId
);

public sealed class ServiceRegistrationHandle(
    string serviceName,
    string instanceId,
    LeaseId leaseId,
    long revision)
{
    public string ServiceName { get; } = serviceName;
    public string InstanceId { get; } = instanceId;
    public LeaseId LeaseId { get; } = leaseId;
    public long Revision { get; internal set; } = revision;
}