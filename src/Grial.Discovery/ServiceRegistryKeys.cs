namespace Grial.Discovery;

using Core;

internal static class ServiceRegistryKeys
{
    public static Utf8Key BuildInstanceKey(string serviceName, string instanceId) 
        => $"services/{serviceName}/instances/{instanceId}";

    public static Utf8Key BuildServicePrefix(string serviceName) 
        => $"services/{serviceName}/instances/";
}