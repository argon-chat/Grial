namespace Grial.Discovery;

using System.Text.Json;

public static class ServiceInstanceJson
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public static byte[] Encode(ServiceInstancePayload payload)
        => JsonSerializer.SerializeToUtf8Bytes(payload, Options);

    public static ServiceInstancePayload Decode(ReadOnlySpan<byte> json)
        => JsonSerializer.Deserialize<ServiceInstancePayload>(json, Options)
           ?? throw new InvalidDataException("Invalid service instance payload JSON");
}