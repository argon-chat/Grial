namespace Grial.Core.KV.Leases;

public readonly struct LeaseId(long value) : IEquatable<LeaseId>
{
    public long Value { get; } = value;

    public bool Equals(LeaseId other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is LeaseId other && Equals(other);
    public override int GetHashCode() => Value.GetHashCode();

    public static bool operator ==(LeaseId left, LeaseId right) => left.Value == right.Value;
    public static bool operator !=(LeaseId left, LeaseId right) => left.Value != right.Value;

    public override string ToString() => Value.ToString();
}

public readonly struct LeaseEntry(LeaseId id, long expireAtMillis, TimeSpan ttl)
{
    public LeaseId Id { get; } = id;
    public long ExpireAtMillis { get; } = expireAtMillis;
    public TimeSpan Ttl { get; } = ttl;

    public override string ToString() => $"Lease [{Id.Value}]({Ttl})";
}