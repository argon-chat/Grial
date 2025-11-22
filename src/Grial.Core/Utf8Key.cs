namespace Grial.Core;

using System.Text;

public readonly struct Utf8Key(ReadOnlyMemory<byte> bytes) : IEquatable<Utf8Key>
{
    private readonly ReadOnlyMemory<byte> bytes = bytes;
    private readonly int hash = ComputeHash(bytes.Span);

    public Utf8Key(ReadOnlySpan<byte> span)
        : this(new ReadOnlyMemory<byte>(span.ToArray()))
    { }

    public ReadOnlySpan<byte> Span => bytes.Span;
    public ReadOnlyMemory<byte> Memory => bytes;
    public int Length => bytes.Length;

    public static implicit operator Utf8Key(string? value)
    {
        if (value == null)
            return default;

        var arr = Encoding.UTF8.GetBytes(value);
        return new Utf8Key(arr);
    }

    public static implicit operator Utf8Key(ReadOnlyMemory<byte> value) => new(value);
    public static implicit operator Utf8Key(ReadOnlySpan<byte> value) => new(value);

    public static implicit operator string(Utf8Key key) =>
        key.bytes.IsEmpty ? string.Empty : Encoding.UTF8.GetString(key.bytes.Span);

    public bool Equals(Utf8Key other) => bytes.Span.SequenceEqual(other.bytes.Span);

    public override bool Equals(object? obj)
        => obj is Utf8Key k && Equals(k);

    public override int GetHashCode() => hash;

    private static int ComputeHash(ReadOnlySpan<byte> span)
    {
        const int offset = unchecked((int)2166136261);
        const int prime = 16777619;

        var hash = offset;

        foreach (var b in span)
            hash = (hash ^ b) * prime;

        return hash;
    }

    public override string ToString() => this;

    public Utf8Key Clone() => new(bytes.ToArray()); // TODO
}