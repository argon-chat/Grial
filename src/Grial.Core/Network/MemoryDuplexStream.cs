namespace Grial.Core.Network;

public sealed class MemoryDuplexStream : IReplicationStream
{
    public Stream Input { get; } = new MemoryStream();
    public Stream Output { get; } = new MemoryStream();

    public ValueTask DisposeAsync()
    {
        Input.Dispose();
        Output.Dispose();
        return ValueTask.CompletedTask;
    }
}

public sealed class DuplexStream(Stream input, Stream output) : IReplicationStream
{
    public Stream Input { get; } = input;
    public Stream Output { get; } = output;

    public ValueTask DisposeAsync()
    {
        return new ValueTask();
    }
}