namespace Grial.Core.Network;

public interface IReplicationStream : IAsyncDisposable
{
    Stream Input { get; }
    Stream Output { get; }
}