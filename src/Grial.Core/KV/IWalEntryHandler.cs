namespace Grial.Core.KV;

public interface IWalEntryHandler
{
    /// <summary>
    /// Processes the WAL record.
    /// </summary>
    void OnEntry(long seq, ReadOnlyMemory<byte> payload);
}