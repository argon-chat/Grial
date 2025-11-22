namespace Grial.Core.KV;

using System.Formats.Cbor;
using WAL;

public struct ScanFromRevisionAdapter<T>(long afterRevision, ref T inner) : IWalEntryHandler
    where T : struct, IWalReplayHandler
{
    private T inner = inner;

    public void OnEntry(long seq, ReadOnlyMemory<byte> payload)
    {
        var reader = new CborReader(payload);
        var rec = ChangeRecordCbor.Read(ref reader);

        if (rec.Revision > afterRevision)
            inner.OnReplay(rec);
    }
}