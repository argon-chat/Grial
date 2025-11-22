namespace Grial.Core.KV;

using WAL;

public interface IWalReplayHandler
{
    void OnReplay(in ChangeRecord rec);
}