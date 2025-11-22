namespace Grial.Test;

using System.Reflection.Emit;
using System.Text;
using Core.Clocks;
using Core.KV;
using Core.Storage;
using Core.WAL;

public class Tests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void FullStepsTest()
    {
        var wal = new SegmentedLogStorage(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N")));
        var clock = new HybridLogicalClock(nodeId: "ru-east-1");
        var changelog = new ChangeLog(wal, clock);
        var store = new ReplicatedKvStore(clock, changelog, new KvWatchManager());

        var handler = new ApplyHandler(store);

        changelog.ScanFrom(0, int.MaxValue, ref handler);

        var rec = new ChangeRecord(
            0,
            clock.NextLocal(), 
            op: ChangeRecordOperation.SET,
            key: "foo"u8.ToArray(),
            value: "bar"u8.ToArray()
        );
        changelog.Append(rec);
    }
}
