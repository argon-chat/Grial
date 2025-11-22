namespace Grial.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Core.Storage;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class LogScanBench
{
    SegmentedLogStorage log = null!;
    const int count = 20000;

    struct DummyHandler : ILogEntryHandler
    {
        public int Cnt;

        public void OnEntry(long seq, ReadOnlyMemory<byte> payload)
        {
            if (!payload.IsEmpty)
                _ = payload.Span[0];
            Cnt++;
        }
    }

    [GlobalSetup]
    public void Setup()
    {
        var dir = Path.Combine(Path.GetTempPath(), "seglog-scan-bench-" + Guid.NewGuid());
        log = new SegmentedLogStorage(dir);

        var payload = new byte[256];
        Random.Shared.NextBytes(payload);

        for (int i = 0; i < count; i++)
            log.Append(payload);
    }

    [Benchmark]
    public int Scan_All()
    {
        var handler = new DummyHandler();
        log.ScanFrom(0, count + 10, ref handler);
        return handler.Cnt;
    }
}