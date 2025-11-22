namespace Grial.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Grial.Core.Storage;
using System.Text;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class LogAppendBench
{
    SegmentedLogStorage log = null!;
    byte[] smallPayload = null!;
    byte[] mediumPayload = null!;
    byte[] largePayload = null!;

    const int segmentSize = 32 * 1024 * 1024;

    [GlobalSetup]
    public void Setup()
    {
        var dir = Path.Combine(Path.GetTempPath(), "seglog-bench-" + Guid.NewGuid());
        log = new SegmentedLogStorage(dir, segmentSize);

        smallPayload = "hello_world"u8.ToArray();
        mediumPayload = new byte[1024];
        largePayload = new byte[1024 * 128];

        Random.Shared.NextBytes(mediumPayload);
        Random.Shared.NextBytes(largePayload);
    }

    [Benchmark]
    public long Append_Small_10B() => log.Append(smallPayload);

    [Benchmark]
    public long Append_Medium_1KB() => log.Append(mediumPayload);

    [Benchmark]
    public long Append_Large_128KB() => log.Append(largePayload);
}