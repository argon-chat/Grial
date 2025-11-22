namespace Grial.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Core.KV;
using Grial.Core;
using System.Text;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class RadixPrefixBench
{
    [Params(500_000)]
    public int KeyCount;

    [Params("a", "ab", "abc", "zzz")]
    public string Prefix;

    private RadixKeyIndex radix = null!;
    private Utf8Key prefixBytes = null!;

    // --------- Setup -----------------------------------------------------

    [GlobalSetup]
    public void Setup()
    {
        radix = new RadixKeyIndex();
        prefixBytes = Prefix;

        // Создаём заранее предсказуемые ключи с префиксами
        // чтобы VisitByPrefix реально что-то находил
        var rnd = new Random(12345);

        for (int i = 0; i < KeyCount; i++)
        {
            // Пример ключей:
            // a/00129387
            // ab/01928374
            // abc/9128374
            // zzz/random
            string key = GenerateKey(rnd);

            radix.Add(key);
        }
    }

    private string GenerateKey(Random rnd)
    {
        // распределяем ключи по наборам:
        int p = rnd.Next(0, 4);

        return p switch
        {
            0 => "a/" + rnd.NextInt64().ToString(),
            1 => "ab/" + rnd.NextInt64().ToString(),
            2 => "abc/" + rnd.NextInt64().ToString(),
            _ => "zzz/" + rnd.NextInt64().ToString()
        };
    }

    // -------- Benchmark Target ------------------------------------------

    [Benchmark]
    public int Visit_By_Prefix()
    {
        var visitor = new CounterVisitor();
        radix.VisitByPrefix(prefixBytes, visitor);
        return visitor.Count;
    }

    // Visitor без аллокаций — только считает

    sealed class CounterVisitor : IKeyVisitor
    {
        public int Count;

        public void OnKey(Utf8Key key)
        {
            Count++;
        }
    }
}