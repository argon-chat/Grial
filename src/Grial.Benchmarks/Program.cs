using BenchmarkDotNet.Running;
using Grial.Benchmarks;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);