namespace Grial.Test;

using Core.Clocks;
using Core.KV;
using Core.Network;
using Core.Storage;
using Core.WAL;
using System.Text;
using System.Threading.Channels;

public class ReplicationConnectionTests
{
    [Test]
    public async Task Replication_Roundtrip_Test()
    {
        var srvClock = new HybridLogicalClock("srv");
        var srvWal = new SegmentedLogStorage(
            Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N")), 1024 * 1024);
        var srvLog = new ChangeLog(srvWal, srvClock);
        var srvKv = new ReplicatedKvStore(srvClock, srvLog, new KvWatchManager());

        var clientClock = new HybridLogicalClock("cli");
        var clientWal = new SegmentedLogStorage(
            Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N")), 1024 * 1024);
        var clientLog = new ChangeLog(clientWal, clientClock);
        var clientKv = new ReplicatedKvStore(clientClock, clientLog, new KvWatchManager());

        var server = new ReplicationServer(srvKv, srvLog);
        var client = new ReplicationClient(clientKv, clientLog);

        var rec = srvLog.Append(new ChangeRecord(
            0,
            srvClock.NextLocal(),
            ChangeRecordOperation.SET,
            "foo"u8.ToArray(),
            "bar"u8.ToArray()));

        srvKv.Apply(rec);

        var stream = new TestDuplexPipeStream();
        var sct = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        var serverTask = server.ServeAsync(new DuplexStream(stream.A_Input, stream.A_Output), sct.Token);
        var clientTask = client.ConnectAsync(new DuplexStream(stream.B_Input, stream.B_Output), sct.Token);


        Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await Task.WhenAll(serverTask.AsTask(), clientTask.AsTask());
        });

        clientKv.TryGet("foo"u8, out var got);
        Assert.That(Encoding.UTF8.GetString(got!.Span), Is.EqualTo("bar"));
    }

    [Test]
    public async Task Hello_Handshake_Roundtrip()
    {
        var pipes = new TestDuplexPipeStream();

        await using var conn1 = new ReplicationConnection(pipes.A_Input, pipes.A_Output, leaveOpen: true);
        await using var conn2 = new ReplicationConnection(pipes.B_Input, pipes.B_Output, leaveOpen: true);

        var hello = new HelloMessage(
            ClusterId: "cluster-x",
            NodeId: "node-a",
            RegionId: "eu-north",
            ProtocolVersion: 1,
            Options: null);

        var sendTask = conn1.SendHelloAsync(hello);
        var received = await conn2.ReceiveHelloAsync();

        await sendTask;

        Assert.That(received.ClusterId, Is.EqualTo("cluster-x"));
        Assert.That(received.NodeId, Is.EqualTo("node-a"));
        Assert.That(received.RegionId, Is.EqualTo("eu-north"));
    }

    [Test]
    public async Task WalBatch_Roundtrip_Through_Connection()
    {
        var stream = new TestDuplexPipeStream();
        await using var conn1 = new ReplicationConnection(stream.A_Input, stream.A_Output);
        await using var conn2 = new ReplicationConnection(stream.B_Input, stream.B_Output);

        var ts = new HybridTimestamp(1000, 1, "n1");
        var rec = new ChangeRecord(
            42,
            ts,
            ChangeRecordOperation.SET,
            "foo"u8.ToArray(),
            "bar"u8.ToArray());

        var batch = new WalBatchMessage(
            StreamId: "s1",
            BaseRevision: 42,
            LastRevision: 42,
            Records: [rec]);

        var sendTask = conn1.SendWalBatchAsync(batch);
        var received = await conn2.ReceiveWalBatchAsync();

        await sendTask;

        Assert.That(received.BaseRevision, Is.EqualTo(42));
        Assert.That(received.Records.Length, Is.EqualTo(1));
        Assert.That(received.Records[0].Revision, Is.EqualTo(42));
    }
}