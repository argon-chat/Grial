namespace Grial.Test;

using Core.Clocks;
using Core.KV;
using Core.KV.Leases;
using Core.Storage;
using Core.WAL;

public class EphemeralTests
{
    [Test]
    public async Task Ephemeral_Key_Is_Removed_After_Lease_Expires()
    {
        using var dir = new TempDir();
        var wal = new SegmentedLogStorage(dir.Path, 1024 * 1024);
        var clock = new HybridLogicalClock("nodeA");
        var changeLog = new ChangeLog(wal, clock);
        var wch = new KvWatchManager();
        var kv = new ReplicatedKvStore(clock, changeLog, wch);
        var leaseManager = new LeaseManager(TimeProvider.System);
        var leaseKeyIndex = new LeaseKeyIndex();

        var expirationHandler = new LeaseExpirationHandler(leaseKeyIndex, kv, clock, changeLog);
        var sessionStore = new EphemeralSessionStore(kv, changeLog, clock, leaseManager, leaseKeyIndex);

        var lease = sessionStore.OpenSession(TimeSpan.FromMilliseconds(100));

        var ok = sessionStore.PutEphemeral(lease.Id, "foo", "bar"u8, out var rev);
        Assert.That(ok, Is.True);
        Assert.That(rev, Is.GreaterThan(0));

        kv.TryGet("foo", out var got);
        Assert.That(System.Text.Encoding.UTF8.GetString(got!.Span), Is.EqualTo("bar"));

        await Task.Delay(150);

        var now = TimeProvider.System.GetUtcNow().ToUnixTimeMilliseconds();
        var active = leaseManager.ExportAllActive(now);
        Assert.That(active.Length, Is.EqualTo(0));

        expirationHandler.OnLeaseExpired(lease);

        var stillHas = kv.TryGet("foo", out _);
        Assert.That(stillHas, Is.False);
    }
}