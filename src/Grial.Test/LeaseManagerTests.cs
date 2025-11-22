namespace Grial.Test;

using Core.KV;
using Core.KV.Leases;

[TestFixture]
public sealed class LeaseManagerTests
{
    [Test]
    public void Create_And_Expire_Lease()
    {
        var lm = new LeaseManager(TimeProvider.System);

        var ttl = TimeSpan.FromMilliseconds(100);
        var now = TimeProvider.System.GetUtcNow().ToUnixTimeMilliseconds();

        var lease = lm.CreateLeaseAt(now, ttl);

        Assert.That(lm.LeaseCount, Is.EqualTo(1));

        var handler = new TestExpirationHandler();

        lm.CollectExpiredAt(now + 50, handler);
        Assert.That(handler.Expired.Count, Is.EqualTo(0));
        Assert.That(lm.LeaseCount, Is.EqualTo(1));

        lm.CollectExpiredAt(now + 150, handler);
        Assert.That(handler.Expired.Count, Is.EqualTo(1));
        Assert.That(handler.Expired[0].Id, Is.EqualTo(lease.Id));
        Assert.That(lm.LeaseCount, Is.EqualTo(0));
    }

    [Test]
    public void Renew_Works()
    {
        var lm = new LeaseManager(TimeProvider.System);

        var ttl = TimeSpan.FromMilliseconds(100);
        var now = TimeProvider.System.GetUtcNow().ToUnixTimeMilliseconds();

        var lease = lm.CreateLeaseAt(now, ttl);

        var handler = new TestExpirationHandler();

        Assert.That(
            lm.TryRenewAt(lease.Id, now + 90, TimeSpan.FromMilliseconds(200), out var renewed),
            Is.True);

        lm.CollectExpiredAt(now + 150, handler);
        Assert.That(handler.Expired.Count, Is.EqualTo(0));

        lm.CollectExpiredAt(now + 320, handler);
        Assert.That(handler.Expired.Count, Is.EqualTo(1));
        Assert.That(handler.Expired[0].Id, Is.EqualTo(lease.Id));
    }

    [Test]
    public void Revoke_Removes_Lease()
    {
        var lm = new LeaseManager(TimeProvider.System);

        var ttl = TimeSpan.FromSeconds(10);
        var now = TimeProvider.System.GetUtcNow().ToUnixTimeMilliseconds();

        var lease = lm.CreateLeaseAt(now, ttl);

        Assert.That(lm.TryRevoke(lease.Id), Is.True);
        Assert.That(lm.LeaseCount, Is.EqualTo(0));

        var handler = new TestExpirationHandler();
        lm.CollectExpiredAt(now + 999999, handler);

        Assert.That(handler.Expired.Count, Is.EqualTo(0));
    }

    sealed class TestExpirationHandler : ILeaseExpirationHandler
    {
        public readonly List<LeaseEntry> Expired = new();

        public void OnLeaseExpired(LeaseEntry lease)
        {
            Expired.Add(lease);
        }
    }
}