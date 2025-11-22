namespace Grial.Test;

using Core.Network;

public class ReplicationFrameIOTests
{
    [Test]
    public async Task Frame_Roundtrip_Works()
    {
        var ms = new MemoryStream();

        var original = new byte[] { 10, 20, 30, 40 };

        await ReplicationFrameIO.WriteFrameAsync(
            ms,
            w => w.WriteByteString(original));

        ms.Position = 0;

        var frame = await ReplicationFrameIO.ReadFrameAsync(ms);
        Assert.That(frame, Is.Not.Null);

        var reader = new System.Formats.Cbor.CborReader(frame);
        var parsed = reader.ReadByteString();

        Assert.That(parsed, Is.EqualTo(original));
    }

    [Test]
    public async Task Frame_Multiple_Frames_Sequential()
    {
        var ms = new MemoryStream();

        await ReplicationFrameIO.WriteFrameAsync(ms, w => w.WriteInt32(100));
        await ReplicationFrameIO.WriteFrameAsync(ms, w => w.WriteInt32(200));
        await ReplicationFrameIO.WriteFrameAsync(ms, w => w.WriteInt32(300));

        ms.Position = 0;

        var f1 = await ReplicationFrameIO.ReadFrameAsync(ms);
        var f2 = await ReplicationFrameIO.ReadFrameAsync(ms);
        var f3 = await ReplicationFrameIO.ReadFrameAsync(ms);

        Assert.That(f1, Is.Not.Null);
        Assert.That(f2, Is.Not.Null);
        Assert.That(f3, Is.Not.Null);

        Assert.That(ReadInt(f1), Is.EqualTo(100));
        Assert.That(ReadInt(f2), Is.EqualTo(200));
        Assert.That(ReadInt(f3), Is.EqualTo(300));
    }

    [Test]
    public async Task Frame_EOF_Returns_Null()
    {
        var ms = new MemoryStream();

        var frame = await ReplicationFrameIO.ReadFrameAsync(ms);

        Assert.That(frame, Is.Null);
    }

    private static int ReadInt(byte[]? frame)
    {
        var reader = new System.Formats.Cbor.CborReader(frame!);
        return reader.ReadInt32();
    }
}