namespace Grial.Core.Network;

using System.Formats.Cbor;

public sealed class ReplicationConnection(Stream input, Stream output, bool leaveOpen = false) : IAsyncDisposable
{
    public Stream Input { get; } = input;
    public Stream Output { get; } = output;

    public ValueTask DisposeAsync()
    {
        if (!leaveOpen)
        {
            Input.Dispose();
            Output.Dispose();
        }
        return ValueTask.CompletedTask;
    }


    static CborReader CreateReader(ReadOnlyMemory<byte> frame)
        => new(frame);

    static ReadOnlyMemory<byte> EnsureFrame(byte[]? frame)
    {
        if (frame is null)
            throw new EndOfStreamException("Unexpected EOF while reading replication frame.");
        return frame;
    }

    public ValueTask SendHelloAsync(HelloMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeHello(ref writer, msg),
            cancellationToken);

    public async ValueTask<HelloMessage> ReceiveHelloAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.Ping)
            throw new InvalidDataException($"Expected Hello, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeHello(ref reader);
    }

    public ValueTask SendHelloAckAsync(HelloAckMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeHelloAck(ref writer, msg),
            cancellationToken);

    public async ValueTask<HelloAckMessage> ReceiveHelloAckAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.Pong)
            throw new InvalidDataException($"Expected HelloAck, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeHelloAck(ref reader);
    }

    public ValueTask SendFollowAsync(FollowMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeFollow(ref writer, msg),
            cancellationToken);

    public async ValueTask<FollowMessage> ReceiveFollowAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.Follow)
            throw new InvalidDataException($"Expected Follow, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeFollow(ref reader);
    }

    public ValueTask SendFollowAcceptAsync(FollowAcceptMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeFollowAccept(ref writer, msg),
            cancellationToken);

    public async ValueTask<FollowAcceptMessage> ReceiveFollowAcceptAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.FollowAccept)
            throw new InvalidDataException($"Expected FollowAccept, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeFollowAccept(ref reader);
    }
    public ValueTask SendSnapshotStartAsync(SnapshotStartMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeSnapshotStart(ref writer, msg),
            cancellationToken);

    public async ValueTask<SnapshotStartMessage> ReceiveSnapshotStartAsync(
        CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.SnapshotStart)
            throw new InvalidDataException($"Expected SnapshotStart, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeSnapshotStart(ref reader);
    }

    public ValueTask SendSnapshotChunkAsync(SnapshotChunkMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeSnapshotChunk(ref writer, msg),
            cancellationToken);

    public async ValueTask<SnapshotChunkMessage> ReceiveSnapshotChunkAsync(
        CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.SnapshotChunk)
            throw new InvalidDataException($"Expected SnapshotChunk, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeSnapshotChunk(ref reader);
    }

    public ValueTask SendSnapshotEndAsync(SnapshotEndMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeSnapshotEnd(ref writer, msg),
            cancellationToken);

    public async ValueTask<SnapshotEndMessage> ReceiveSnapshotEndAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.SnapshotEnd)
            throw new InvalidDataException($"Expected SnapshotEnd, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeSnapshotEnd(ref reader);
    }

    public ValueTask SendWalBatchAsync(WalBatchMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeWalBatch(ref writer, msg),
            cancellationToken);

    public async ValueTask<WalBatchMessage> ReceiveWalBatchAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.WalBatch)
            throw new InvalidDataException($"Expected WalBatch, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeWalBatch(ref reader);
    }

    public ValueTask SendHeartbeatAsync(HeartbeatMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeHeartbeat(ref writer, msg),
            cancellationToken);

    public async ValueTask<HeartbeatMessage> ReceiveHeartbeatAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.Heartbeat)
            throw new InvalidDataException($"Expected Heartbeat, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeHeartbeat(ref reader);
    }

    public ValueTask SendAckAsync(AckMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeAck(ref writer, msg),
            cancellationToken);

    public async ValueTask<AckMessage> ReceiveAckAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.Ack)
            throw new InvalidDataException($"Expected Ack, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeAck(ref reader);
    }

    public ValueTask SendErrorAsync(ErrorMessage msg, CancellationToken cancellationToken = default)
        => ReplicationFrameIO.WriteFrameAsync(
            Output,
            writer => ReplicationCodec.EncodeError(ref writer, msg),
            cancellationToken);

    public async ValueTask<ErrorMessage> ReceiveErrorAsync(CancellationToken cancellationToken = default)
    {
        var frame = EnsureFrame(await ReplicationFrameIO.ReadFrameAsync(Input, cancellationToken)
            .ConfigureAwait(false));

        var type = ReplicationCodec.PeekMessageType(frame.Span);
        if (type != ReplicationMessageType.Error)
            throw new InvalidDataException($"Expected Error, got {type}.");

        var reader = CreateReader(frame);
        return ReplicationCodec.DecodeError(ref reader);
    }
}