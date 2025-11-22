namespace Grial.Core.Network;

using WAL;

public enum ReplicationMessageType : int
{
    Ping = 0,
    Pong = 1,
    Follow = 2,
    FollowAccept = 3,
    SnapshotStart = 4,
    SnapshotChunk = 5,
    SnapshotEnd = 6,
    WalBatch = 7,
    Heartbeat = 8,
    Ack = 9,
    Error = 10,
}

public enum ReplicationErrorCode
{
    Unknown = 0,
    ProtocolMismatch = 1,
    ClusterIdMismatch = 2,
    UnsupportedVersion = 3,
    InvalidFollow = 4,
    SnapshotFailed = 5,
    WalGapTooLarge = 6,
    InternalError = 7,
}


public readonly record struct HelloMessage(
    string ClusterId,
    string NodeId,
    string RegionId,
    int ProtocolVersion,
    IReadOnlyDictionary<string, string>? Options);

public readonly record struct HelloAckMessage(
    int ProtocolVersion,
    string NodeId,
    long LastRevision,
    Dictionary<string, string>? Info);

public readonly record struct FollowMessage(
    long FromRevision,
    string StreamId,
    IReadOnlyDictionary<string, string>? Options);

public readonly record struct FollowAcceptMessage(
    string StreamId,
    long EffectiveFromRevision,
    bool RequireSnapshot);

public readonly record struct SnapshotStartMessage(
    string StreamId,
    long SnapshotId,
    long LastRevision,
    long TotalSizeBytes,
    int ChunkSizeBytes);

public readonly record struct SnapshotChunkMessage(
    string StreamId,
    long SnapshotId,
    int ChunkIndex,
    ReadOnlyMemory<byte> Data);

public readonly record struct SnapshotEndMessage(
    string StreamId,
    long SnapshotId);

public readonly record struct WalBatchMessage(
    string StreamId,
    long BaseRevision,
    long LastRevision,
    ChangeRecord[] Records);

public readonly record struct HeartbeatMessage(
    string StreamId,
    long LastRevision);

public readonly record struct AckMessage(
    string StreamId,
    long AppliedRevision,
    int MaxInFlight);

public readonly record struct ErrorMessage(
    string StreamId,
    ReplicationErrorCode Code,
    string Message);