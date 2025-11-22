namespace Grial.Core.WAL;

public sealed class SnapshotEffluentSchedulerOptions
{
    /// <summary>How often to try to take a snapshot.</summary>
    public TimeSpan SnapshotInterval { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>The delay after the application starts before the first snapshot.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// The minimum increase in seq since the last snapshot,
    /// so that it makes sense to make a new one.
    /// </summary>
    public long MinSeqDeltaForSnapshot { get; init; } = 10_000;

    /// <summary>
    /// The minimum number of entries in WAL (LastSeq), after which
    /// we generally start thinking about snapshots.
    /// </summary>
    public long MinSeqToStartSnapshots { get; init; } = 1;
}