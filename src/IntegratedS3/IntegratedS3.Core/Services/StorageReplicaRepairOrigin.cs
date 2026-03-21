namespace IntegratedS3.Core.Services;

/// <summary>
/// Identifies how a replica repair entry was originated.
/// </summary>
public enum StorageReplicaRepairOrigin
{
    /// <summary>The repair was created because an asynchronous replication pass detected divergence.</summary>
    AsyncReplication,

    /// <summary>The repair was created because a write succeeded on the primary but failed on the replica.</summary>
    PartialWriteFailure,

    /// <summary>The repair was created during a reconciliation pass comparing primary and replica state.</summary>
    Reconciliation
}
