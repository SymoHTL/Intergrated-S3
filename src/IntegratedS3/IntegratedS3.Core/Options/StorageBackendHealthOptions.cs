namespace IntegratedS3.Core.Options;

public sealed class StorageBackendHealthOptions
{
    public bool EnableDynamicSnapshots { get; set; } = true;

    public bool EnableActiveProbing { get; set; }

    public TimeSpan HealthySnapshotTtl { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan UnhealthySnapshotTtl { get; set; } = TimeSpan.FromSeconds(10);

    public TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromSeconds(3);
}
