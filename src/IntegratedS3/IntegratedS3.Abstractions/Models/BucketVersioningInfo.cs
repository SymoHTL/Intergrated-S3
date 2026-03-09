namespace IntegratedS3.Abstractions.Models;

public sealed class BucketVersioningInfo
{
    public string BucketName { get; init; } = string.Empty;

    public BucketVersioningStatus Status { get; init; } = BucketVersioningStatus.Disabled;

    public bool VersioningEnabled => Status == BucketVersioningStatus.Enabled;
}

public enum BucketVersioningStatus
{
    Disabled,
    Enabled,
    Suspended
}