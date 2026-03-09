using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskBucketMetadata
{
    public BucketVersioningStatus VersioningStatus { get; init; } = BucketVersioningStatus.Disabled;
}