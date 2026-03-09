using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class PutBucketVersioningRequest
{
    public required string BucketName { get; init; }

    public required BucketVersioningStatus Status { get; init; }
}