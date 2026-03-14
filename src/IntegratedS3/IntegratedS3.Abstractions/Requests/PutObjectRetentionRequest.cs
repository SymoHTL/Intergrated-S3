using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

public sealed class PutObjectRetentionRequest
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public string? VersionId { get; init; }

    public required ObjectRetentionPolicy Policy { get; init; }
}
