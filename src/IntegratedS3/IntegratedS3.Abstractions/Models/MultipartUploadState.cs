namespace IntegratedS3.Abstractions.Models;

public class MultipartUploadState
{
    public required string BucketName { get; init; }

    public required string Key { get; init; }

    public required string UploadId { get; init; }

    public DateTimeOffset InitiatedAtUtc { get; init; }

    public string? ContentType { get; init; }

    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    public string? ChecksumAlgorithm { get; init; }
}
