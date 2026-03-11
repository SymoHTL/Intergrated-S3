namespace IntegratedS3.Protocol;

public sealed class S3CopyObjectResult
{
    public required string ETag { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public string? ChecksumCrc32 { get; init; }

    public string? ChecksumCrc32c { get; init; }

    public string? ChecksumSha1 { get; init; }

    public string? ChecksumSha256 { get; init; }

    public string? ChecksumType { get; init; }
}
