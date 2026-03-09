namespace IntegratedS3.Protocol;

public sealed class S3CompleteMultipartUploadResult
{
    public string? Location { get; init; }

    public required string Bucket { get; init; }

    public required string Key { get; init; }

    public required string ETag { get; init; }

    public string? ChecksumCrc32 { get; init; }

    public string? ChecksumSha256 { get; init; }

    public string? ChecksumType { get; init; }
}
