namespace IntegratedS3.Protocol;

public sealed class S3ListObjectVersionsResult
{
    public required string Name { get; init; }

    public string? Prefix { get; init; }

    public string? Delimiter { get; init; }

    public string? KeyMarker { get; init; }

    public string? VersionIdMarker { get; init; }

    public string? NextKeyMarker { get; init; }

    public string? NextVersionIdMarker { get; init; }

    public int MaxKeys { get; init; }

    public bool IsTruncated { get; init; }

    public IReadOnlyList<S3ObjectVersionEntry> Versions { get; init; } = [];

    public IReadOnlyList<S3ListBucketCommonPrefix> CommonPrefixes { get; init; } = [];
}

public sealed class S3ObjectVersionEntry
{
    public required string Key { get; init; }

    public required string VersionId { get; init; }

    public bool IsLatest { get; init; }

    public bool IsDeleteMarker { get; init; }

    public string? ETag { get; init; }

    public long Size { get; init; }

    public DateTimeOffset LastModifiedUtc { get; init; }

    public string StorageClass { get; init; } = "STANDARD";
}