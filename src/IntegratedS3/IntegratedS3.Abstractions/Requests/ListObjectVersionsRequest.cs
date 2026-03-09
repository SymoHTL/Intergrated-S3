namespace IntegratedS3.Abstractions.Requests;

public sealed class ListObjectVersionsRequest
{
    public required string BucketName { get; init; }

    public string? Prefix { get; init; }

    public string? Delimiter { get; init; }

    public string? KeyMarker { get; init; }

    public string? VersionIdMarker { get; init; }

    public int? PageSize { get; init; }
}