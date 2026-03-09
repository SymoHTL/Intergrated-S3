namespace IntegratedS3.Core.Persistence;

public sealed class MultipartUploadCatalogRecord
{
    public int Id { get; set; }

    public string ProviderName { get; set; } = string.Empty;

    public string BucketName { get; set; } = string.Empty;

    public string Key { get; set; } = string.Empty;

    public string UploadId { get; set; } = string.Empty;

    public DateTimeOffset InitiatedAtUtc { get; set; }

    public string? ContentType { get; set; }

    public string? MetadataJson { get; set; }

    public string? ChecksumAlgorithm { get; set; }

    public DateTimeOffset LastSyncedAtUtc { get; set; }
}
