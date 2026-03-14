using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Provider.Disk.Internal;

internal sealed class DiskObjectMetadata
{
    public string? VersionId { get; init; }

    public bool IsLatest { get; init; }

    public bool IsDeleteMarker { get; init; }

    public DateTimeOffset? LastModifiedUtc { get; init; }

    public string? ContentType { get; init; }

    public Dictionary<string, string>? Metadata { get; init; }

    public Dictionary<string, string>? Tags { get; init; }

    public Dictionary<string, string>? Checksums { get; init; }

    public ObjectRetentionPolicy? Retention { get; init; }

    public ObjectLegalHoldStatus? LegalHold { get; init; }
}
