namespace IntegratedS3.Core.Models;

/// <summary>
/// Describes an object presign request sent from a client or HTTP endpoint to the presign service.
/// </summary>
public sealed class StoragePresignRequest
{
    /// <summary>The object operation to presign.</summary>
    public required StoragePresignOperation Operation { get; init; }

    /// <summary>The target bucket name.</summary>
    public required string BucketName { get; init; }

    /// <summary>The target object key.</summary>
    public required string Key { get; init; }

    /// <summary>The requested presign lifetime in seconds.</summary>
    public required int ExpiresInSeconds { get; init; }

    /// <summary>An optional version identifier for read operations.</summary>
    public string? VersionId { get; init; }

    /// <summary>An optional content type to include in a signed write request.</summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// The caller's preferred access mode for the returned presigned grant.
    /// When <see langword="null" /> the server chooses the default mode (typically <see cref="StorageAccessMode.Proxy" />).
    /// Strategies may honour, downgrade, or ignore this preference depending on provider capabilities.
    /// </summary>
    public StorageAccessMode? PreferredAccessMode { get; init; }
}
