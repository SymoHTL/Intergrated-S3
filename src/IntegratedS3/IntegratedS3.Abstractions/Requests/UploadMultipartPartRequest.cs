using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Requests;

/// <summary>Request parameters for the UploadPart operation in a multipart upload.</summary>
public sealed class UploadMultipartPartRequest
{
    /// <summary>The name of the bucket for the multipart upload.</summary>
    public required string BucketName { get; init; }

    /// <summary>The object key for the multipart upload.</summary>
    public required string Key { get; init; }

    /// <summary>The upload identifier returned by the InitiateMultipartUpload operation.</summary>
    public required string UploadId { get; init; }

    /// <summary>The part number identifying this part within the upload (1–10000).</summary>
    public required int PartNumber { get; init; }

    /// <summary>The part data stream to upload. Null when the part is sourced from a copy operation.</summary>
    public Stream? Content { get; init; }

    /// <summary>The size of the part data in bytes, if known.</summary>
    public long? ContentLength { get; init; }

    /// <summary>The checksum algorithm to use for part integrity verification.</summary>
    public string? ChecksumAlgorithm { get; init; }

    /// <summary>Checksum values for the part keyed by algorithm name.</summary>
    public IReadOnlyDictionary<string, string>? Checksums { get; init; }

    /// <summary>Customer-provided encryption settings for the upload.</summary>
    public ObjectCustomerEncryptionSettings? CustomerEncryption { get; init; }

    /// <summary>The bucket name of the source object for a copy-part operation.</summary>
    public string? CopySourceBucketName { get; init; }

    /// <summary>The key of the source object for a copy-part operation.</summary>
    public string? CopySourceKey { get; init; }

    /// <summary>The version identifier of the source object for a copy-part operation.</summary>
    public string? CopySourceVersionId { get; init; }

    /// <summary>Copy the source only if its ETag matches this value.</summary>
    public string? CopySourceIfMatchETag { get; init; }

    /// <summary>Copy the source only if its ETag does not match this value.</summary>
    public string? CopySourceIfNoneMatchETag { get; init; }

    /// <summary>Copy the source only if it has been modified since this timestamp (UTC).</summary>
    public DateTimeOffset? CopySourceIfModifiedSinceUtc { get; init; }

    /// <summary>Copy the source only if it has not been modified since this timestamp (UTC).</summary>
    public DateTimeOffset? CopySourceIfUnmodifiedSinceUtc { get; init; }

    /// <summary>The byte range to copy from the source object.</summary>
    public ObjectRange? CopySourceRange { get; init; }
}
