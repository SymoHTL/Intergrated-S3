namespace IntegratedS3.Abstractions.Errors;

public enum StorageErrorCode
{
    Unknown,
    ObjectNotFound,
    BucketNotFound,
    AccessDenied,
    InvalidChecksum,
    InvalidRange,
    PreconditionFailed,
    VersionConflict,
    BucketAlreadyExists,
    MultipartConflict,
    Throttled,
    ProviderUnavailable,
    UnsupportedCapability,
    QuotaExceeded
}
