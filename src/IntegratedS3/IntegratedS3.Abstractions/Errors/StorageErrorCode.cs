namespace IntegratedS3.Abstractions.Errors;

public enum StorageErrorCode
{
    Unknown,
    ObjectNotFound,
    BucketNotFound,
    CorsConfigurationNotFound,
    ObjectLockConfigurationNotFound,
    AccessDenied,
    InvalidRequest,
    InvalidChecksum,
    InvalidRange,
    PreconditionFailed,
    MethodNotAllowed,
    VersionConflict,
    BucketAlreadyExists,
    MultipartConflict,
    Throttled,
    ProviderUnavailable,
    UnsupportedCapability,
    QuotaExceeded
}
