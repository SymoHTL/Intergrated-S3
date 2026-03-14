namespace IntegratedS3.Provider.S3;

/// <summary>
/// Configures the AWS SDK-backed S3 storage provider.
/// </summary>
public sealed class S3StorageOptions
{
    /// <summary>The provider name reported in service metadata.</summary>
    public string ProviderName { get; set; } = "s3-primary";

    /// <summary>Whether this provider should be treated as the primary backend.</summary>
    public bool IsPrimary { get; set; } = true;

    /// <summary>The AWS region or compatible region string used by the SDK client.</summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>An optional service URL for S3-compatible endpoints such as MinIO or LocalStack.</summary>
    public string? ServiceUrl { get; set; }

    /// <summary>Whether the SDK client should force path-style bucket addressing.</summary>
    public bool ForcePathStyle { get; set; }

    /// <summary>
    /// AWS/S3-compatible access key ID. When both <see cref="AccessKey"/> and
    /// <see cref="SecretKey"/> are non-empty, explicit credentials are used instead
    /// of the ambient credential chain. Required for self-hosted endpoints such as
    /// MinIO or LocalStack.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// AWS/S3-compatible secret access key that corresponds to <see cref="AccessKey"/>.
    /// </summary>
    public string? SecretKey { get; set; }
}
