namespace IntegratedS3.Protocol;

public sealed class S3ObjectRetentionConfiguration
{
    public required string Mode { get; init; }

    public required DateTimeOffset RetainUntilDateUtc { get; init; }
}
