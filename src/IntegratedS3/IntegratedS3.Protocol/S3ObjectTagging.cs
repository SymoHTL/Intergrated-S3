namespace IntegratedS3.Protocol;

public sealed class S3ObjectTagging
{
    public IReadOnlyList<S3ObjectTag> TagSet { get; init; } = [];
}

public sealed class S3ObjectTag
{
    public required string Key { get; init; }

    public required string Value { get; init; }
}
