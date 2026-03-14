using System.Text.Json.Serialization;

namespace IntegratedS3.Abstractions.Models;

public sealed class ObjectRetentionPolicy
{
    public required ObjectRetentionMode Mode { get; init; }

    public required DateTimeOffset RetainUntilUtc { get; init; }
}

public sealed class ObjectRetentionInfo
{
    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public string? VersionId { get; init; }

    public required ObjectRetentionPolicy Policy { get; init; }
}

[JsonConverter(typeof(JsonStringEnumConverter<ObjectRetentionMode>))]
public enum ObjectRetentionMode
{
    Governance,
    Compliance
}
