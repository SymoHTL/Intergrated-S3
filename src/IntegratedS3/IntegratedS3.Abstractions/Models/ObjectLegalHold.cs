using System.Text.Json.Serialization;

namespace IntegratedS3.Abstractions.Models;

public sealed class ObjectLegalHoldInfo
{
    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public string? VersionId { get; init; }

    public ObjectLegalHoldStatus Status { get; init; } = ObjectLegalHoldStatus.Off;
}

[JsonConverter(typeof(JsonStringEnumConverter<ObjectLegalHoldStatus>))]
public enum ObjectLegalHoldStatus
{
    Off,
    On
}
