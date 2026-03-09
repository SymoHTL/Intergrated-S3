namespace IntegratedS3.Abstractions.Models;

public sealed class DeleteObjectResult
{
    public string BucketName { get; init; } = string.Empty;

    public string Key { get; init; } = string.Empty;

    public string? VersionId { get; init; }

    public bool IsDeleteMarker { get; init; }

    public ObjectInfo? CurrentObject { get; init; }
}