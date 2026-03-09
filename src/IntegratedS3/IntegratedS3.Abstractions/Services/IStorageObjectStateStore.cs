using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Models;

namespace IntegratedS3.Abstractions.Services;

public interface IStorageObjectStateStore
{
    StorageSupportStateOwnership Ownership { get; }

    ValueTask<ObjectInfo?> GetObjectInfoAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default);

    IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(string providerName, string bucketName, string? prefix = null, CancellationToken cancellationToken = default);

    ValueTask UpsertObjectInfoAsync(string providerName, ObjectInfo @object, CancellationToken cancellationToken = default);

    ValueTask RemoveObjectInfoAsync(string providerName, string bucketName, string key, string? versionId = null, CancellationToken cancellationToken = default);
}