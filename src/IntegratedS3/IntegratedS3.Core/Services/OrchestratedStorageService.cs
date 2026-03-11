using System.Runtime.CompilerServices;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Options;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Core.Services;

internal sealed class OrchestratedStorageService(
    IEnumerable<IStorageBackend> backends,
    IStorageCatalogStore catalogStore,
    IOptions<IntegratedS3CoreOptions> options,
    StorageBackendHealthMonitor backendHealthMonitor) : IStorageService
{
    private readonly IStorageBackend[] _backends = backends.ToArray();
    private readonly Lazy<IStorageBackend> _primaryBackend = new(() => ResolvePrimaryBackend(backends.ToArray()));

    public async IAsyncEnumerable<BucketInfo> ListBucketsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var backend = await SelectReadBackendAsync(cancellationToken);
        await foreach (var bucket in backend.ListBucketsAsync(cancellationToken).WithCancellation(cancellationToken)) {
            await catalogStore.UpsertBucketAsync(backend.Name, bucket, cancellationToken);
            yield return bucket;
        }
    }

    public async ValueTask<StorageResult<BucketInfo>> CreateBucketAsync(CreateBucketRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var result = await backend.CreateBucketAsync(request, cancellationToken);
        if (result.IsSuccess && result.Value is not null) {
            await catalogStore.UpsertBucketAsync(backend.Name, result.Value, cancellationToken);

            var replicationError = await ReplicateBucketCreateAsync(request, backend, cancellationToken);
            if (replicationError is not null) {
                return StorageResult<BucketInfo>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> GetBucketVersioningAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            (backend, ct) => backend.GetBucketVersioningAsync(bucketName, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<BucketVersioningInfo>> PutBucketVersioningAsync(PutBucketVersioningRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var result = await backend.PutBucketVersioningAsync(request, cancellationToken);
        if (result.IsSuccess) {
            var refreshedBucket = await backend.HeadBucketAsync(request.BucketName, cancellationToken);
            if (refreshedBucket.IsSuccess && refreshedBucket.Value is not null) {
                await catalogStore.UpsertBucketAsync(backend.Name, refreshedBucket.Value, cancellationToken);
            }

            var replicationError = await ReplicateBucketVersioningAsync(request, backend, cancellationToken);
            if (replicationError is not null) {
                return StorageResult<BucketVersioningInfo>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<BucketInfo>> HeadBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            (backend, ct) => backend.HeadBucketAsync(bucketName, ct),
            async (backend, result, ct) => {
                if (result.Value is not null) {
                    await catalogStore.UpsertBucketAsync(backend.Name, result.Value, ct);
                }
            },
            cancellationToken);
    }

    public async ValueTask<StorageResult> DeleteBucketAsync(DeleteBucketRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var result = await backend.DeleteBucketAsync(request, cancellationToken);
        if (result.IsSuccess) {
            await catalogStore.RemoveBucketAsync(backend.Name, request.BucketName, cancellationToken);

            var replicationError = await ReplicateBucketDeleteAsync(request, backend, cancellationToken);
            if (replicationError is not null) {
                return StorageResult.Failure(replicationError);
            }
        }

        return result;
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectsAsync(ListObjectsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var backend = await SelectReadBackendAsync(cancellationToken);
        await foreach (var @object in backend.ListObjectsAsync(request, cancellationToken).WithCancellation(cancellationToken)) {
            await catalogStore.UpsertObjectAsync(backend.Name, @object, cancellationToken);
            yield return @object;
        }
    }

    public async IAsyncEnumerable<ObjectInfo> ListObjectVersionsAsync(ListObjectVersionsRequest request, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var backend = await SelectReadBackendAsync(cancellationToken);
        await foreach (var @object in backend.ListObjectVersionsAsync(request, cancellationToken).WithCancellation(cancellationToken)) {
            await catalogStore.UpsertObjectAsync(backend.Name, @object, cancellationToken);
            yield return @object;
        }
    }

    public async ValueTask<StorageResult<GetObjectResponse>> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            (backend, ct) => backend.GetObjectAsync(request, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectTagSet>> GetObjectTagsAsync(GetObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            (backend, ct) => backend.GetObjectTagsAsync(request, ct),
            onSuccess: null,
            cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectInfo>> CopyObjectAsync(CopyObjectRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var result = await backend.CopyObjectAsync(request, cancellationToken);
        if (result.IsSuccess && result.Value is not null) {
            await catalogStore.UpsertObjectAsync(backend.Name, result.Value, cancellationToken);

            var wasCopiedToDestination = string.Equals(result.Value.BucketName, request.DestinationBucketName, StringComparison.Ordinal)
                && string.Equals(result.Value.Key, request.DestinationKey, StringComparison.Ordinal);

            if (wasCopiedToDestination) {
                var replicationError = await ReplicateCopyObjectAsync(request, backend, cancellationToken);
                if (replicationError is not null) {
                    return StorageResult<ObjectInfo>.Failure(replicationError);
                }
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<ObjectInfo>> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        StorageResult<ObjectInfo> result;

        if (ShouldReplicateWrites(backend)) {
            var tempFilePath = await BufferStreamToTempFileAsync(request.Content, cancellationToken);
            try {
                result = await PutBufferedObjectAsync(backend, request, tempFilePath, cancellationToken);
                if (!result.IsSuccess || result.Value is null) {
                    return result;
                }

                await catalogStore.UpsertObjectAsync(backend.Name, result.Value, cancellationToken);

                var replicationError = await ReplicateBufferedObjectWriteAsync(request, tempFilePath, backend, cancellationToken);
                if (replicationError is not null) {
                    return StorageResult<ObjectInfo>.Failure(replicationError);
                }

                return result;
            }
            finally {
                DeleteTempFileIfPresent(tempFilePath);
            }
        }

        result = await backend.PutObjectAsync(request, cancellationToken);
        if (result.IsSuccess && result.Value is not null) {
            await catalogStore.UpsertObjectAsync(backend.Name, result.Value, cancellationToken);
        }

        return result;
    }

    public async ValueTask<StorageResult<ObjectTagSet>> PutObjectTagsAsync(PutObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var result = await backend.PutObjectTagsAsync(request, cancellationToken);
        if (result.IsSuccess && result.Value is not null) {
            await RefreshCatalogObjectAsync(backend, request.BucketName, request.Key, request.VersionId, cancellationToken);

            var replicationError = await ReplicateObjectTagsAsync(request, backend, cancellationToken);
            if (replicationError is not null) {
                return StorageResult<ObjectTagSet>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<ObjectTagSet>> DeleteObjectTagsAsync(DeleteObjectTagsRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var result = await backend.DeleteObjectTagsAsync(request, cancellationToken);
        if (result.IsSuccess && result.Value is not null) {
            await RefreshCatalogObjectAsync(backend, request.BucketName, request.Key, request.VersionId, cancellationToken);

            var replicationError = await ReplicateObjectTagDeletionAsync(request, backend, cancellationToken);
            if (replicationError is not null) {
                return StorageResult<ObjectTagSet>.Failure(replicationError);
            }
        }

        return result;
    }

    public async ValueTask<StorageResult<MultipartUploadInfo>> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult<MultipartUploadInfo>.Failure(replicationError);
        }

        return await backend.InitiateMultipartUploadAsync(request, cancellationToken);
    }

    public async ValueTask<StorageResult<MultipartUploadPart>> UploadMultipartPartAsync(UploadMultipartPartRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult<MultipartUploadPart>.Failure(replicationError);
        }

        return await backend.UploadMultipartPartAsync(request, cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectInfo>> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult<ObjectInfo>.Failure(replicationError);
        }

        var result = await backend.CompleteMultipartUploadAsync(request, cancellationToken);
        if (result.IsSuccess && result.Value is not null) {
            await catalogStore.UpsertObjectAsync(backend.Name, result.Value, cancellationToken);
        }

        return result;
    }

    public async ValueTask<StorageResult> AbortMultipartUploadAsync(AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var replicationError = GetMultipartReplicationError(backend, request.BucketName, request.Key);
        if (replicationError is not null) {
            return StorageResult.Failure(replicationError);
        }

        return await backend.AbortMultipartUploadAsync(request, cancellationToken);
    }

    public async ValueTask<StorageResult<ObjectInfo>> HeadObjectAsync(HeadObjectRequest request, CancellationToken cancellationToken = default)
    {
        return await ExecuteReadAsync(
            (backend, ct) => backend.HeadObjectAsync(request, ct),
            async (backend, result, ct) => {
                if (result.Value is not null) {
                    await catalogStore.UpsertObjectAsync(backend.Name, result.Value, ct);
                }
            },
            cancellationToken);
    }

    public async ValueTask<StorageResult<DeleteObjectResult>> DeleteObjectAsync(DeleteObjectRequest request, CancellationToken cancellationToken = default)
    {
        var backend = _primaryBackend.Value;
        var result = await backend.DeleteObjectAsync(request, cancellationToken);
        if (result.IsSuccess && result.Value is not null) {
            if (result.Value.CurrentObject is not null) {
                await catalogStore.UpsertObjectAsync(backend.Name, result.Value.CurrentObject, cancellationToken);
            }
            else if (!string.IsNullOrWhiteSpace(request.VersionId)) {
                await catalogStore.RemoveObjectAsync(backend.Name, request.BucketName, request.Key, request.VersionId, cancellationToken);
            }
            else {
                await catalogStore.RemoveObjectAsync(backend.Name, request.BucketName, request.Key, versionId: null, cancellationToken);
            }

            var replicationError = await ReplicateObjectDeleteAsync(request, backend, cancellationToken);
            if (replicationError is not null) {
                return StorageResult<DeleteObjectResult>.Failure(replicationError);
            }
        }

        return result;
    }

    private static IStorageBackend ResolvePrimaryBackend(IEnumerable<IStorageBackend> backends)
    {
        var resolvedBackends = backends.ToArray();
        return resolvedBackends.FirstOrDefault(static backend => backend.IsPrimary)
            ?? resolvedBackends.FirstOrDefault()
            ?? throw new InvalidOperationException("No storage backends have been registered.");
    }

    private async ValueTask<IStorageBackend> SelectReadBackendAsync(CancellationToken cancellationToken)
    {
        return (await GetOrderedReadBackendsAsync(cancellationToken))[0];
    }

    private async ValueTask<IReadOnlyList<IStorageBackend>> GetOrderedReadBackendsAsync(CancellationToken cancellationToken)
    {
        var primaryBackend = _primaryBackend.Value;
        if (_backends.Length <= 1 || options.Value.ReadRoutingMode == StorageReadRoutingMode.PrimaryOnly) {
            return [primaryBackend];
        }

        var candidates = new List<ReadBackendCandidate>(_backends.Length);
        foreach (var backend in _backends) {
            var healthStatus = await backendHealthMonitor.GetStatusAsync(backend, cancellationToken);
            candidates.Add(new ReadBackendCandidate(backend, healthStatus));
        }

        return candidates
            .OrderBy(candidate => GetReadPriority(candidate, primaryBackend, options.Value.ReadRoutingMode))
            .ThenBy(candidate => GetOriginalIndex(candidate.Backend))
            .Select(candidate => candidate.Backend)
            .ToArray();
    }

    private async ValueTask<StorageResult<T>> ExecuteReadAsync<T>(
        Func<IStorageBackend, CancellationToken, ValueTask<StorageResult<T>>> operation,
        Func<IStorageBackend, StorageResult<T>, CancellationToken, ValueTask>? onSuccess,
        CancellationToken cancellationToken)
    {
        StorageResult<T>? lastFailure = null;

        foreach (var backend in await GetOrderedReadBackendsAsync(cancellationToken)) {
            var result = await operation(backend, cancellationToken);
            if (result.IsSuccess) {
                backendHealthMonitor.ReportSuccess(backend);
                if (onSuccess is not null) {
                    await onSuccess(backend, result, cancellationToken);
                }

                return result;
            }

            backendHealthMonitor.ReportFailure(backend, result.Error);
            lastFailure = result;
            if (!ShouldFailoverRead(result.Error)) {
                return result;
            }
        }

        return lastFailure ?? StorageResult<T>.Failure(new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = "No storage backend could satisfy the read request.",
            ProviderName = _primaryBackend.Value.Name,
            SuggestedHttpStatusCode = 503
        });
    }

    private bool ShouldReplicateWrites(IStorageBackend primaryBackend)
    {
        return options.Value.ConsistencyMode == StorageConsistencyMode.WriteThroughAll
            && GetReplicaBackends(primaryBackend).Count > 0;
    }

    private StorageError? GetMultipartReplicationError(IStorageBackend primaryBackend, string bucketName, string key)
    {
        return ShouldReplicateWrites(primaryBackend)
            ? StorageError.Unsupported(
                "Multipart uploads are not yet supported when write-through replication is enabled.",
                bucketName,
                key)
            : null;
    }

    private IReadOnlyList<IStorageBackend> GetReplicaBackends(IStorageBackend primaryBackend)
    {
        if (options.Value.ConsistencyMode != StorageConsistencyMode.WriteThroughAll) {
            return [];
        }

        return _backends.Where(backend => !ReferenceEquals(backend, primaryBackend)).ToArray();
    }

    private async ValueTask<StorageError?> ReplicateBucketCreateAsync(CreateBucketRequest request, IStorageBackend primaryBackend, CancellationToken cancellationToken)
    {
        foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
            var replicaResult = await replicaBackend.CreateBucketAsync(request, cancellationToken);
            if (!replicaResult.IsSuccess || replicaResult.Value is null) {
                return CreateReplicationError(replicaBackend, replicaResult.Error, request.BucketName);
            }

            await catalogStore.UpsertBucketAsync(replicaBackend.Name, replicaResult.Value, cancellationToken);
        }

        return null;
    }

    private async ValueTask<StorageError?> ReplicateBucketDeleteAsync(DeleteBucketRequest request, IStorageBackend primaryBackend, CancellationToken cancellationToken)
    {
        foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
            var replicaResult = await replicaBackend.DeleteBucketAsync(request, cancellationToken);
            if (!replicaResult.IsSuccess) {
                return CreateReplicationError(replicaBackend, replicaResult.Error, request.BucketName);
            }

            await catalogStore.RemoveBucketAsync(replicaBackend.Name, request.BucketName, cancellationToken);
        }

        return null;
    }

    private async ValueTask<StorageError?> ReplicateBucketVersioningAsync(PutBucketVersioningRequest request, IStorageBackend primaryBackend, CancellationToken cancellationToken)
    {
        foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
            var replicaResult = await replicaBackend.PutBucketVersioningAsync(request, cancellationToken);
            if (!replicaResult.IsSuccess || replicaResult.Value is null) {
                return CreateReplicationError(replicaBackend, replicaResult.Error, request.BucketName);
            }

            var refreshedBucket = await replicaBackend.HeadBucketAsync(request.BucketName, cancellationToken);
            if (refreshedBucket.IsSuccess && refreshedBucket.Value is not null) {
                await catalogStore.UpsertBucketAsync(replicaBackend.Name, refreshedBucket.Value, cancellationToken);
            }
        }

        return null;
    }

    private async ValueTask<StorageError?> ReplicateBufferedObjectWriteAsync(PutObjectRequest request, string tempFilePath, IStorageBackend primaryBackend, CancellationToken cancellationToken)
    {
        foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
            var replicaResult = await PutBufferedObjectAsync(replicaBackend, request, tempFilePath, cancellationToken);
            if (!replicaResult.IsSuccess || replicaResult.Value is null) {
                return CreateReplicationError(replicaBackend, replicaResult.Error, request.BucketName, request.Key);
            }

            await catalogStore.UpsertObjectAsync(replicaBackend.Name, replicaResult.Value, cancellationToken);
        }

        return null;
    }

    private async ValueTask<StorageError?> ReplicateCopyObjectAsync(CopyObjectRequest request, IStorageBackend primaryBackend, CancellationToken cancellationToken)
    {
        var sourceResponseResult = await primaryBackend.GetObjectAsync(new GetObjectRequest
        {
            BucketName = request.SourceBucketName,
            Key = request.SourceKey,
            VersionId = request.SourceVersionId
        }, cancellationToken);

        if (!sourceResponseResult.IsSuccess || sourceResponseResult.Value is null) {
            return CreateReplicationError(primaryBackend, sourceResponseResult.Error, request.DestinationBucketName, request.DestinationKey);
        }

        await using var sourceResponse = sourceResponseResult.Value;
        var tempFilePath = await BufferStreamToTempFileAsync(sourceResponse.Content, cancellationToken);
        try {
            foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
                await using var content = OpenBufferedReadStream(tempFilePath);
                var replicaResult = await replicaBackend.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = request.DestinationBucketName,
                    Key = request.DestinationKey,
                    Content = content,
                    ContentLength = sourceResponse.Object.ContentLength,
                    ContentType = sourceResponse.Object.ContentType,
                    Metadata = CloneMetadata(sourceResponse.Object.Metadata),
                    Checksums = CloneChecksums(sourceResponse.Object.Checksums),
                    OverwriteIfExists = request.OverwriteIfExists
                }, cancellationToken);

                if (!replicaResult.IsSuccess || replicaResult.Value is null) {
                    return CreateReplicationError(replicaBackend, replicaResult.Error, request.DestinationBucketName, request.DestinationKey);
                }

                await catalogStore.UpsertObjectAsync(replicaBackend.Name, replicaResult.Value, cancellationToken);
            }

            return null;
        }
        finally {
            DeleteTempFileIfPresent(tempFilePath);
        }
    }

    private async ValueTask<StorageError?> ReplicateObjectDeleteAsync(DeleteObjectRequest request, IStorageBackend primaryBackend, CancellationToken cancellationToken)
    {
        foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
            var replicaResult = await replicaBackend.DeleteObjectAsync(request, cancellationToken);
            if (!replicaResult.IsSuccess) {
                return CreateReplicationError(replicaBackend, replicaResult.Error, request.BucketName, request.Key);
            }

            await catalogStore.RemoveObjectAsync(replicaBackend.Name, request.BucketName, request.Key, request.VersionId, cancellationToken);
        }

        return null;
    }

    private async ValueTask<StorageError?> ReplicateObjectTagsAsync(PutObjectTagsRequest request, IStorageBackend primaryBackend, CancellationToken cancellationToken)
    {
        foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
            var replicaResult = await replicaBackend.PutObjectTagsAsync(request, cancellationToken);
            if (!replicaResult.IsSuccess || replicaResult.Value is null) {
                return CreateReplicationError(replicaBackend, replicaResult.Error, request.BucketName, request.Key);
            }

            await RefreshCatalogObjectAsync(replicaBackend, request.BucketName, request.Key, request.VersionId, cancellationToken);
        }

        return null;
    }

    private async ValueTask<StorageError?> ReplicateObjectTagDeletionAsync(DeleteObjectTagsRequest request, IStorageBackend primaryBackend, CancellationToken cancellationToken)
    {
        foreach (var replicaBackend in GetReplicaBackends(primaryBackend)) {
            var replicaResult = await replicaBackend.DeleteObjectTagsAsync(request, cancellationToken);
            if (!replicaResult.IsSuccess || replicaResult.Value is null) {
                return CreateReplicationError(replicaBackend, replicaResult.Error, request.BucketName, request.Key);
            }

            await RefreshCatalogObjectAsync(replicaBackend, request.BucketName, request.Key, request.VersionId, cancellationToken);
        }

        return null;
    }

    private static async ValueTask<StorageResult<ObjectInfo>> PutBufferedObjectAsync(IStorageBackend backend, PutObjectRequest request, string tempFilePath, CancellationToken cancellationToken)
    {
        await using var content = OpenBufferedReadStream(tempFilePath);
        return await backend.PutObjectAsync(new PutObjectRequest
        {
            BucketName = request.BucketName,
            Key = request.Key,
            Content = content,
            ContentLength = request.ContentLength,
            ContentType = request.ContentType,
            Metadata = CloneMetadata(request.Metadata),
            Checksums = CloneChecksums(request.Checksums),
            OverwriteIfExists = request.OverwriteIfExists
        }, cancellationToken);
    }

    private static async Task<string> BufferStreamToTempFileAsync(Stream content, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(content);

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"integrateds3-orchestration-{Guid.NewGuid():N}.tmp");
        await using var tempFileStream = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
        await content.CopyToAsync(tempFileStream, cancellationToken);
        await tempFileStream.FlushAsync(cancellationToken);
        return tempFilePath;
    }

    private static Stream OpenBufferedReadStream(string tempFilePath)
    {
        return new FileStream(tempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    private static IReadOnlyDictionary<string, string>? CloneMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        return metadata is null
            ? null
            : new Dictionary<string, string>(metadata, StringComparer.OrdinalIgnoreCase);
    }

    private static IReadOnlyDictionary<string, string>? CloneChecksums(IReadOnlyDictionary<string, string>? checksums)
    {
        return checksums is null
            ? null
            : new Dictionary<string, string>(checksums, StringComparer.OrdinalIgnoreCase);
    }

    private async ValueTask RefreshCatalogObjectAsync(IStorageBackend backend, string bucketName, string key, CancellationToken cancellationToken)
    {
        await RefreshCatalogObjectAsync(backend, bucketName, key, versionId: null, cancellationToken);
    }

    private async ValueTask RefreshCatalogObjectAsync(IStorageBackend backend, string bucketName, string key, string? versionId, CancellationToken cancellationToken)
    {
        var headResult = await backend.HeadObjectAsync(new HeadObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            VersionId = versionId
        }, cancellationToken);

        if (headResult.IsSuccess && headResult.Value is not null) {
            await catalogStore.UpsertObjectAsync(backend.Name, headResult.Value, cancellationToken);
        }
    }

    private static void DeleteTempFileIfPresent(string tempFilePath)
    {
        if (File.Exists(tempFilePath)) {
            File.Delete(tempFilePath);
        }
    }

    private static StorageError CreateReplicationError(IStorageBackend backend, StorageError? underlyingError, string bucketName, string? objectKey = null)
    {
        return new StorageError
        {
            Code = StorageErrorCode.ProviderUnavailable,
            Message = underlyingError?.Message is { Length: > 0 } message
                ? $"Replicated write to provider '{backend.Name}' failed after the primary backend succeeded: {message}"
                : $"Replicated write to provider '{backend.Name}' failed after the primary backend succeeded.",
            BucketName = bucketName,
            ObjectKey = objectKey,
            ProviderName = backend.Name,
            SuggestedHttpStatusCode = 503
        };
    }

    private bool ShouldFailoverRead(StorageError? error)
    {
        return error?.Code is StorageErrorCode.ProviderUnavailable or StorageErrorCode.Throttled;
    }

    private int GetOriginalIndex(IStorageBackend backend)
    {
        for (var index = 0; index < _backends.Length; index++) {
            if (ReferenceEquals(_backends[index], backend)) {
                return index;
            }
        }

        return int.MaxValue;
    }

    private static int GetReadPriority(ReadBackendCandidate candidate, IStorageBackend primaryBackend, StorageReadRoutingMode readRoutingMode)
    {
        var isPrimary = ReferenceEquals(candidate.Backend, primaryBackend);
        return readRoutingMode switch
        {
            StorageReadRoutingMode.PreferPrimary => candidate.HealthStatus switch
            {
                StorageBackendHealthStatus.Healthy when isPrimary => 0,
                StorageBackendHealthStatus.Healthy => 1,
                StorageBackendHealthStatus.Unknown when isPrimary => 2,
                StorageBackendHealthStatus.Unknown => 3,
                StorageBackendHealthStatus.Unhealthy when isPrimary => 4,
                _ => 5
            },
            StorageReadRoutingMode.PreferHealthyReplica => candidate.HealthStatus switch
            {
                StorageBackendHealthStatus.Healthy when !isPrimary => 0,
                StorageBackendHealthStatus.Healthy => 1,
                StorageBackendHealthStatus.Unknown when !isPrimary => 2,
                StorageBackendHealthStatus.Unknown => 3,
                StorageBackendHealthStatus.Unhealthy when !isPrimary => 4,
                _ => 5
            },
            _ => isPrimary ? 0 : 1
        };
    }

    private readonly record struct ReadBackendCandidate(IStorageBackend Backend, StorageBackendHealthStatus HealthStatus);

}
