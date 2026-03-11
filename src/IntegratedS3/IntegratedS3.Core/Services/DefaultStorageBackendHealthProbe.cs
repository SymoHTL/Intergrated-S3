using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

internal sealed class DefaultStorageBackendHealthProbe : IStorageBackendHealthProbe
{
    public async ValueTask<StorageBackendHealthStatus> ProbeAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(backend);
        cancellationToken.ThrowIfCancellationRequested();

        try {
            await using var enumerator = backend.ListBucketsAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
            _ = await enumerator.MoveNextAsync();
            return StorageBackendHealthStatus.Healthy;
        }
        catch (OperationCanceledException) {
            throw;
        }
        catch {
            return StorageBackendHealthStatus.Unhealthy;
        }
    }
}
