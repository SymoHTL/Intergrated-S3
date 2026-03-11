using IntegratedS3.Abstractions.Services;

namespace IntegratedS3.Core.Services;

public interface IStorageBackendHealthProbe
{
    ValueTask<StorageBackendHealthStatus> ProbeAsync(IStorageBackend backend, CancellationToken cancellationToken = default);
}
