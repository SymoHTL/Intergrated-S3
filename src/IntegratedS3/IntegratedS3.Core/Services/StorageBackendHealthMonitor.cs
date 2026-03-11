using System.Collections.Concurrent;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Core.Options;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Core.Services;

internal sealed class StorageBackendHealthMonitor(
    IStorageBackendHealthEvaluator healthEvaluator,
    IStorageBackendHealthProbe healthProbe,
    IOptions<IntegratedS3CoreOptions> options,
    TimeProvider timeProvider)
{
    private readonly ConcurrentDictionary<string, StorageBackendHealthSnapshot> _snapshots = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _probeLocks = new(StringComparer.Ordinal);

    public async ValueTask<StorageBackendHealthStatus> GetStatusAsync(IStorageBackend backend, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(backend);
        cancellationToken.ThrowIfCancellationRequested();

        var evaluatorStatus = await healthEvaluator.GetStatusAsync(backend, cancellationToken);
        var healthOptions = options.Value.BackendHealth;
        if (!healthOptions.EnableDynamicSnapshots) {
            return evaluatorStatus;
        }

        var now = timeProvider.GetUtcNow();
        if (TryGetSnapshot(backend.Name, now, out var snapshot)) {
            return CombineStatuses(evaluatorStatus, snapshot.Status);
        }

        if (!healthOptions.EnableActiveProbing || evaluatorStatus == StorageBackendHealthStatus.Unhealthy) {
            return evaluatorStatus;
        }

        var probeStatus = await ProbeAsync(backend, healthOptions, cancellationToken);
        return CombineStatuses(evaluatorStatus, probeStatus);
    }

    public void ReportSuccess(IStorageBackend backend)
    {
        ArgumentNullException.ThrowIfNull(backend);
        UpdateSnapshot(backend.Name, StorageBackendHealthStatus.Healthy, options.Value.BackendHealth.HealthySnapshotTtl);
    }

    public void ReportFailure(IStorageBackend backend, StorageError? error)
    {
        ArgumentNullException.ThrowIfNull(backend);

        var status = error?.Code is StorageErrorCode.ProviderUnavailable or StorageErrorCode.Throttled
            ? StorageBackendHealthStatus.Unhealthy
            : StorageBackendHealthStatus.Healthy;
        var ttl = status == StorageBackendHealthStatus.Unhealthy
            ? options.Value.BackendHealth.UnhealthySnapshotTtl
            : options.Value.BackendHealth.HealthySnapshotTtl;

        UpdateSnapshot(backend.Name, status, ttl);
    }

    private async ValueTask<StorageBackendHealthStatus> ProbeAsync(IStorageBackend backend, StorageBackendHealthOptions healthOptions, CancellationToken cancellationToken)
    {
        var probeLock = _probeLocks.GetOrAdd(backend.Name, static _ => new SemaphoreSlim(1, 1));
        await probeLock.WaitAsync(cancellationToken);
        try {
            var now = timeProvider.GetUtcNow();
            if (TryGetSnapshot(backend.Name, now, out var snapshot)) {
                return snapshot.Status;
            }

            using var timeoutSource = CreateProbeTimeoutSource(healthOptions.ProbeTimeout, cancellationToken);
            var probeToken = timeoutSource?.Token ?? cancellationToken;
            StorageBackendHealthStatus probeStatus;
            try {
                probeStatus = await healthProbe.ProbeAsync(backend, probeToken);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) {
                probeStatus = StorageBackendHealthStatus.Unhealthy;
            }
            catch {
                probeStatus = StorageBackendHealthStatus.Unknown;
            }

            UpdateSnapshot(
                backend.Name,
                probeStatus,
                probeStatus == StorageBackendHealthStatus.Unhealthy
                    ? healthOptions.UnhealthySnapshotTtl
                    : healthOptions.HealthySnapshotTtl);

            return probeStatus;
        }
        finally {
            probeLock.Release();
        }
    }

    private void UpdateSnapshot(string backendName, StorageBackendHealthStatus status, TimeSpan ttl)
    {
        if (!options.Value.BackendHealth.EnableDynamicSnapshots || ttl <= TimeSpan.Zero) {
            _snapshots.TryRemove(backendName, out _);
            return;
        }

        var observedAtUtc = timeProvider.GetUtcNow();
        _snapshots[backendName] = new StorageBackendHealthSnapshot(status, observedAtUtc + ttl);
    }

    private bool TryGetSnapshot(string backendName, DateTimeOffset now, out StorageBackendHealthSnapshot snapshot)
    {
        if (_snapshots.TryGetValue(backendName, out snapshot)) {
            if (snapshot.ExpiresAtUtc > now) {
                return true;
            }

            _snapshots.TryRemove(new KeyValuePair<string, StorageBackendHealthSnapshot>(backendName, snapshot));
        }

        snapshot = default;
        return false;
    }

    private static CancellationTokenSource? CreateProbeTimeoutSource(TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (timeout <= TimeSpan.Zero) {
            return null;
        }

        var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        return timeoutSource;
    }

    private static StorageBackendHealthStatus CombineStatuses(StorageBackendHealthStatus evaluatorStatus, StorageBackendHealthStatus snapshotStatus)
    {
        if (evaluatorStatus == StorageBackendHealthStatus.Unhealthy || snapshotStatus == StorageBackendHealthStatus.Unhealthy) {
            return StorageBackendHealthStatus.Unhealthy;
        }

        if (evaluatorStatus == StorageBackendHealthStatus.Healthy || snapshotStatus == StorageBackendHealthStatus.Healthy) {
            return StorageBackendHealthStatus.Healthy;
        }

        return StorageBackendHealthStatus.Unknown;
    }

    private readonly record struct StorageBackendHealthSnapshot(StorageBackendHealthStatus Status, DateTimeOffset ExpiresAtUtc);
}
