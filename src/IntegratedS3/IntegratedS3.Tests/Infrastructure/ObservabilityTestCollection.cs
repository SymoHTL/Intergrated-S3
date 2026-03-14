using Xunit;

namespace IntegratedS3.Tests.Infrastructure;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ObservabilityTestCollection
{
    public const string Name = "Observability";
}