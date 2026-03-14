using Microsoft.AspNetCore.Routing;

namespace IntegratedS3.AspNetCore;

/// <summary>
/// Configures which endpoint groups are mapped and how the root route groups should be customized.
/// </summary>
public sealed class IntegratedS3EndpointOptions
{
    /// <summary>Whether service-document routes should be mapped.</summary>
    public bool EnableServiceEndpoints { get; set; } = true;

    /// <summary>Whether bucket routes should be mapped.</summary>
    public bool EnableBucketEndpoints { get; set; } = true;

    /// <summary>Whether object routes should be mapped.</summary>
    public bool EnableObjectEndpoints { get; set; } = true;

    /// <summary>Whether multipart routes should be mapped.</summary>
    public bool EnableMultipartEndpoints { get; set; } = true;

    /// <summary>Whether admin and capability routes should be mapped.</summary>
    public bool EnableAdminEndpoints { get; set; } = true;

    /// <summary>Applies conventions to the main IntegratedS3 route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureRouteGroup { get; set; }

    /// <summary>Applies conventions to the shared root GET route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureRootRouteGroup { get; set; }

    /// <summary>Applies conventions to the shared S3-compatible compatibility route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureCompatibilityRouteGroup { get; set; }

    /// <summary>Applies conventions to the service route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureServiceRouteGroup { get; set; }

    /// <summary>Applies conventions to the bucket route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureBucketRouteGroup { get; set; }

    /// <summary>Applies conventions to the object route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureObjectRouteGroup { get; set; }

    /// <summary>Applies conventions to the multipart route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureMultipartRouteGroup { get; set; }

    /// <summary>Applies conventions to the admin route group.</summary>
    public Action<RouteGroupBuilder>? ConfigureAdminRouteGroup { get; set; }

    internal IntegratedS3EndpointOptions Clone()
    {
        return new IntegratedS3EndpointOptions
        {
            EnableServiceEndpoints = EnableServiceEndpoints,
            EnableBucketEndpoints = EnableBucketEndpoints,
            EnableObjectEndpoints = EnableObjectEndpoints,
            EnableMultipartEndpoints = EnableMultipartEndpoints,
            EnableAdminEndpoints = EnableAdminEndpoints,
            ConfigureRouteGroup = ConfigureRouteGroup,
            ConfigureRootRouteGroup = ConfigureRootRouteGroup,
            ConfigureCompatibilityRouteGroup = ConfigureCompatibilityRouteGroup,
            ConfigureServiceRouteGroup = ConfigureServiceRouteGroup,
            ConfigureBucketRouteGroup = ConfigureBucketRouteGroup,
            ConfigureObjectRouteGroup = ConfigureObjectRouteGroup,
            ConfigureMultipartRouteGroup = ConfigureMultipartRouteGroup,
            ConfigureAdminRouteGroup = ConfigureAdminRouteGroup
        };
    }
}
