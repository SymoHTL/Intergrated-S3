using System.Net;
using System.Globalization;
using System.Text;
using IntegratedS3.Abstractions.Capabilities;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Models;
using IntegratedS3.Abstractions.Requests;
using IntegratedS3.Abstractions.Responses;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.AspNetCore.Services;
using IntegratedS3.Protocol;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Net.Http.Headers;
using IntegratedS3.Core.Services;

namespace IntegratedS3.AspNetCore.Endpoints;

public static class IntegratedS3EndpointRouteBuilderExtensions
{
    private const string SigV4AuthenticationClaimType = "integrateds3:auth-type";
    private const string SigV4AuthenticationClaimValue = "sigv4";
    private const string MetadataHeaderPrefix = "x-integrateds3-meta-";
    private const string ContinuationTokenHeaderName = "x-integrateds3-continuation-token";
    private const string CopySourceHeaderName = "x-amz-copy-source";
    private const string CopySourceIfMatchHeaderName = "x-amz-copy-source-if-match";
    private const string CopySourceIfNoneMatchHeaderName = "x-amz-copy-source-if-none-match";
    private const string CopySourceIfModifiedSinceHeaderName = "x-amz-copy-source-if-modified-since";
    private const string CopySourceIfUnmodifiedSinceHeaderName = "x-amz-copy-source-if-unmodified-since";
    private const string SdkChecksumAlgorithmHeaderName = "x-amz-sdk-checksum-algorithm";
    private const string ChecksumAlgorithmHeaderName = "x-amz-checksum-algorithm";
    private const string ChecksumCrc32HeaderName = "x-amz-checksum-crc32";
    private const string ChecksumSha256HeaderName = "x-amz-checksum-sha256";
    private const string ChecksumTypeHeaderName = "x-amz-checksum-type";
    private const string VersionIdHeaderName = "x-amz-version-id";
    private const string XmlContentType = "application/xml";
    private const string ListTypeQueryParameterName = "list-type";
    private const string PrefixQueryParameterName = "prefix";
    private const string DelimiterQueryParameterName = "delimiter";
    private const string StartAfterQueryParameterName = "start-after";
    private const string MaxKeysQueryParameterName = "max-keys";
    private const string ContinuationTokenQueryParameterName = "continuation-token";
    private const string TaggingQueryParameterName = "tagging";
    private const string VersioningQueryParameterName = "versioning";
    private const string VersionsQueryParameterName = "versions";
    private const string KeyMarkerQueryParameterName = "key-marker";
    private const string VersionIdMarkerQueryParameterName = "version-id-marker";
    private const string UploadsQueryParameterName = "uploads";
    private const string UploadIdQueryParameterName = "uploadId";
    private const string PartNumberQueryParameterName = "partNumber";
    private const string VersionIdQueryParameterName = "versionId";
    private static readonly HashSet<string> SupportedBucketGetQueryParameters = [ListTypeQueryParameterName, PrefixQueryParameterName, DelimiterQueryParameterName, StartAfterQueryParameterName, MaxKeysQueryParameterName, ContinuationTokenQueryParameterName, VersioningQueryParameterName, VersionsQueryParameterName, KeyMarkerQueryParameterName, VersionIdMarkerQueryParameterName];
    private static readonly HashSet<string> SupportedBucketPostQueryParameters = ["delete"];

    public static RouteGroupBuilder MapIntegratedS3Endpoints(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var options = endpoints.ServiceProvider.GetRequiredService<IOptions<IntegratedS3Options>>().Value;
        var group = endpoints.MapGroup(options.RoutePrefix);
        group.AddEndpointFilter<IntegratedS3RequestAuthenticationEndpointFilter>();

        group.MapGet("/", HandleRootGetAsync)
            .WithName("GetIntegratedS3ServiceDocument");

        group.MapMethods("/", ["PUT", "HEAD", "DELETE", "POST"], HandleS3CompatibleRootAsync)
            .WithName("HandleIntegratedS3CompatibleRoot");

        group.MapGet("/capabilities", GetCapabilitiesAsync)
            .WithName("GetIntegratedS3Capabilities");

        group.MapGet("/buckets", ListBucketsAsync)
            .WithName("ListIntegratedS3Buckets");

        group.MapPut("/buckets/{bucketName}", CreateBucketAsync)
            .WithName("CreateIntegratedS3Bucket");

        group.MapMethods("/buckets/{bucketName}", ["HEAD"], HeadBucketAsync)
            .WithName("HeadIntegratedS3Bucket");

        group.MapDelete("/buckets/{bucketName}", DeleteBucketAsync)
            .WithName("DeleteIntegratedS3Bucket");

        group.MapGet("/buckets/{bucketName}/objects", ListObjectsAsync)
            .WithName("ListIntegratedS3Objects");

        group.MapPut("/buckets/{bucketName}/objects/{**key}", PutObjectAsync)
            .WithName("PutIntegratedS3Object");

        group.MapGet("/buckets/{bucketName}/objects/{**key}", GetObjectAsync)
            .WithName("GetIntegratedS3Object");

        group.MapMethods("/buckets/{bucketName}/objects/{**key}", ["HEAD"], HeadObjectAsync)
            .WithName("HeadIntegratedS3Object");

        group.MapDelete("/buckets/{bucketName}/objects/{**key}", DeleteObjectAsync)
            .WithName("DeleteIntegratedS3Object");

        group.MapMethods("/{**s3Path}", ["GET", "PUT", "HEAD", "DELETE", "POST"], HandleS3CompatiblePathAsync)
            .WithName("HandleIntegratedS3CompatiblePath");

        return group;
    }

    private static async Task<IResult> HandleRootGetAsync(
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        IStorageServiceDescriptorProvider descriptorProvider,
        CancellationToken cancellationToken)
    {
        if (TryResolveCompatibleRequest(httpContext.Request, options.Value, null, out var resolvedRequest, out var resolutionError)
            && resolvedRequest is not null
            && !string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            if (!string.IsNullOrWhiteSpace(resolutionError)) {
                return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError, resource: null);
            }

            return await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, requestContextAccessor, storageService, cancellationToken);
        }

        if (IsSigV4AuthenticatedRequest(httpContext)) {
            return await ListBucketsS3CompatibleAsync(httpContext, requestContextAccessor, storageService, descriptorProvider, cancellationToken);
        }

        return await GetServiceDocumentAsync(descriptorProvider, cancellationToken);
    }

    private static async Task<Ok<StorageServiceDocument>> GetServiceDocumentAsync(
        IStorageServiceDescriptorProvider descriptorProvider,
        CancellationToken cancellationToken)
    {
        var descriptor = await descriptorProvider.GetServiceDescriptorAsync(cancellationToken);
        return TypedResults.Ok(StorageServiceDocument.FromDescriptor(descriptor));
    }

    private static async Task<Ok<StorageCapabilities>> GetCapabilitiesAsync(
        IStorageCapabilityProvider capabilityProvider,
        CancellationToken cancellationToken)
    {
        var capabilities = await capabilityProvider.GetCapabilitiesAsync(cancellationToken);
        return TypedResults.Ok(capabilities);
    }

    private static async Task<Ok<BucketInfo[]>> ListBucketsAsync(
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
            try {
                var buckets = await storageService.ListBucketsAsync(innerCancellationToken).ToArrayAsync(innerCancellationToken);
                return TypedResults.Ok(buckets);
            }
            catch (StorageAuthorizationException exception) {
                throw new EndpointStorageAuthorizationException(exception.Error);
            }
        }, cancellationToken);
    }

    private static async Task<IResult> ListBucketsS3CompatibleAsync(
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        IStorageServiceDescriptorProvider descriptorProvider,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                try {
                    var buckets = await storageService.ListBucketsAsync(innerCancellationToken).ToArrayAsync(innerCancellationToken);
                    var descriptor = await descriptorProvider.GetServiceDescriptorAsync(innerCancellationToken);

                    return new XmlContentResult(
                        S3XmlResponseWriter.WriteListAllMyBucketsResult(new S3ListAllMyBucketsResult
                        {
                            Owner = new S3BucketOwner
                            {
                                Id = "integrated-s3",
                                DisplayName = descriptor.ServiceName
                            },
                            Buckets = buckets.Select(static bucket => new S3BucketListEntry
                            {
                                Name = bucket.Name,
                                CreationDateUtc = bucket.CreatedAtUtc
                            }).ToArray()
                        }),
                        StatusCodes.Status200OK,
                        XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error);
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error);
        }
    }

    private static async Task<IResult> CreateBucketAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.CreateBucketAsync(new CreateBucketRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken).AsTask(), cancellationToken);

            return result.IsSuccess
                ? TypedResults.Created($"buckets/{bucketName}", result.Value)
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidBucketName", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> HandleS3CompatibleRootAsync(
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryResolveCompatibleRequest(httpContext.Request, options.Value, null, out var resolvedRequest, out var resolutionError)
            || resolvedRequest is null
            || string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError ?? "A bucket name could not be resolved from the request.", resource: null);
        }

        return await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, requestContextAccessor, storageService, cancellationToken);
    }

    private static async Task<IResult> HandleS3CompatiblePathAsync(
        string s3Path,
        HttpContext httpContext,
        IOptions<IntegratedS3Options> options,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryResolveCompatibleRequest(httpContext.Request, options.Value, s3Path, out var resolvedRequest, out var resolutionError)
            || resolvedRequest is null
            || string.IsNullOrWhiteSpace(resolvedRequest.BucketName)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", resolutionError ?? "A bucket name could not be resolved from the request.", resource: null);
        }

        return resolvedRequest.Key is null
            ? await ExecuteS3CompatibleBucketRequestAsync(resolvedRequest, httpContext, requestContextAccessor, storageService, cancellationToken)
            : await ExecuteS3CompatibleObjectRequestAsync(resolvedRequest, httpContext, requestContextAccessor, storageService, cancellationToken);
    }

    private static async Task<IResult> HeadBucketAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor,
                innerCancellationToken => storageService.HeadBucketAsync(bucketName, innerCancellationToken).AsTask(),
                cancellationToken);
            return result.IsSuccess
                ? TypedResults.Ok()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidBucketName", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> DeleteBucketAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.DeleteBucketAsync(new DeleteBucketRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken).AsTask(), cancellationToken);

            return result.IsSuccess
                ? TypedResults.NoContent()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidBucketName", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> ListObjectsAsync(
        string bucketName,
        string? prefix,
        string? continuationToken,
        int? pageSize,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (pageSize is <= 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Page size must be greater than zero.", BuildObjectResource(bucketName, null), bucketName);
                }

                var requestedPageSize = pageSize;
                var fetchPageSize = requestedPageSize switch
                {
                    null => null,
                    int.MaxValue => int.MaxValue,
                    _ => requestedPageSize + 1
                };

                try {
                    var objects = await storageService.ListObjectsAsync(new ListObjectsRequest
                    {
                        BucketName = bucketName,
                        Prefix = prefix,
                        ContinuationToken = continuationToken,
                        PageSize = fetchPageSize
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    if (requestedPageSize is null || objects.Length <= requestedPageSize.Value) {
                        httpContext.Response.Headers.Remove(ContinuationTokenHeaderName);
                        return TypedResults.Ok(objects);
                    }

                    var page = objects.Take(requestedPageSize.Value).ToArray();
                    httpContext.Response.Headers[ContinuationTokenHeaderName] = page[^1].Key;

                    return TypedResults.Ok(page);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> PutObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        HttpRequest request,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var preparedBody = await PrepareRequestBodyAsync(request, cancellationToken);
            try {
                return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                if (TryGetCopySource(request, out var copySource, out var copySourceError)) {
                    if (copySourceError is not null) {
                        return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", copySourceError, BuildObjectResource(bucketName, key), bucketName, key);
                    }

                    var copyResult = await storageService.CopyObjectAsync(new CopyObjectRequest
                    {
                        SourceBucketName = copySource!.BucketName,
                        SourceKey = copySource.Key,
                        SourceVersionId = copySource.VersionId,
                        DestinationBucketName = bucketName,
                        DestinationKey = key,
                        SourceIfMatchETag = request.Headers[CopySourceIfMatchHeaderName].ToString(),
                        SourceIfNoneMatchETag = request.Headers[CopySourceIfNoneMatchHeaderName].ToString(),
                        SourceIfModifiedSinceUtc = ParseOptionalHttpDateHeader(request.Headers[CopySourceIfModifiedSinceHeaderName].ToString()),
                        SourceIfUnmodifiedSinceUtc = ParseOptionalHttpDateHeader(request.Headers[CopySourceIfUnmodifiedSinceHeaderName].ToString())
                    }, innerCancellationToken);

                    return copyResult.IsSuccess
                        ? ToCopyObjectResult(httpContext, copyResult.Value!)
                        : ToErrorResult(httpContext, copyResult.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                if (!TryParseRequestChecksums(request, requireChecksumValueForDeclaredAlgorithm: true, out _, out var requestedChecksums, out var checksumErrorResult)) {
                    return checksumErrorResult!;
                }

                var metadata = request.Headers
                    .Where(static pair => pair.Key.StartsWith(MetadataHeaderPrefix, StringComparison.OrdinalIgnoreCase))
                    .ToDictionary(
                        static pair => pair.Key[MetadataHeaderPrefix.Length..],
                        static pair => pair.Value.ToString(),
                        StringComparer.OrdinalIgnoreCase);

                var result = await storageService.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    Content = preparedBody.Content,
                    ContentLength = preparedBody.ContentLength,
                    ContentType = request.ContentType,
                    Metadata = metadata.Count == 0 ? null : metadata,
                    Checksums = requestedChecksums
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyObjectHeaders(httpContext.Response, result.Value);
                }

                return result.IsSuccess
                    ? TypedResults.Ok(result.Value)
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }, cancellationToken);
            }
            finally {
                await preparedBody.DisposeAsync();
            }
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> GetObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        HttpRequest request,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var headers = request.GetTypedHeaders();
                var result = await storageService.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(request),
                    Range = ParseRangeHeader(request.Headers.Range.ToString()),
                    IfMatchETag = request.Headers.IfMatch.ToString(),
                    IfNoneMatchETag = request.Headers.IfNoneMatch.ToString(),
                    IfModifiedSinceUtc = headers.IfModifiedSince,
                    IfUnmodifiedSinceUtc = headers.IfUnmodifiedSince
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                return new StreamObjectResult(result.Value!);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status416RangeNotSatisfiable, "InvalidRange", exception.Message, BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> HeadObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var headers = httpContext.Request.GetTypedHeaders();
                var result = await storageService.HeadObjectAsync(new HeadObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request),
                    IfMatchETag = httpContext.Request.Headers.IfMatch.ToString(),
                    IfNoneMatchETag = httpContext.Request.Headers.IfNoneMatch.ToString(),
                    IfModifiedSinceUtc = headers.IfModifiedSince,
                    IfUnmodifiedSinceUtc = headers.IfUnmodifiedSince
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var objectInfo = result.Value!;
                ApplyObjectHeaders(httpContext.Response, objectInfo);
                httpContext.Response.Headers.AcceptRanges = "bytes";

                if (!MatchesIfMatch(httpContext.Request.Headers.IfMatch.ToString(), objectInfo.ETag)) {
                    return TypedResults.StatusCode(StatusCodes.Status412PreconditionFailed);
                }

                if (string.IsNullOrWhiteSpace(httpContext.Request.Headers.IfMatch)
                    && headers.IfUnmodifiedSince is { } ifUnmodifiedSinceUtc
                    && WasModifiedAfter(objectInfo.LastModifiedUtc, ifUnmodifiedSinceUtc)) {
                    return TypedResults.StatusCode(StatusCodes.Status412PreconditionFailed);
                }

                if (MatchesAnyETag(httpContext.Request.Headers.IfNoneMatch.ToString(), objectInfo.ETag)) {
                    return TypedResults.StatusCode(StatusCodes.Status304NotModified);
                }

                if (string.IsNullOrWhiteSpace(httpContext.Request.Headers.IfNoneMatch)
                    && headers.IfModifiedSince is { } ifModifiedSinceUtc
                    && !WasModifiedAfter(objectInfo.LastModifiedUtc, ifModifiedSinceUtc)) {
                    return TypedResults.StatusCode(StatusCodes.Status304NotModified);
                }

                httpContext.Response.ContentLength = objectInfo.ContentLength;
                httpContext.Response.ContentType = objectInfo.ContentType ?? "application/octet-stream";

                return TypedResults.Ok();
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> DeleteObjectAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.DeleteObjectAsync(new DeleteObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request)
                }, innerCancellationToken).AsTask(), cancellationToken);

            if (result.IsSuccess && result.Value is not null) {
                ApplyDeleteObjectHeaders(httpContext.Response, result.Value);
            }

            return result.IsSuccess
                ? TypedResults.NoContent()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> ExecuteS3CompatibleBucketRequestAsync(
        ResolvedS3Request resolvedRequest,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryValidateBucketRequestSubresources(httpContext.Request, out var validationErrorCode, out var validationMessage, out var validationStatusCode)) {
            return ToErrorResult(httpContext, validationStatusCode, validationErrorCode!, validationMessage!, resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName);
        }

        return httpContext.Request.Method switch
        {
            "GET" when httpContext.Request.Query.ContainsKey(VersioningQueryParameterName) => await GetBucketVersioningAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" when httpContext.Request.Query.ContainsKey(VersionsQueryParameterName) => await ListObjectVersionsAsync(
                resolvedRequest.BucketName,
                ParsePrefix(httpContext.Request),
                ParseDelimiter(httpContext.Request),
                ParseKeyMarker(httpContext.Request),
                ParseVersionIdMarker(httpContext.Request),
                ParseMaxKeys(httpContext.Request),
                httpContext,
                requestContextAccessor,
                storageService,
                cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(VersioningQueryParameterName) => await PutBucketVersioningAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            _ => httpContext.Request.Method switch
        {
            "GET" => await ListObjectsV2Async(
                resolvedRequest.BucketName,
                ParsePrefix(httpContext.Request),
                ParseDelimiter(httpContext.Request),
                ParseStartAfter(httpContext.Request),
                ParseContinuationToken(httpContext.Request),
                ParseMaxKeys(httpContext.Request),
                httpContext,
                requestContextAccessor,
                storageService,
                cancellationToken),
            "PUT" => await CreateBucketS3CompatibleAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "HEAD" => await HeadBucketAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" => await DeleteBucketAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when httpContext.Request.Query.ContainsKey("delete") => await DeleteObjectsAsync(resolvedRequest.BucketName, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" => ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidRequest", "Unsupported bucket subresource request.", resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName),
            _ => TypedResults.StatusCode(StatusCodes.Status405MethodNotAllowed)
        }};
    }

    private static async Task<IResult> GetBucketVersioningAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetBucketVersioningAsync(bucketName, innerCancellationToken);
                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var status = result.Value!.Status switch
                {
                    BucketVersioningStatus.Enabled => "Enabled",
                    BucketVersioningStatus.Suspended => "Suspended",
                    _ => null
                };

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteBucketVersioningConfiguration(new S3BucketVersioningConfiguration
                    {
                        Status = status
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> PutBucketVersioningAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3BucketVersioningConfiguration requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadBucketVersioningConfigurationAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        var requestedStatus = requestBody.Status switch
        {
            null or "" => BucketVersioningStatus.Disabled,
            "Enabled" => BucketVersioningStatus.Enabled,
            "Suspended" => BucketVersioningStatus.Suspended,
            _ => (BucketVersioningStatus?)null
        };

        if (requestedStatus is null) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "Bucket versioning status must be 'Enabled' or 'Suspended'.", BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutBucketVersioningAsync(new PutBucketVersioningRequest
                {
                    BucketName = bucketName,
                    Status = requestedStatus.Value
                }, innerCancellationToken);

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<IResult> CreateBucketS3CompatibleAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.CreateBucketAsync(new CreateBucketRequest
                {
                    BucketName = bucketName
                }, innerCancellationToken).AsTask(), cancellationToken);

            return result.IsSuccess
                ? TypedResults.Ok()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidBucketName", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> ExecuteS3CompatibleObjectRequestAsync(
        ResolvedS3Request resolvedRequest,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        var key = resolvedRequest.Key!;

        if (!TryValidateObjectRequestSubresources(httpContext.Request, out var validationErrorCode, out var validationMessage, out var validationStatusCode)) {
            return ToErrorResult(httpContext, validationStatusCode, validationErrorCode!, validationMessage!, resolvedRequest.CanonicalResourcePath, resolvedRequest.BucketName, key);
        }

        return httpContext.Request.Method switch
        {
            "GET" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await GetObjectTaggingAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await PutObjectTaggingAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when httpContext.Request.Query.ContainsKey(TaggingQueryParameterName) => await DeleteObjectTaggingAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when httpContext.Request.Query.ContainsKey(UploadsQueryParameterName) => await InitiateMultipartUploadAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "PUT" when TryGetMultipartUploadId(httpContext.Request, out _, out _) && httpContext.Request.Query.ContainsKey(PartNumberQueryParameterName) => await UploadMultipartPartAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "POST" when TryGetMultipartUploadId(httpContext.Request, out _, out _) => await CompleteMultipartUploadAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" when TryGetMultipartUploadId(httpContext.Request, out _, out _) => await AbortMultipartUploadAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "GET" => await GetObjectAsync(resolvedRequest.BucketName, key, httpContext, httpContext.Request, requestContextAccessor, storageService, cancellationToken),
            "PUT" => await PutObjectAsync(resolvedRequest.BucketName, key, httpContext, httpContext.Request, requestContextAccessor, storageService, cancellationToken),
            "HEAD" => await HeadObjectAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            "DELETE" => await DeleteObjectAsync(resolvedRequest.BucketName, key, httpContext, requestContextAccessor, storageService, cancellationToken),
            _ => TypedResults.StatusCode(StatusCodes.Status405MethodNotAllowed)
        };
    }

    private static async Task<IResult> GetObjectTaggingAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.GetObjectTagsAsync(new GetObjectTagsRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request)
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyObjectTaggingHeaders(httpContext.Response, result.Value);
                }

                return result.IsSuccess
                    ? new XmlContentResult(
                        S3XmlResponseWriter.WriteObjectTagging(new S3ObjectTagging
                        {
                            TagSet = result.Value!.Tags
                                .OrderBy(static pair => pair.Key, StringComparer.Ordinal)
                                .Select(static pair => new S3ObjectTag
                                {
                                    Key = pair.Key,
                                    Value = pair.Value
                                })
                                .ToArray()
                        }),
                        StatusCodes.Status200OK,
                        XmlContentType)
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> PutObjectTaggingAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3ObjectTagging requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadObjectTaggingAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.PutObjectTagsAsync(new PutObjectTagsRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request),
                    Tags = requestBody.TagSet.ToDictionary(static tag => tag.Key, static tag => tag.Value, StringComparer.Ordinal)
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyObjectTaggingHeaders(httpContext.Response, result.Value);
                }

                return result.IsSuccess
                    ? TypedResults.Ok()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> DeleteObjectTaggingAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.DeleteObjectTagsAsync(new DeleteObjectTagsRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = ParseVersionId(httpContext.Request)
                }, innerCancellationToken);

                if (result.IsSuccess && result.Value is not null) {
                    ApplyObjectTaggingHeaders(httpContext.Response, result.Value);
                }

                return result.IsSuccess
                    ? TypedResults.NoContent()
                    : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> InitiateMultipartUploadAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                if (!TryParseRequestChecksums(httpContext.Request, requireChecksumValueForDeclaredAlgorithm: false, out var checksumAlgorithm, out _, out var checksumErrorResult)) {
                    return checksumErrorResult!;
                }

                var metadata = httpContext.Request.Headers
                    .Where(static pair => pair.Key.StartsWith(MetadataHeaderPrefix, StringComparison.OrdinalIgnoreCase))
                    .ToDictionary(
                        static pair => pair.Key[MetadataHeaderPrefix.Length..],
                        static pair => pair.Value.ToString(),
                        StringComparer.OrdinalIgnoreCase);

                var result = await storageService.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentType = httpContext.Request.ContentType,
                    Metadata = metadata.Count == 0 ? null : metadata,
                    ChecksumAlgorithm = checksumAlgorithm
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                ApplyChecksumAlgorithmHeader(httpContext.Response, result.Value!.ChecksumAlgorithm);
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteInitiateMultipartUploadResult(new S3InitiateMultipartUploadResult
                    {
                        Bucket = bucketName,
                        Key = key,
                        UploadId = result.Value.UploadId,
                        ChecksumAlgorithm = ToS3ChecksumAlgorithmValue(result.Value.ChecksumAlgorithm)
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> UploadMultipartPartAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryGetMultipartUploadId(httpContext.Request, out var uploadId, out var uploadIdError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", uploadIdError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        if (!TryGetPartNumber(httpContext.Request, out var partNumber, out var partNumberError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", partNumberError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            if (!TryParseRequestChecksums(httpContext.Request, requireChecksumValueForDeclaredAlgorithm: false, out var checksumAlgorithm, out var requestedChecksums, out var checksumErrorResult)) {
                return checksumErrorResult!;
            }

            var preparedBody = await PrepareRequestBodyAsync(httpContext.Request, cancellationToken);
            try {
                return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                    var result = await storageService.UploadMultipartPartAsync(new UploadMultipartPartRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        UploadId = uploadId!,
                        PartNumber = partNumber!.Value,
                        Content = preparedBody.Content,
                        ContentLength = preparedBody.ContentLength,
                        ChecksumAlgorithm = checksumAlgorithm,
                        Checksums = requestedChecksums
                    }, innerCancellationToken);

                    if (!result.IsSuccess) {
                        return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                    }

                    httpContext.Response.Headers.ETag = QuoteETag(result.Value!.ETag);
                    ApplyChecksumHeaders(httpContext.Response, result.Value.Checksums);
                    return TypedResults.Ok();
                }, cancellationToken);
            }
            finally {
                await preparedBody.DisposeAsync();
            }
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> CompleteMultipartUploadAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryGetMultipartUploadId(httpContext.Request, out var uploadId, out var uploadIdError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", uploadIdError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        S3CompleteMultipartUploadRequest requestBody;
        try {
            requestBody = await S3XmlRequestReader.ReadCompleteMultipartUploadRequestAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var result = await storageService.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = uploadId!,
                    Parts = requestBody.Parts.Select(static part => new MultipartUploadPart
                    {
                        PartNumber = part.PartNumber,
                        ETag = part.ETag,
                        ContentLength = 0,
                        LastModifiedUtc = default,
                        Checksums = part.Checksums
                    }).ToArray()
                }, innerCancellationToken);

                if (!result.IsSuccess) {
                    return ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
                }

                var completedObject = result.Value!;
                ApplyObjectIdentityHeaders(httpContext.Response, completedObject);
                return new XmlContentResult(
                    S3XmlResponseWriter.WriteCompleteMultipartUploadResult(new S3CompleteMultipartUploadResult
                    {
                        Location = $"{httpContext.Request.Scheme}://{httpContext.Request.Host}{httpContext.Request.PathBase}{httpContext.Request.Path}",
                        Bucket = bucketName,
                        Key = key,
                        ETag = completedObject.ETag ?? string.Empty,
                        ChecksumCrc32 = GetChecksumValue(completedObject.Checksums, "crc32"),
                        ChecksumSha256 = GetChecksumValue(completedObject.Checksums, "sha256"),
                        ChecksumType = GetChecksumType(completedObject.Checksums)
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> AbortMultipartUploadAsync(
        string bucketName,
        string key,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        if (!TryGetMultipartUploadId(httpContext.Request, out var uploadId, out var uploadIdError)) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", uploadIdError!, BuildObjectResource(bucketName, key), bucketName, key);
        }

        try {
            var result = await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, innerCancellationToken =>
                storageService.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = uploadId!
                }, innerCancellationToken).AsTask(), cancellationToken);

            return result.IsSuccess
                ? TypedResults.NoContent()
                : ToErrorResult(httpContext, result.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, key));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, key));
        }
    }

    private static async Task<IResult> ListObjectsV2Async(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? startAfter,
        string? continuationToken,
        int? maxKeys,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (maxKeys is <= 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "max-keys must be greater than zero.", BuildObjectResource(bucketName, null), bucketName);
                }

                var requestedPageSize = maxKeys ?? 1000;

                try {
                    var objects = await storageService.ListObjectsAsync(new ListObjectsRequest
                    {
                        BucketName = bucketName,
                        Prefix = prefix
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    var response = BuildListBucketResult(
                        bucketName,
                        prefix,
                        delimiter,
                        startAfter,
                        continuationToken,
                        requestedPageSize,
                        objects);

                    return new XmlContentResult(S3XmlResponseWriter.WriteListBucketResult(response), StatusCodes.Status200OK, XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> ListObjectVersionsAsync(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? versionIdMarker,
        int? maxKeys,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                if (maxKeys is <= 0) {
                    return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", "max-keys must be greater than zero.", BuildObjectResource(bucketName, null), bucketName);
                }

                var requestedPageSize = maxKeys ?? 1000;

                try {
                    var versions = await storageService.ListObjectVersionsAsync(new ListObjectVersionsRequest
                    {
                        BucketName = bucketName,
                        Prefix = prefix,
                        Delimiter = delimiter,
                        KeyMarker = keyMarker,
                        VersionIdMarker = versionIdMarker
                    }, innerCancellationToken).ToArrayAsync(innerCancellationToken);

                    var response = BuildListObjectVersionsResult(
                        bucketName,
                        prefix,
                        delimiter,
                        keyMarker,
                        versionIdMarker,
                        requestedPageSize,
                        versions);

                    return new XmlContentResult(S3XmlResponseWriter.WriteListObjectVersionsResult(response), StatusCodes.Status200OK, XmlContentType);
                }
                catch (StorageAuthorizationException exception) {
                    return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
        catch (ArgumentException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "InvalidArgument", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }
    }

    private static async Task<IResult> DeleteObjectsAsync(
        string bucketName,
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        IStorageService storageService,
        CancellationToken cancellationToken)
    {
        S3DeleteObjectsRequest deleteRequest;
        try {
            deleteRequest = await S3XmlRequestReader.ReadDeleteObjectsRequestAsync(httpContext.Request.Body, cancellationToken);
        }
        catch (FormatException exception) {
            return ToErrorResult(httpContext, StatusCodes.Status400BadRequest, "MalformedXML", exception.Message, BuildObjectResource(bucketName, null), bucketName);
        }

        try {
            return await ExecuteWithRequestContextAsync(httpContext, requestContextAccessor, async innerCancellationToken => {
                var bucketResult = await storageService.HeadBucketAsync(bucketName, innerCancellationToken);
                if (!bucketResult.IsSuccess) {
                    return ToErrorResult(httpContext, bucketResult.Error, resourceOverride: BuildObjectResource(bucketName, null));
                }

                var deleted = new List<S3DeletedObjectResult>(deleteRequest.Objects.Count);
                var errors = new List<S3DeleteObjectError>();

                foreach (var objectIdentifier in deleteRequest.Objects) {
                    var result = await storageService.DeleteObjectAsync(new DeleteObjectRequest
                    {
                        BucketName = bucketName,
                        Key = objectIdentifier.Key,
                        VersionId = objectIdentifier.VersionId
                    }, innerCancellationToken);

                    if (result.IsSuccess || result.Error?.Code == StorageErrorCode.ObjectNotFound) {
                        if (!deleteRequest.Quiet) {
                            deleted.Add(new S3DeletedObjectResult
                            {
                                Key = objectIdentifier.Key,
                                VersionId = result.Value?.IsDeleteMarker == true ? null : result.Value?.VersionId ?? objectIdentifier.VersionId,
                                DeleteMarker = result.Value?.IsDeleteMarker == true,
                                DeleteMarkerVersionId = result.Value?.IsDeleteMarker == true ? result.Value.VersionId : null
                            });
                        }

                        continue;
                    }

                    errors.Add(new S3DeleteObjectError
                    {
                        Key = objectIdentifier.Key,
                        VersionId = objectIdentifier.VersionId,
                        Code = ToS3ErrorCode(result.Error!.Code),
                        Message = result.Error.Message
                    });
                }

                return new XmlContentResult(
                    S3XmlResponseWriter.WriteDeleteObjectsResult(new S3DeleteObjectsResult
                    {
                        Deleted = deleted,
                        Errors = errors
                    }),
                    StatusCodes.Status200OK,
                    XmlContentType);
            }, cancellationToken);
        }
        catch (EndpointStorageAuthorizationException exception) {
            return ToErrorResult(httpContext, exception.Error, resourceOverride: BuildObjectResource(bucketName, null));
        }
    }

    private static async Task<T> ExecuteWithRequestContextAsync<T>(
        HttpContext httpContext,
        IIntegratedS3RequestContextAccessor requestContextAccessor,
        Func<CancellationToken, Task<T>> action,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(requestContextAccessor);
        ArgumentNullException.ThrowIfNull(action);

        var previousContext = requestContextAccessor.Current;
        requestContextAccessor.Current = new IntegratedS3RequestContext
        {
            Principal = httpContext.User
        };

        try {
            return await action(cancellationToken);
        }
        catch (StorageAuthorizationException exception) {
            throw new EndpointStorageAuthorizationException(exception.Error);
        }
        finally {
            requestContextAccessor.Current = previousContext;
        }
    }

    private static IResult ToErrorResult(HttpContext httpContext, StorageError? error, string? resourceOverride = null)
    {
        if (error is null) {
            return ToErrorResult(httpContext, StatusCodes.Status500InternalServerError, "InternalError", "Storage operation failed.", resourceOverride);
        }

        return ToErrorResult(
            httpContext,
            error.SuggestedHttpStatusCode ?? ToStatusCode(error.Code),
            ToS3ErrorCode(error.Code),
            error.Message,
            resourceOverride ?? BuildResource(error.BucketName, error.ObjectKey),
            error.BucketName,
            error.ObjectKey);
    }

    private static IResult ToErrorResult(
        HttpContext httpContext,
        int statusCode,
        string code,
        string message,
        string? resource,
        string? bucketName = null,
        string? key = null)
    {
        return new XmlContentResult(
            S3XmlResponseWriter.WriteError(new S3ErrorResponse
            {
                Code = code,
                Message = message,
                Resource = resource,
                RequestId = httpContext.TraceIdentifier,
                BucketName = bucketName,
                Key = key
            }),
            statusCode,
            XmlContentType);
    }

    private static IResult ToCopyObjectResult(HttpContext httpContext, ObjectInfo @object)
    {
        ApplyObjectHeaders(httpContext.Response, @object);

        return new XmlContentResult(
            S3XmlResponseWriter.WriteCopyObjectResult(new S3CopyObjectResult
            {
                ETag = @object.ETag ?? string.Empty,
                LastModifiedUtc = @object.LastModifiedUtc
            }),
            StatusCodes.Status200OK,
            XmlContentType);
    }

    private static int ToStatusCode(StorageErrorCode code)
    {
        return code switch
        {
            StorageErrorCode.ObjectNotFound => StatusCodes.Status404NotFound,
            StorageErrorCode.BucketNotFound => StatusCodes.Status404NotFound,
            StorageErrorCode.AccessDenied => StatusCodes.Status403Forbidden,
            StorageErrorCode.InvalidChecksum => StatusCodes.Status400BadRequest,
            StorageErrorCode.InvalidRange => StatusCodes.Status416RangeNotSatisfiable,
            StorageErrorCode.PreconditionFailed => StatusCodes.Status412PreconditionFailed,
            StorageErrorCode.VersionConflict => StatusCodes.Status409Conflict,
            StorageErrorCode.BucketAlreadyExists => StatusCodes.Status409Conflict,
            StorageErrorCode.MultipartConflict => StatusCodes.Status409Conflict,
            StorageErrorCode.Throttled => StatusCodes.Status429TooManyRequests,
            StorageErrorCode.ProviderUnavailable => StatusCodes.Status503ServiceUnavailable,
            StorageErrorCode.UnsupportedCapability => StatusCodes.Status501NotImplemented,
            StorageErrorCode.QuotaExceeded => StatusCodes.Status413PayloadTooLarge,
            _ => StatusCodes.Status500InternalServerError
        };
    }

    private static string ToS3ErrorCode(StorageErrorCode code)
    {
        return code switch
        {
            StorageErrorCode.ObjectNotFound => "NoSuchKey",
            StorageErrorCode.BucketNotFound => "NoSuchBucket",
            StorageErrorCode.AccessDenied => "AccessDenied",
            StorageErrorCode.InvalidChecksum => "BadDigest",
            StorageErrorCode.InvalidRange => "InvalidRange",
            StorageErrorCode.PreconditionFailed => "PreconditionFailed",
            StorageErrorCode.VersionConflict => "OperationAborted",
            StorageErrorCode.BucketAlreadyExists => "BucketAlreadyExists",
            StorageErrorCode.MultipartConflict => "InvalidRequest",
            StorageErrorCode.Throttled => "SlowDown",
            StorageErrorCode.ProviderUnavailable => "ServiceUnavailable",
            StorageErrorCode.UnsupportedCapability => "NotImplemented",
            StorageErrorCode.QuotaExceeded => "EntityTooLarge",
            _ => "InternalError"
        };
    }

    private static string? BuildResource(string? bucketName, string? key)
    {
        if (string.IsNullOrWhiteSpace(bucketName)) {
            return null;
        }

        return BuildObjectResource(bucketName, key);
    }

    private static string BuildObjectResource(string bucketName, string? key)
    {
        return string.IsNullOrWhiteSpace(key)
            ? $"/{bucketName}"
            : $"/{bucketName}/{key}";
    }

    private static bool IsSigV4AuthenticatedRequest(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        return httpContext.User.HasClaim(SigV4AuthenticationClaimType, SigV4AuthenticationClaimValue);
    }

    private static bool TryResolveCompatibleRequest(
        HttpRequest request,
        IntegratedS3Options options,
        string? s3Path,
        out ResolvedS3Request? resolvedRequest,
        out string? error)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(options);

        var normalizedPath = string.IsNullOrWhiteSpace(s3Path)
            ? null
            : s3Path.Trim('/');

        var virtualHostedBucketName = TryResolveVirtualHostedBucketName(request.Host, options);
        if (!string.IsNullOrWhiteSpace(virtualHostedBucketName)) {
            resolvedRequest = CreateResolvedRequest(virtualHostedBucketName, string.IsNullOrWhiteSpace(normalizedPath) ? null : normalizedPath, S3AddressingStyle.VirtualHosted);
            error = null;
            return true;
        }

        if (string.IsNullOrWhiteSpace(normalizedPath)) {
            resolvedRequest = null;
            error = null;
            return false;
        }

        var separatorIndex = normalizedPath.IndexOf('/');
        var bucketName = separatorIndex < 0
            ? normalizedPath
            : normalizedPath[..separatorIndex];

        if (string.IsNullOrWhiteSpace(bucketName)) {
            resolvedRequest = null;
            error = "The request path must contain a bucket name.";
            return false;
        }

        var key = separatorIndex < 0 || separatorIndex == normalizedPath.Length - 1
            ? null
            : normalizedPath[(separatorIndex + 1)..];

        resolvedRequest = CreateResolvedRequest(bucketName, key, S3AddressingStyle.Path);
        error = null;
        return true;
    }

    private static string? TryResolveVirtualHostedBucketName(HostString host, IntegratedS3Options options)
    {
        if (!options.EnableVirtualHostedStyleAddressing || options.VirtualHostedStyleHostSuffixes.Count == 0) {
            return null;
        }

        var hostValue = host.Host;
        if (string.IsNullOrWhiteSpace(hostValue)) {
            return null;
        }

        foreach (var suffix in options.VirtualHostedStyleHostSuffixes) {
            if (string.Equals(hostValue, suffix, StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            if (!hostValue.EndsWith($".{suffix}", StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            var bucketName = hostValue[..^(suffix.Length + 1)];
            if (!string.IsNullOrWhiteSpace(bucketName)) {
                return bucketName;
            }
        }

        return null;
    }

    private static ResolvedS3Request CreateResolvedRequest(string bucketName, string? key, S3AddressingStyle addressingStyle)
    {
        var canonicalResourcePath = string.IsNullOrWhiteSpace(key)
            ? $"/{bucketName}"
            : $"/{bucketName}/{key}";

        var canonicalPath = string.IsNullOrWhiteSpace(key)
            ? "/"
            : $"/{string.Join('/', key.Split('/', StringSplitOptions.RemoveEmptyEntries).Select(Uri.EscapeDataString))}";

        return new ResolvedS3Request(bucketName, key, addressingStyle, canonicalResourcePath, canonicalPath);
    }

    private static string? ParsePrefix(HttpRequest request)
    {
        return request.Query.TryGetValue(PrefixQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static string? ParseDelimiter(HttpRequest request)
    {
        if (!request.Query.TryGetValue(DelimiterQueryParameterName, out var values)) {
            return null;
        }

        var delimiter = values.ToString();
        return string.IsNullOrEmpty(delimiter)
            ? null
            : delimiter;
    }

    private static string? ParseStartAfter(HttpRequest request)
    {
        if (!request.Query.TryGetValue(StartAfterQueryParameterName, out var values)) {
            return null;
        }

        var startAfter = values.ToString();
        return string.IsNullOrWhiteSpace(startAfter)
            ? null
            : startAfter;
    }

    private static string? ParseContinuationToken(HttpRequest request)
    {
        return request.Query.TryGetValue(ContinuationTokenQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static string? ParseKeyMarker(HttpRequest request)
    {
        return request.Query.TryGetValue(KeyMarkerQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static string? ParseVersionIdMarker(HttpRequest request)
    {
        return request.Query.TryGetValue(VersionIdMarkerQueryParameterName, out var values)
            ? values.ToString()
            : null;
    }

    private static S3ListBucketResult BuildListBucketResult(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? startAfter,
        string? continuationToken,
        int maxKeys,
        IReadOnlyList<ObjectInfo> objects)
    {
        var normalizedPrefix = prefix ?? string.Empty;
        var normalizedDelimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter;
        var marker = string.IsNullOrWhiteSpace(continuationToken)
            ? startAfter
            : continuationToken;

        var entries = new List<ListBucketResultEntry>();

        for (var index = 0; index < objects.Count; index++) {
            var currentObject = objects[index];
            if (!currentObject.Key.StartsWith(normalizedPrefix, StringComparison.Ordinal)) {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(marker)
                && StringComparer.Ordinal.Compare(currentObject.Key, marker) <= 0) {
                continue;
            }

            if (!string.IsNullOrEmpty(normalizedDelimiter)) {
                var suffix = currentObject.Key[normalizedPrefix.Length..];
                var delimiterIndex = suffix.IndexOf(normalizedDelimiter, StringComparison.Ordinal);
                if (delimiterIndex >= 0) {
                    var commonPrefix = normalizedPrefix + suffix[..(delimiterIndex + normalizedDelimiter.Length)];
                    var lastObjectKey = currentObject.Key;

                    while (index + 1 < objects.Count) {
                        var nextObject = objects[index + 1];
                        if (!nextObject.Key.StartsWith(commonPrefix, StringComparison.Ordinal)) {
                            break;
                        }

                        lastObjectKey = nextObject.Key;
                        index++;
                    }

                    entries.Add(ListBucketResultEntry.ForCommonPrefix(commonPrefix, lastObjectKey));
                    continue;
                }
            }

            entries.Add(ListBucketResultEntry.ForObject(currentObject));
        }

        var isTruncated = entries.Count > maxKeys;
        var page = isTruncated
            ? entries.Take(maxKeys).ToArray()
            : entries.ToArray();

        return new S3ListBucketResult
        {
            Name = bucketName,
            Prefix = prefix,
            Delimiter = normalizedDelimiter,
            StartAfter = startAfter,
            ContinuationToken = continuationToken,
            NextContinuationToken = isTruncated ? page[^1].ContinuationToken : null,
            KeyCount = page.Length,
            MaxKeys = maxKeys,
            IsTruncated = isTruncated,
            Contents = page
                .Where(static entry => entry.Object is not null)
                .Select(static entry => new S3ListBucketObject
                {
                    Key = entry.Object!.Key,
                    ETag = entry.Object.ETag,
                    Size = entry.Object.ContentLength,
                    LastModifiedUtc = entry.Object.LastModifiedUtc
                })
                .ToArray(),
            CommonPrefixes = page
                .Where(static entry => entry.CommonPrefix is not null)
                .Select(static entry => new S3ListBucketCommonPrefix
                {
                    Prefix = entry.CommonPrefix!
                })
                .ToArray()
        };
    }

    private static S3ListObjectVersionsResult BuildListObjectVersionsResult(
        string bucketName,
        string? prefix,
        string? delimiter,
        string? keyMarker,
        string? versionIdMarker,
        int maxKeys,
        IReadOnlyList<ObjectInfo> versions)
    {
        var normalizedPrefix = prefix ?? string.Empty;
        var normalizedDelimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter;
        var entries = new List<ListObjectVersionsResultEntry>();

        for (var index = 0; index < versions.Count; index++) {
            var currentVersion = versions[index];
            if (!currentVersion.Key.StartsWith(normalizedPrefix, StringComparison.Ordinal)) {
                continue;
            }

            if (!IsVersionAfterMarker(currentVersion, keyMarker, versionIdMarker)) {
                continue;
            }

            if (!string.IsNullOrEmpty(normalizedDelimiter)) {
                var suffix = currentVersion.Key[normalizedPrefix.Length..];
                var delimiterIndex = suffix.IndexOf(normalizedDelimiter, StringComparison.Ordinal);
                if (delimiterIndex >= 0) {
                    var commonPrefix = normalizedPrefix + suffix[..(delimiterIndex + normalizedDelimiter.Length)];
                    var lastVersion = currentVersion;

                    while (index + 1 < versions.Count) {
                        var nextVersion = versions[index + 1];
                        if (!nextVersion.Key.StartsWith(commonPrefix, StringComparison.Ordinal)) {
                            break;
                        }

                        lastVersion = nextVersion;
                        index++;
                    }

                    entries.Add(ListObjectVersionsResultEntry.ForCommonPrefix(commonPrefix, lastVersion.Key, lastVersion.VersionId));
                    continue;
                }
            }

            entries.Add(ListObjectVersionsResultEntry.ForVersion(currentVersion));
        }

        var isTruncated = entries.Count > maxKeys;
        var page = isTruncated
            ? entries.Take(maxKeys).ToArray()
            : entries.ToArray();

        return new S3ListObjectVersionsResult
        {
            Name = bucketName,
            Prefix = prefix,
            Delimiter = normalizedDelimiter,
            KeyMarker = keyMarker,
            VersionIdMarker = versionIdMarker,
            NextKeyMarker = isTruncated ? page[^1].NextKeyMarker : null,
            NextVersionIdMarker = isTruncated ? page[^1].NextVersionIdMarker : null,
            MaxKeys = maxKeys,
            IsTruncated = isTruncated,
            Versions = page
                .Where(static entry => entry.Version is not null)
                .Select(static entry => new S3ObjectVersionEntry
                {
                    Key = entry.Version!.Key,
                    VersionId = entry.Version.VersionId ?? string.Empty,
                    IsLatest = entry.Version.IsLatest,
                    IsDeleteMarker = entry.Version.IsDeleteMarker,
                    ETag = entry.Version.ETag,
                    Size = entry.Version.ContentLength,
                    LastModifiedUtc = entry.Version.LastModifiedUtc
                })
                .ToArray(),
            CommonPrefixes = page
                .Where(static entry => entry.CommonPrefix is not null)
                .Select(static entry => new S3ListBucketCommonPrefix
                {
                    Prefix = entry.CommonPrefix!
                })
                .ToArray()
        };
    }

    private static int? ParseMaxKeys(HttpRequest request)
    {
        if (!request.Query.TryGetValue(MaxKeysQueryParameterName, out var values)
            || string.IsNullOrWhiteSpace(values.ToString())) {
            return 1000;
        }

        return int.TryParse(values.ToString(), out var parsedValue)
            ? parsedValue
            : throw new ArgumentException("The max-keys query parameter must be an integer.", MaxKeysQueryParameterName);
    }

    private static bool TryValidateBucketRequestSubresources(HttpRequest request, out string? errorCode, out string? errorMessage, out int statusCode)
    {
        var queryKeys = request.Query.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
        switch (request.Method) {
            case "GET":
                if (queryKeys.Contains(VersionsQueryParameterName)) {
                    var supportedVersionKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                    {
                        VersionsQueryParameterName,
                        PrefixQueryParameterName,
                        DelimiterQueryParameterName,
                        MaxKeysQueryParameterName,
                        KeyMarkerQueryParameterName,
                        VersionIdMarkerQueryParameterName
                    };

                    foreach (var queryKey in queryKeys) {
                        if (!supportedVersionKeys.Contains(queryKey)) {
                            errorCode = "NotImplemented";
                            errorMessage = $"Bucket subresource '{queryKey}' is not implemented.";
                            statusCode = StatusCodes.Status501NotImplemented;
                            return false;
                        }
                    }

                    break;
                }

                if (queryKeys.SetEquals([VersioningQueryParameterName])) {
                    break;
                }

                foreach (var queryKey in queryKeys) {
                    if (!SupportedBucketGetQueryParameters.Contains(queryKey)) {
                        errorCode = "NotImplemented";
                        errorMessage = $"Bucket subresource '{queryKey}' is not implemented.";
                        statusCode = StatusCodes.Status501NotImplemented;
                        return false;
                    }
                }

                if (request.Query.TryGetValue(ListTypeQueryParameterName, out var listTypeValue)
                    && !string.IsNullOrWhiteSpace(listTypeValue.ToString())
                    && !string.Equals(listTypeValue.ToString(), "2", StringComparison.Ordinal)) {
                    errorCode = "InvalidArgument";
                    errorMessage = "Only list-type=2 is supported for S3-compatible bucket listing.";
                    statusCode = StatusCodes.Status400BadRequest;
                    return false;
                }

                break;

            case "POST":
                foreach (var queryKey in queryKeys) {
                    if (!SupportedBucketPostQueryParameters.Contains(queryKey)) {
                        errorCode = "NotImplemented";
                        errorMessage = $"Bucket subresource '{queryKey}' is not implemented.";
                        statusCode = StatusCodes.Status501NotImplemented;
                        return false;
                    }
                }

                if (!queryKeys.Contains("delete")) {
                    errorCode = "InvalidRequest";
                    errorMessage = "Only POST ?delete is supported for bucket-compatible subresource operations.";
                    statusCode = StatusCodes.Status400BadRequest;
                    return false;
                }

                break;

            case "PUT":
                if (queryKeys.SetEquals([VersioningQueryParameterName])) {
                    break;
                }

                if (queryKeys.Count > 0) {
                    errorCode = "NotImplemented";
                    errorMessage = $"Bucket subresource '{queryKeys.OrderBy(static key => key, StringComparer.OrdinalIgnoreCase).First()}' is not implemented.";
                    statusCode = StatusCodes.Status501NotImplemented;
                    return false;
                }

                break;

            case "HEAD":
            case "DELETE":
                if (queryKeys.Count > 0) {
                    errorCode = "NotImplemented";
                    errorMessage = $"Bucket subresource '{queryKeys.OrderBy(static key => key, StringComparer.OrdinalIgnoreCase).First()}' is not implemented.";
                    statusCode = StatusCodes.Status501NotImplemented;
                    return false;
                }

                break;
        }

        errorCode = null;
        errorMessage = null;
        statusCode = StatusCodes.Status200OK;
        return true;
    }

    private static bool TryValidateObjectRequestSubresources(HttpRequest request, out string? errorCode, out string? errorMessage, out int statusCode)
    {
        var hasTagging = request.Query.ContainsKey(TaggingQueryParameterName);
        var hasUploads = request.Query.ContainsKey(UploadsQueryParameterName);
        var hasUploadId = request.Query.ContainsKey(UploadIdQueryParameterName);
        var hasPartNumber = request.Query.ContainsKey(PartNumberQueryParameterName);
        var hasVersionId = request.Query.ContainsKey(VersionIdQueryParameterName);
        if (!hasTagging && !hasUploads && !hasUploadId && !hasPartNumber && !hasVersionId) {
            errorCode = null;
            errorMessage = null;
            statusCode = StatusCodes.Status200OK;
            return true;
        }

        var queryKeys = request.Query.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var isVersionedObjectRequest = request.Method switch
        {
            "GET" when queryKeys.SetEquals([VersionIdQueryParameterName]) => true,
            "HEAD" when queryKeys.SetEquals([VersionIdQueryParameterName]) => true,
            "DELETE" when queryKeys.SetEquals([VersionIdQueryParameterName]) => true,
            _ => false
        };

        if (isVersionedObjectRequest) {
            errorCode = null;
            errorMessage = null;
            statusCode = StatusCodes.Status200OK;
            return true;
        }

        var isValidTaggingRequest = request.Method switch
        {
            "GET" when hasTagging && !hasUploads && !hasUploadId && !hasPartNumber && queryKeys.All(static key => key is TaggingQueryParameterName or VersionIdQueryParameterName) => true,
            "PUT" when hasTagging && !hasUploads && !hasUploadId && !hasPartNumber && queryKeys.All(static key => key is TaggingQueryParameterName or VersionIdQueryParameterName) => true,
            "DELETE" when hasTagging && !hasUploads && !hasUploadId && !hasPartNumber && queryKeys.All(static key => key is TaggingQueryParameterName or VersionIdQueryParameterName) => true,
            _ => false
        };

        if (isValidTaggingRequest) {
            errorCode = null;
            errorMessage = null;
            statusCode = StatusCodes.Status200OK;
            return true;
        }

        var isValidMultipartRequest = request.Method switch
        {
            "POST" when hasUploads && !hasTagging && !hasUploadId && !hasPartNumber => true,
            "PUT" when hasUploadId && hasPartNumber && !hasTagging && !hasUploads => true,
            "POST" when hasUploadId && !hasTagging && !hasUploads && !hasPartNumber => true,
            "DELETE" when hasUploadId && !hasTagging && !hasUploads && !hasPartNumber => true,
            _ => false
        };

        if (isValidMultipartRequest) {
            errorCode = null;
            errorMessage = null;
            statusCode = StatusCodes.Status200OK;
            return true;
        }

        errorCode = "NotImplemented";
        errorMessage = "The requested object subresource combination is not implemented.";
        statusCode = StatusCodes.Status501NotImplemented;
        return false;
    }

    private static bool TryGetMultipartUploadId(HttpRequest request, out string? uploadId, out string? error)
    {
        if (!request.Query.TryGetValue(UploadIdQueryParameterName, out var values)) {
            uploadId = null;
            error = $"The '{UploadIdQueryParameterName}' query parameter is required.";
            return false;
        }

        uploadId = values.ToString();
        if (string.IsNullOrWhiteSpace(uploadId)) {
            error = $"The '{UploadIdQueryParameterName}' query parameter must not be empty.";
            return false;
        }

        error = null;
        return true;
    }

    private static bool TryGetPartNumber(HttpRequest request, out int? partNumber, out string? error)
    {
        if (!request.Query.TryGetValue(PartNumberQueryParameterName, out var values)) {
            partNumber = null;
            error = $"The '{PartNumberQueryParameterName}' query parameter is required.";
            return false;
        }

        if (!int.TryParse(values.ToString(), out var parsedPartNumber) || parsedPartNumber <= 0) {
            partNumber = null;
            error = $"The '{PartNumberQueryParameterName}' query parameter must be a positive integer.";
            return false;
        }

        partNumber = parsedPartNumber;
        error = null;
        return true;
    }

    private static string QuoteETag(string value)
    {
        return string.IsNullOrWhiteSpace(value)
            ? "\"\""
            : value.StartsWith('"') ? value : $"\"{value}\"";
    }

    private static ObjectRange? ParseRangeHeader(string? rangeHeader)
    {
        if (string.IsNullOrWhiteSpace(rangeHeader)) {
            return null;
        }

        var trimmed = rangeHeader.Trim();
        if (!trimmed.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase)) {
            throw new FormatException("Only single byte range requests are supported.");
        }

        var value = trimmed[6..].Trim();
        if (value.Contains(',')) {
            throw new FormatException("Multiple byte ranges are not supported.");
        }

        var separatorIndex = value.IndexOf('-');
        if (separatorIndex < 0) {
            throw new FormatException("The Range header is malformed.");
        }

        var startText = value[..separatorIndex].Trim();
        var endText = value[(separatorIndex + 1)..].Trim();
        if (string.IsNullOrEmpty(startText) && string.IsNullOrEmpty(endText)) {
            throw new FormatException("The Range header must specify a start, an end, or both.");
        }

        long? start = null;
        long? end = null;

        if (!string.IsNullOrEmpty(startText)) {
            if (!long.TryParse(startText, out var parsedStart) || parsedStart < 0) {
                throw new FormatException("The Range header contains an invalid start offset.");
            }

            start = parsedStart;
        }

        if (!string.IsNullOrEmpty(endText)) {
            if (!long.TryParse(endText, out var parsedEnd) || parsedEnd < 0) {
                throw new FormatException("The Range header contains an invalid end offset.");
            }

            end = parsedEnd;
        }

        return new ObjectRange
        {
            Start = start,
            End = end
        };
    }

    private static bool TryGetCopySource(HttpRequest request, out CopySourceReference? copySource, out string? error)
    {
        var rawValue = request.Headers[CopySourceHeaderName].ToString();
        if (string.IsNullOrWhiteSpace(rawValue)) {
            copySource = null;
            error = null;
            return false;
        }

        try {
            copySource = ParseCopySource(rawValue);
            error = null;
            return true;
        }
        catch (FormatException exception) {
            copySource = null;
            error = exception.Message;
            return true;
        }
    }

    private static async Task<PreparedRequestBody> PrepareRequestBodyAsync(HttpRequest request, CancellationToken cancellationToken)
    {
        if (!IsAwsChunkedContent(request)) {
            return new PreparedRequestBody(request.Body, request.ContentLength, tempFilePath: null);
        }

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"integrateds3-aws-chunked-{Guid.NewGuid():N}.tmp");
        try {
            await using (var tempWriteStream = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan)) {
                await CopyAwsChunkedContentToAsync(request.Body, tempWriteStream, cancellationToken);
                await tempWriteStream.FlushAsync(cancellationToken);
            }

            var decodedLength = TryParseDecodedContentLength(request.Headers["x-amz-decoded-content-length"].ToString());
            var tempReadStream = new FileStream(tempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
            var contentLength = decodedLength ?? tempReadStream.Length;
            return new PreparedRequestBody(tempReadStream, contentLength, tempFilePath);
        }
        catch {
            if (File.Exists(tempFilePath)) {
                File.Delete(tempFilePath);
            }

            throw;
        }
    }

    private static bool IsAwsChunkedContent(HttpRequest request)
    {
        if (!request.Headers.TryGetValue(HeaderNames.ContentEncoding, out var contentEncodingValues)) {
            return false;
        }

        return contentEncodingValues
            .Where(static value => value is not null)
            .SelectMany(static value => value!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .Any(static value => string.Equals(value, "aws-chunked", StringComparison.OrdinalIgnoreCase));
    }

    private static long? TryParseDecodedContentLength(string? rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue)) {
            return null;
        }

        return long.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedValue)
            ? parsedValue
            : null;
    }

    private static async Task CopyAwsChunkedContentToAsync(Stream source, Stream destination, CancellationToken cancellationToken)
    {
        while (true) {
            var chunkHeader = await ReadLineAsync(source, cancellationToken)
                ?? throw new FormatException("The aws-chunked request body ended unexpectedly.");
            var separatorIndex = chunkHeader.IndexOf(';');
            var chunkLengthText = (separatorIndex >= 0 ? chunkHeader[..separatorIndex] : chunkHeader).Trim();
            if (!long.TryParse(chunkLengthText, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var chunkLength) || chunkLength < 0) {
                throw new FormatException("The aws-chunked request body contains an invalid chunk length.");
            }

            if (chunkLength == 0) {
                await ConsumeChunkTrailersAsync(source, cancellationToken);
                return;
            }

            await CopyExactBytesAsync(source, destination, chunkLength, cancellationToken);
            await ExpectCrLfAsync(source, cancellationToken);
        }
    }

    private static async Task<string?> ReadLineAsync(Stream source, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        while (true) {
            var nextByte = await ReadSingleByteAsync(source, cancellationToken);
            if (nextByte < 0) {
                return buffer.Length == 0 ? null : throw new FormatException("The aws-chunked request body contains an incomplete line.");
            }

            if (nextByte == '\r') {
                var lineFeed = await ReadSingleByteAsync(source, cancellationToken);
                if (lineFeed != '\n') {
                    throw new FormatException("The aws-chunked request body contains an invalid line terminator.");
                }

                return Encoding.ASCII.GetString(buffer.ToArray());
            }

            buffer.WriteByte((byte)nextByte);
        }
    }

    private static async Task ConsumeChunkTrailersAsync(Stream source, CancellationToken cancellationToken)
    {
        while (true) {
            var trailerLine = await ReadLineAsync(source, cancellationToken)
                ?? throw new FormatException("The aws-chunked request body ended before the terminating trailer section.");
            if (trailerLine.Length == 0) {
                return;
            }
        }
    }

    private static async Task CopyExactBytesAsync(Stream source, Stream destination, long byteCount, CancellationToken cancellationToken)
    {
        var remaining = byteCount;
        var buffer = new byte[81920];

        while (remaining > 0) {
            var read = await source.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, remaining)), cancellationToken);
            if (read == 0) {
                throw new FormatException("The aws-chunked request body ended unexpectedly while reading a chunk.");
            }

            await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            remaining -= read;
        }
    }

    private static async Task ExpectCrLfAsync(Stream source, CancellationToken cancellationToken)
    {
        if (await ReadSingleByteAsync(source, cancellationToken) != '\r'
            || await ReadSingleByteAsync(source, cancellationToken) != '\n') {
            throw new FormatException("The aws-chunked request body is missing the expected chunk terminator.");
        }
    }

    private static async Task<int> ReadSingleByteAsync(Stream source, CancellationToken cancellationToken)
    {
        var buffer = new byte[1];
        var read = await source.ReadAsync(buffer.AsMemory(0, 1), cancellationToken);
        return read == 0 ? -1 : buffer[0];
    }

    private static CopySourceReference ParseCopySource(string rawValue)
    {
        var trimmed = rawValue.Trim();
        if (trimmed.StartsWith('/')) {
            trimmed = trimmed[1..];
        }

        trimmed = Uri.UnescapeDataString(trimmed);
        var querySeparatorIndex = trimmed.IndexOf('?');
        var pathValue = querySeparatorIndex >= 0
            ? trimmed[..querySeparatorIndex]
            : trimmed;

        var separatorIndex = pathValue.IndexOf('/');
        if (separatorIndex <= 0 || separatorIndex == pathValue.Length - 1) {
            throw new FormatException("The copy source header must be in the form '/bucket/key'.");
        }

        string? versionId = null;
        if (querySeparatorIndex >= 0 && querySeparatorIndex < trimmed.Length - 1) {
            var query = QueryHelpers.ParseQuery(trimmed[querySeparatorIndex..]);
            if (query.TryGetValue(VersionIdQueryParameterName, out var versionIdValues)) {
                versionId = versionIdValues.ToString();
            }
        }

        return new CopySourceReference(pathValue[..separatorIndex], pathValue[(separatorIndex + 1)..], versionId);
    }

    private static string? ParseVersionId(HttpRequest request)
    {
        if (!request.Query.TryGetValue(VersionIdQueryParameterName, out var values)) {
            return null;
        }

        var versionId = values.ToString();
        return string.IsNullOrWhiteSpace(versionId)
            ? null
            : versionId;
    }

    private static bool TryParseRequestChecksums(
        HttpRequest request,
        bool requireChecksumValueForDeclaredAlgorithm,
        out string? checksumAlgorithm,
        out IReadOnlyDictionary<string, string>? checksums,
        out IResult? errorResult)
    {
        if (!TryGetRequestChecksumAlgorithm(request, out checksumAlgorithm, out errorResult)) {
            checksums = null;
            return false;
        }

        var parsedChecksums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var checksumSha256 = request.Headers[ChecksumSha256HeaderName].ToString();
        if (!string.IsNullOrWhiteSpace(checksumSha256)) {
            parsedChecksums["sha256"] = checksumSha256.Trim();
        }

        var checksumCrc32 = request.Headers[ChecksumCrc32HeaderName].ToString();
        if (!string.IsNullOrWhiteSpace(checksumCrc32)) {
            parsedChecksums["crc32"] = checksumCrc32.Trim();
        }

        if (requireChecksumValueForDeclaredAlgorithm
            && string.Equals(checksumAlgorithm, "sha256", StringComparison.OrdinalIgnoreCase)
            && !parsedChecksums.ContainsKey("sha256")) {
            checksums = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{ChecksumSha256HeaderName}' header is required when either '{SdkChecksumAlgorithmHeaderName}=SHA256' or '{ChecksumAlgorithmHeaderName}=SHA256' is supplied.",
                resource: null);
            return false;
        }

        if (requireChecksumValueForDeclaredAlgorithm
            && string.Equals(checksumAlgorithm, "crc32", StringComparison.OrdinalIgnoreCase)
            && !parsedChecksums.ContainsKey("crc32")) {
            checksums = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{ChecksumCrc32HeaderName}' header is required when either '{SdkChecksumAlgorithmHeaderName}=CRC32' or '{ChecksumAlgorithmHeaderName}=CRC32' is supplied.",
                resource: null);
            return false;
        }

        if (parsedChecksums.Count == 0) {
            checksums = null;
            errorResult = null;
            return true;
        }

        checksums = parsedChecksums;
        errorResult = null;
        return true;
    }

    private static bool TryGetRequestChecksumAlgorithm(HttpRequest request, out string? checksumAlgorithm, out IResult? errorResult)
    {
        var sdkChecksumAlgorithm = request.Headers[SdkChecksumAlgorithmHeaderName].ToString();
        var checksumAlgorithmHeader = request.Headers[ChecksumAlgorithmHeaderName].ToString();
        if (!string.IsNullOrWhiteSpace(sdkChecksumAlgorithm)
            && !string.IsNullOrWhiteSpace(checksumAlgorithmHeader)
            && !string.Equals(sdkChecksumAlgorithm, checksumAlgorithmHeader, StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = null;
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                $"The '{SdkChecksumAlgorithmHeaderName}' and '{ChecksumAlgorithmHeaderName}' headers must match when both are supplied.",
                resource: null);
            return false;
        }

        var rawChecksumAlgorithm = !string.IsNullOrWhiteSpace(checksumAlgorithmHeader)
            ? checksumAlgorithmHeader
            : sdkChecksumAlgorithm;

        if (!TryNormalizeChecksumAlgorithm(rawChecksumAlgorithm, out checksumAlgorithm)) {
            errorResult = ToErrorResult(
                request.HttpContext,
                StatusCodes.Status501NotImplemented,
                "NotImplemented",
                $"Checksum algorithm '{rawChecksumAlgorithm}' is not implemented.",
                resource: null);
            return false;
        }

        errorResult = null;
        return true;
    }

    private static bool TryNormalizeChecksumAlgorithm(string? rawValue, out string? checksumAlgorithm)
    {
        if (string.IsNullOrWhiteSpace(rawValue)) {
            checksumAlgorithm = null;
            return true;
        }

        if (string.Equals(rawValue, "SHA256", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = "sha256";
            return true;
        }

        if (string.Equals(rawValue, "CRC32", StringComparison.OrdinalIgnoreCase)) {
            checksumAlgorithm = "crc32";
            return true;
        }

        checksumAlgorithm = null;
        return false;
    }

    private static DateTimeOffset? ParseOptionalHttpDateHeader(string? rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue)) {
            return null;
        }

        if (!DateTimeOffset.TryParse(rawValue, out var parsedValue)) {
            throw new FormatException($"Invalid HTTP date header value '{rawValue}'.");
        }

        return parsedValue;
    }

    private static void ApplyDeleteObjectHeaders(HttpResponse httpResponse, DeleteObjectResult result)
    {
        ApplyVersionIdHeader(httpResponse, result.VersionId);

        if (result.IsDeleteMarker) {
            httpResponse.Headers["x-amz-delete-marker"] = "true";
        }
    }

    private static void ApplyObjectTaggingHeaders(HttpResponse httpResponse, ObjectTagSet tagSet)
    {
        ApplyVersionIdHeader(httpResponse, tagSet.VersionId);
    }

    private static void ApplyObjectHeaders(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        httpResponse.Headers.LastModified = objectInfo.LastModifiedUtc.ToString("R");
        ApplyObjectIdentityHeaders(httpResponse, objectInfo);

        IEnumerable<KeyValuePair<string, string>> metadataPairs = objectInfo.Metadata ?? Enumerable.Empty<KeyValuePair<string, string>>();
        foreach (var metadataPair in metadataPairs) {
            httpResponse.Headers[$"{MetadataHeaderPrefix}{metadataPair.Key}"] = metadataPair.Value;
        }
    }

    private static void ApplyObjectIdentityHeaders(HttpResponse httpResponse, ObjectInfo objectInfo)
    {
        if (!string.IsNullOrWhiteSpace(objectInfo.ETag)) {
            httpResponse.Headers.ETag = QuoteETag(objectInfo.ETag);
        }

        ApplyVersionIdHeader(httpResponse, objectInfo.VersionId);

        ApplyChecksumHeaders(httpResponse, objectInfo.Checksums);
    }

    private static void ApplyVersionIdHeader(HttpResponse httpResponse, string? versionId)
    {
        if (!string.IsNullOrWhiteSpace(versionId)) {
            httpResponse.Headers[VersionIdHeaderName] = versionId;
        }
    }

    private static void ApplyChecksumHeaders(HttpResponse httpResponse, IReadOnlyDictionary<string, string>? checksums)
    {
        var checksumCrc32 = GetChecksumValue(checksums, "crc32");
        if (!string.IsNullOrWhiteSpace(checksumCrc32)) {
            httpResponse.Headers[ChecksumCrc32HeaderName] = checksumCrc32;
        }

        var checksumSha256 = GetChecksumValue(checksums, "sha256");
        if (!string.IsNullOrWhiteSpace(checksumSha256)) {
            httpResponse.Headers[ChecksumSha256HeaderName] = checksumSha256;
        }

        var checksumType = GetChecksumType(checksums);
        if (!string.IsNullOrWhiteSpace(checksumType)) {
            httpResponse.Headers[ChecksumTypeHeaderName] = checksumType;
        }
    }

    private static void ApplyChecksumAlgorithmHeader(HttpResponse httpResponse, string? checksumAlgorithm)
    {
        var responseValue = ToS3ChecksumAlgorithmValue(checksumAlgorithm);
        if (!string.IsNullOrWhiteSpace(responseValue)) {
            httpResponse.Headers[ChecksumAlgorithmHeaderName] = responseValue;
        }
    }

    private static string? GetChecksumValue(IReadOnlyDictionary<string, string>? checksums, string algorithm)
    {
        if (checksums is null || string.IsNullOrWhiteSpace(algorithm)) {
            return null;
        }

        if (checksums.TryGetValue(algorithm, out var directValue) && !string.IsNullOrWhiteSpace(directValue)) {
            return directValue;
        }

        foreach (var checksum in checksums) {
            if (string.Equals(checksum.Key, algorithm, StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrWhiteSpace(checksum.Value)) {
                return checksum.Value;
            }
        }

        return null;
    }

    private static string? GetChecksumType(IReadOnlyDictionary<string, string>? checksums)
    {
        if (checksums is null) {
            return null;
        }

        foreach (var checksum in checksums.Values) {
            if (IsCompositeChecksumValue(checksum)) {
                return "COMPOSITE";
            }
        }

        return null;
    }

    private static bool IsCompositeChecksumValue(string? checksum)
    {
        if (string.IsNullOrWhiteSpace(checksum)) {
            return false;
        }

        var separatorIndex = checksum.LastIndexOf('-');
        if (separatorIndex <= 0 || separatorIndex >= checksum.Length - 1) {
            return false;
        }

        return int.TryParse(checksum[(separatorIndex + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out var partCount)
            && partCount > 0;
    }

    private static string? ToS3ChecksumAlgorithmValue(string? checksumAlgorithm)
    {
        return checksumAlgorithm switch
        {
            "sha256" => "SHA256",
            "crc32" => "CRC32",
            _ => null
        };
    }

    private static bool MatchesIfMatch(string? rawHeader, string? currentETag)
    {
        if (string.IsNullOrWhiteSpace(rawHeader)) {
            return true;
        }

        if (rawHeader.Trim() == "*") {
            return true;
        }

        return MatchesAnyETag(rawHeader, currentETag);
    }

    private static bool MatchesAnyETag(string? rawHeader, string? currentETag)
    {
        if (string.IsNullOrWhiteSpace(rawHeader) || string.IsNullOrWhiteSpace(currentETag)) {
            return false;
        }

        var normalizedCurrent = NormalizeETag(currentETag);
        foreach (var candidate in rawHeader.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            if (candidate == "*" || NormalizeETag(candidate) == normalizedCurrent) {
                return true;
            }
        }

        return false;
    }

    private static bool IsVersionAfterMarker(ObjectInfo version, string? keyMarker, string? versionIdMarker)
    {
        if (string.IsNullOrWhiteSpace(keyMarker)) {
            return true;
        }

        var keyComparison = StringComparer.Ordinal.Compare(version.Key, keyMarker);
        if (keyComparison > 0) {
            return true;
        }

        if (keyComparison < 0) {
            return false;
        }

        return !string.IsNullOrWhiteSpace(versionIdMarker)
               && StringComparer.Ordinal.Compare(version.VersionId, versionIdMarker) < 0;
    }

    private static string NormalizeETag(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.StartsWith("W/", StringComparison.OrdinalIgnoreCase)) {
            trimmed = trimmed[2..].Trim();
        }

        if (trimmed.Length >= 2 && trimmed.StartsWith('"') && trimmed.EndsWith('"')) {
            trimmed = trimmed[1..^1];
        }

        return trimmed;
    }

    private static bool WasModifiedAfter(DateTimeOffset lastModifiedUtc, DateTimeOffset comparisonUtc)
    {
        return TruncateToWholeSeconds(lastModifiedUtc) > TruncateToWholeSeconds(comparisonUtc);
    }

    private static DateTimeOffset TruncateToWholeSeconds(DateTimeOffset value)
    {
        var utcValue = value.ToUniversalTime();
        return utcValue.AddTicks(-(utcValue.Ticks % TimeSpan.TicksPerSecond));
    }

    private sealed class StreamObjectResult(GetObjectResponse objectResponse) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);

            await using var response = objectResponse;

            ApplyObjectHeaders(httpContext.Response, response.Object);
            httpContext.Response.Headers.AcceptRanges = "bytes";

            if (response.IsNotModified) {
                httpContext.Response.StatusCode = StatusCodes.Status304NotModified;
                return;
            }

            httpContext.Response.ContentType = response.Object.ContentType ?? "application/octet-stream";
            httpContext.Response.ContentLength = response.Object.ContentLength;

            if (response.Range is not null) {
                httpContext.Response.StatusCode = StatusCodes.Status206PartialContent;
                httpContext.Response.Headers.ContentRange = $"bytes {response.Range.Start}-{response.Range.End}/{response.TotalContentLength}";
            }
            else {
                httpContext.Response.StatusCode = StatusCodes.Status200OK;
            }

            await response.Content.CopyToAsync(httpContext.Response.Body, httpContext.RequestAborted);
        }
    }

    private sealed class EndpointStorageAuthorizationException(StorageError error) : Exception(error.Message)
    {
        public StorageError Error { get; } = error;
    }

    private sealed class XmlContentResult(string content, int statusCode, string contentType) : IResult
    {
        public async Task ExecuteAsync(HttpContext httpContext)
        {
            ArgumentNullException.ThrowIfNull(httpContext);

            httpContext.Response.StatusCode = statusCode;
            httpContext.Response.ContentType = contentType;
            await httpContext.Response.WriteAsync(content, httpContext.RequestAborted);
        }
    }

    private sealed class PreparedRequestBody(Stream content, long? contentLength, string? tempFilePath) : IAsyncDisposable
    {
        public Stream Content { get; } = content;

        public long? ContentLength { get; } = contentLength;

        public async ValueTask DisposeAsync()
        {
            if (tempFilePath is null) {
                return;
            }

            await Content.DisposeAsync();
            if (File.Exists(tempFilePath)) {
                File.Delete(tempFilePath);
            }
        }
    }

    private enum S3AddressingStyle
    {
        Path,
        VirtualHosted
    }

    private sealed record ResolvedS3Request(
        string BucketName,
        string? Key,
        S3AddressingStyle AddressingStyle,
        string CanonicalResourcePath,
        string CanonicalPath);

    private sealed record CopySourceReference(string BucketName, string Key, string? VersionId);

    private sealed record ListBucketResultEntry(ObjectInfo? Object, string? CommonPrefix, string ContinuationToken)
    {
        public static ListBucketResultEntry ForObject(ObjectInfo @object)
        {
            ArgumentNullException.ThrowIfNull(@object);
            return new ListBucketResultEntry(@object, null, @object.Key);
        }

        public static ListBucketResultEntry ForCommonPrefix(string commonPrefix, string continuationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(commonPrefix);
            ArgumentException.ThrowIfNullOrWhiteSpace(continuationToken);
            return new ListBucketResultEntry(null, commonPrefix, continuationToken);
        }
    }

    private sealed record ListObjectVersionsResultEntry(ObjectInfo? Version, string? CommonPrefix, string NextKeyMarker, string? NextVersionIdMarker)
    {
        public static ListObjectVersionsResultEntry ForVersion(ObjectInfo version)
        {
            ArgumentNullException.ThrowIfNull(version);
            return new ListObjectVersionsResultEntry(version, null, version.Key, version.VersionId);
        }

        public static ListObjectVersionsResultEntry ForCommonPrefix(string commonPrefix, string nextKeyMarker, string? nextVersionIdMarker)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(commonPrefix);
            ArgumentException.ThrowIfNullOrWhiteSpace(nextKeyMarker);
            return new ListObjectVersionsResultEntry(null, commonPrefix, nextKeyMarker, nextVersionIdMarker);
        }
    }
}
