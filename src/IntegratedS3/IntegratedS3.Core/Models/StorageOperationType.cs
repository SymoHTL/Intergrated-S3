namespace IntegratedS3.Core.Models;

public enum StorageOperationType
{
    ListBuckets,
    CreateBucket,
    GetBucketVersioning,
    PutBucketVersioning,
    GetBucketCors,
    PutBucketCors,
    DeleteBucketCors,
    HeadBucket,
    DeleteBucket,
    ListObjects,
    GetObject,
    PresignGetObject,
    GetObjectTags,
    GetObjectRetention,
    GetObjectLegalHold,
    CopyObject,
    PutObject,
    PresignPutObject,
    PutObjectTags,
    PutObjectRetention,
    PutObjectLegalHold,
    DeleteObjectTags,
    InitiateMultipartUpload,
    UploadMultipartPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    HeadObject,
    DeleteObject
}
