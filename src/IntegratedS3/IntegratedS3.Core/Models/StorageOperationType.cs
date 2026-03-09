namespace IntegratedS3.Core.Models;

public enum StorageOperationType
{
    ListBuckets,
    CreateBucket,
    GetBucketVersioning,
    PutBucketVersioning,
    HeadBucket,
    DeleteBucket,
    ListObjects,
    GetObject,
    GetObjectTags,
    CopyObject,
    PutObject,
    PutObjectTags,
    DeleteObjectTags,
    InitiateMultipartUpload,
    UploadMultipartPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    HeadObject,
    DeleteObject
}
