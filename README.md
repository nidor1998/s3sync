# s3sync

![CI](https://github.com/nidor1998/s3sync/workflows/CI/badge.svg) [![codecov](https://codecov.io/gh/nidor1998/s3sync/branch/main/graph/badge.svg?token=GO3DGS2BR4)](https://codecov.io/gh/nidor1998/s3sync)
[![DeepSource](https://app.deepsource.com/gh/nidor1998/s3sync.svg/?label=active+issues&show_trend=true&token=Q3EjeUmx8Fu-ndXKEG133W-t)](https://app.deepsource.com/gh/nidor1998/s3sync/?ref=repository-badge)

## Overview
s3sync is a reliable, very fast, and powerful synchronization tool for S3.  
It can be used to synchronize local directories with S3 bucket, and also to synchronize S3 to s3 bucket.
Supports multipart upload, versioning, metadata.

**[AWS SDK for rust](https://github.com/awslabs/aws-sdk-rust) is not production ready yet and is not recommended for production use, so s3sync is also not recommended for production use. But any feedback is welcome. I will continue to improve s3sync.**


## As a library
s3sync can be used as a library.

```rust
use s3sync::config::args::parse_from_args;
use s3sync::config::Config;
use s3sync::pipeline::Pipeline;
use s3sync::types::token::create_pipeline_cancellation_token;

#[tokio::main]
async fn main() {
    // You can use all the arguments for s3sync binary here.
    let args = vec!["program_name", "./src", "s3://test-bucket/src/"];

    // s3sync library converts the arguments to Config.
    let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

    // Create a cancellation token for the pipeline.
    // You can use this token to cancel the pipeline.
    let cancellation_token = create_pipeline_cancellation_token();
    let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

    // You can close statistics sender to stop statistics collection, if needed.
    // Statistics collection consumes some Memory, so it is recommended to close it if you don't need it.
    pipeline.close_stats_sender();

    // Run the pipeline. In this simple example, we run the pipeline synchronously.
    pipeline.run().await;

    assert!(!pipeline.has_error());
}
```

## Features
- Reliable: In-depth end-to-end object integrity check  
s3sync calculates ETag(MD5 or equivalent) for each object and compares them with the ETag in the target.  
An object that exists in the local disk is read from the disk and compared with the checksum in the source or target.    
Even if the source object was uploaded with multipart upload, s3sync can calculate and compare ETag for each part and the entire object.(with `--auto-chunksize`)  
Optionally, s3sync can also calculate and compare additional checksum(SHA256/SHA1/CRC32/CRC32C) for each object.
- Very fast  
s3sync implemented in Rust, using AWS SDK for Rust that uses multithreaded asynchronous I/O.  
In my environment(`c6a.large`, with 256 workers), Local to S3, about 4,000 objects/sec (small objects 1-20 kb).  
s3sync is optimized for synchronizing large amounts(over millions) of objects.
Not optimized for transferring small amounts of objects(less than worker-size: default 16) of large size.(Of course, it can be used for this purpose.)  

  Local to S3, `c6a.large(2vCPU, 4GB)` 100,000 objects(1-20 kb objects), 1.00GiB, 40.76MiB/sec, 25 seconds, and all objects are end-to-end integrity verified(MD5, SHA256).
  ```
  [ec2-user@ip-foo ~]$ time ./s3sync --worker-size 256 --additional-checksum-algorithm SHA256 ~/testdata s3://5786c9fb-e2a7-407c-814a-84ed9590e35c/testdata/
  1.00 GiB | 40.76 MiB/sec,  transferred 100000 objects | 3,980 objects/sec,  etag verified 100000 objects,  checksum verified 100000 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 25 seconds

  real    0m25.142s
  user    0m28.928s
  sys     0m19.969s
  ```
  S3 to Local, `c6a.large(2vCPU, 4GB)` 100,000 objects(1-20 kb objects), 1.00GiB, 24.70 MiB/sec, 41 seconds, and all objects are end-to-end integrity verified(MD5, SHA256).
  ```
  [ec2-user@ip-foo ~]$ time ./s3sync --worker-size 256 --enable-additional-checksum  s3://5786c9fb-e2a7-407c-814a-84ed9590e35c/ .
  1.00 GiB | 24.70 MiB/sec,  transferred 100000 objects | 2,412 objects/sec,  etag verified 100000 objects,  checksum verified 100000 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 41 seconds
  
  real    0m41.471s
  user    0m47.906s
  sys     0m25.662s
  ```
  Local to S3, `c6a.large(2vCPU, 4GB)` 16 objects(6GiB objects), 96.00 GiB, 208.92 MiB/sec, 8 minutes, and all objects are end-to-end integrity verified(MD5, SHA256).
  ```
  [ec2-user@ip-foo ~]$ time ./s3sync --additional-checksum-algorithm SHA256 ~/testdata s3://b2f99466-6665-4b8e-8327-a7bdabff8a10/testdata/
  96.00 GiB | 208.92 MiB/sec,  transferred  16 objects | 0 objects/sec,  etag verified 16 objects,  checksum verified 16 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 8 minutes
    
  real    7m50.571s
  user    10m40.117s
  sys     4m28.771s
  ```
  S3 to Local, `c6a.large(2vCPU, 4GB)` 16 objects(6GiB objects), 96.00 GiB , 41.67 MiB/sec, 39 minutes, and all objects are end-to-end integrity verified(MD5, SHA256).  
  ETag/additional checksum verification is costly in the case of S3 to Local. Because s3sync needs to read the entire downloaded object from local disk to calculate ETag/checksum.   
  You can disable it with `--disable-etag-verify`.  
  ```
  [ec2-user@ip-foo ~]$ time ./s3sync --enable-additional-checksum s3://b2f99466-6665-4b8e-8327-a7bdabff8a10 .
  96.00 GiB | 41.67 MiB/sec,  transferred  16 objects | 0 objects/sec,  etag verified 16 objects,  checksum verified 16 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 39 minutes
    
  real    39m19.267s
  user    11m3.478s
  sys     8m35.242s
  ```
  S3 to Local, `c6a.large(2vCPU, 4GB)` 16 objects(6GiB objects), 96.00 GiB , 125.67 MiB/sec, 13 minutes, **without end-to-end integrity verification**.  
  It might be useful when you want to transfer large data fast. 
  ```
  [ec2-user@ip-foo ~]$ time ./s3sync --disable-etag-verify s3://b2f99466-6665-4b8e-8327-a7bdabff8a10 .
  96.00 GiB | 125.67 MiB/sec,  transferred  16 objects | 0 objects/sec,  etag verified 0 objects,  checksum verified 0 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 13 minutes
  
  real    13m2.278s
  user    3m54.569s
  sys     9m40.960s  
  ```
- Multiple ways
  - Local to S3
  - S3 to Local
  - S3 to S3  
- Any object size support  
s3sync can handle any object size. From 0 byte to 5TiB.
- Low memory usage  
Memory usage is low and does not depend on the object size or number of objects.
It mainly depends on the number of workers and multipart chunk size.  
The default setting uses only about 500MB of maximum memory for any object size or number of objects.  
- Incremental transfer  
Transfer only modified objects. If the object is not modified, it is skipped. 
Incremental transfer can be resumed from the last checkpoint.
Checking of modified objects is very fast.  
- Versioning support  
All versions of the object can be synchronized.(Except intermediate delete markers)  
- Tagging support  
All tags of the object can be synchronized.
- Metadata support  
All metadata of the object can be synchronized. For example, `Content-Type`, `Content-Encoding`, `Cache-Control`, user-defined metadata, etc.
- Flexible filtering  
Regular expression, `ContentLength`, `LastModified`.
- Rate limiting by objects, bandwidth  
For example, you can limit the number of objects transferred per second to 1,000, and the bandwidth to 100MB/sec.
- `--delete` support  
Objects that exist in the target but not in the source are deleted during synchronization.  
Since this can cause data loss, test first with the `--dry-run` option.
- S3-compatible storage support  
s3sync uses the AWS SDK for Rust. s3sync can be used with any S3-compatible storage.  
Originally, it was developed for S3-Compatible to S3-Compatible.

- Multi-platform support  
All features are supported on supported platforms.

## Requirements
- 64-bit Linux (kernel 3.2 or later, glibc 2.17 or later)
- 64-bit Windows 10 or later  
- MacOS 11.0 or later

## Licence
This project is licensed under the Apache-2.0 License.

## Installation
Download the latest binary from [Releases](https://github.com/nidor1998/s3sync/releases)  
The binary runs on the above platforms without any dependencies.

You can also build from source following the instructions below.
### Install Rust
See [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)  
s3sync requires Rust 1.70 or later.

### Build
```bash
cargo build --release
```
Output binary is in `target/release/s3sync`

## Usage
AWS credentials are required to use s3sync. IAM Roles, AWC CLI Profile, environment variables, etc supported.  
By default s3sync access credentials from many locations(IAM Roles, environment variables, etc).  
For more information, see [Specifying your credentials and default region](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html)  

You can also specify credentials in the command line options or environment variables.

Region is required. It can be specified in the profile, environment, or command line options.  

Prefix is optional. If not specified, the entire bucket will be synchronized.  

If you specify a prefix, **s3sync doesn't automatically add trailing slash.**  
For example, if you specify `s3://bucket-name/prefix` as the target, s3sync will synchronize source objects named `foo` to `s3://bucket-name/prefixfoo`.  
And if you specify `s3://bucket-name/prefix/` as the target, s3sync will synchronize source objects named `foo` to `s3://bucket-name/prefix/foo`.  

If you use s3sync for the first time, you should use the `--dry-run` option to test the operation.

For all options, see `s3sync --help`

### Local to S3
```bash
s3sync /path/to/local s3://bucket-name/prefix
```

### S3 to Local
```bash
s3sync s3://bucket-name/prefix /path/to/local
```

### S3 to S3
```bash
s3sync s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Access key and secret access key directly
```bash
s3sync --source-access-key foo --source-secret-access-key bar s3://bucket-name/prefix /path/to/local
```

### AWS CLI profile support
```bash
s3sync --source-profile foo --target-profile bar s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Specify region
```bash
s3sync --source-profile foo --source-region ap-northeast-1 s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Versioning
```bash
s3sync --enable-versioning s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Custom endpoint
You can specify a S3-compatible storage endpoint.  
Warning: If you use a custom endpoint, you may need to specify `--source-force-path-style` or `--target-force-path-style`.
```bash
s3sync --target-endpoint-url https://foo --target-force-path-style /path/to/local s3://bucket-name/prefix 
```

### Dry run
```bash
s3sync --dry-run s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### --delete
```bash
s3sync --delete s3://bucket-name1/prefix s3://bucket-name2/prefix
```

## About end-to-end object integrity check
s3sync calculates ETag(MD5) checksums for source object and compares them with the checksums in the target.  
Optionally, s3sync can also calculate and compare additional checksum(SHA256/SHA1/CRC32/CRC32C) for each object.

ETag is not always MD5. If the object is uploaded with multipart upload, ETag is not MD5 digest but the MD5 digest of these concatenated values.  
Even if multipart upload is used, s3sync can calculate and compare ETag for each part and the entire object.(with `--auto-chunksize`)

If the object is uploaded with SSE-KMS/SSE-C, ETag is not MD5. In this case, s3sync cannot calculate and compare checksums for the object.

s3sync always uses the following elements to verify the integrity of the object.
- `Content-MD5` header(End-to-end API level integrity check)
Amazon S3 recommends using `Content-MD5` header for end-to-end integrity check.
- `x-amz-content-sha256` Authentication header  
This header is SHA256 digest of the request payload.    
Note: Some S3-compatible storage ignores this header.
- Additional checksum algorithm(Optional)  
Even if the object is uploaded with SSE-KMS/SSE-C, s3sync can calculate and compare additional checksum algorithm.  
Note: As of writing this document, almost no S3-compatible storage supports additional checksum algorithm.
- TLS(If not explicitly disabled)

The multipart ETag does not always match that of the source object. But with above elements, it is almost safe to assume that the object is not corrupted.  
With `--auto-chunksize`, s3sync can calculate and compare ETag/checksum for each part and the entire object.   
If you want strict integrity check, use `--auto-chunksize`, but it will need more API calls and time.

For more information, see https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html

### Matrix of s3sync end-to-end object integrity check
In the case of Local to S3 or S3 to Local, s3sync compares ETags and additional checksums of Local that s3sync calculates with those of S3.  
In the case of S3 to S3, s3sync just compares ETags and additional checksums that calculated by S3.

If an object is not verified, s3sync show a warning message in the terminal.

#### ETag(MD5 digest or equivalent): plain-text/SSE-S3
|                  | Local to S3   | S3 to Local                                                                                        | S3 to S3                                                                                           |
|------------------|---------------|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| multipart object | always verify | verify as much as possible(without `--auto-chunksize`)<br/> always verify(with `--auto-chunksize`) | verify as much as possible(without `--auto-chunksize`)<br/> always verify(with `--auto-chunksize`) |
| single object    | always verify | always verify                                                                                      | always verify                                                                                      |

#### ETag(MD5 digest or equivalent): SSE-KMS/SSE-C
|                  | Local to S3 | S3 to Local | S3 to S3   |
|------------------|-------------|-------------|------------|
| multipart object | not verify  | not verify  | not verify |
| single object    | not verify  | not verify  | not verify |

#### Additional checksum: plain-text/SSE-S3/SSE-KMS/SSE-C
|                  | Local to S3   | S3 to Local   | S3 to S3                                                                                            |
|------------------|---------------|---------------|-----------------------------------------------------------------------------------------------------|
| multipart object | always verify | always verify | verify as much as possible (without `--auto-chunksize`)<br/> always verify(with `--auto-chunksize`) |
| single object    | always verify | always verify | always verify                                                                                       |
  
Note: To enable end-to-end additional checksum verification, `--enable-additional-checksum` must be specified.  
Note: In the case of S3 to S3, same checksum algorithm must be used for both source and target.  

### About `--auto-chunksize`
If `--auto-chunksize` is specified, s3sync automatically calculates the correct chunk size for multipart upload.  
This is done by `HeadObject` API with `partNumber` parameter.
`--auto-chunksize` requires extra API calls(1 API call per part).  
Remember that not all S3-compatible storage supports `HeadObject` API with `partNumber` parameter.  
If S3-compatible storage does not support it, s3sync will show a warning message in the terminal.

See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax

**Warning: In case of S3 to S3, if the source object is uploaded with a large chunk size, s3sync will consume a lot of memory.**

### Incremental transfer
s3sync transfers only modified objects.It checks `LastModified` timestamp.  

If `--check-size` is specified, s3sync only checks the size of the object.

At first, incremental transfer lists all objects in the target.  
Then, s3sync compares the objects and transfers only modified objects.

Therefore, incremental transfer takes a time to start for large amounts of existing target objects.  
In my environment, Amazon S3 takes about 1 second per 10,000 objects to list objects in the target.(It depends on remote storage.)

If there are few objects in the target, incremental transfer starts immediately, regardless of the number of source objects.

### Versioning support
s3sync uses user-defined metadata to check the version of the object.  
If `--enable-versioning` is specified, s3sync adds user-defined metadata to the object.
If you transfer to existing bucket, because of the lack of user-defined metadata, s3sync will transfer all versions of the object.  
Generally, you should use `--enable-versioning` when you transfer to a new bucket.

Intermediate delete markers are not synchronized. Latest version delete markers are synchronized.

user-defined metadata: `s3sync_origin_version_id`, `s3sync_origin_last_modified`

### Metadata support
The following metadata of the S3 object is synchronized.
- Content-Type
- Content-Encoding
- Cache-Control
- Content-Disposition
- Content-Language
- Expires
- User-defined metadata

### SSE support
The following SSE is supported.
- SSE-S3
- SSE-KMS
- SSE-C

### Memory usage
s3sync consumes memory for each worker.   
For single object, approximately `average size of the object * worker-size(default 16) * 2`.  
For multipart object, approximately `multipart chunksize(default 8MiB) * worker-size(default 16) * 2`.

Because s3sync uses incremental transfer, it lists all objects in the target bucket and stores the result in memory.  
Therefore, if there are a large number of objects in the target bucket, s3sync can consume a lot of memory.  
If you do not use the `--delete` option, s3sync will consume about 100MB per 1,000,000 target objects.  
If you use the `--delete` option, s3sync will consume about 250MB per 1,000,000 target objects.   

To reduce memory usage, you can divide the target objects by prefix and run s3sync multiple times or specify `--remove-modified-filter` and `--head-each-target` options.  
You can also divide the target objects by filter and run s3sync multiple times.

The `--head-each-target` option calls the `HeadObject` API for each source object, but it is a trade-off between memory usage and API calls.

### S3 Permissions
s3sync requires the following permissions.

#### Source bucket
```
"Action": [
    "s3:GetBucketVersioning",
    "s3:GetObject",
    "s3:GetObjectAttributes",
    "s3:GetObjectTagging",
    "s3:GetObjectVersion",
    "s3:GetObjectVersionAttributes",
    "s3:GetObjectVersionTagging",
    "s3:ListBucket",
    "s3:ListBucketVersions",
]
```

#### Target bucket
```
"Action": [
    "s3:AbortMultipartUpload",
    "s3:DeleteObject",
    "s3:DeleteObjectTagging",
    "s3:GetBucketVersioning",
    "s3:GetObject",
    "s3:GetObjectTagging",
    "s3:GetObjectVersion",
    "s3:ListBucket",
    "s3:ListBucketVersions",
    "s3:PutObject",
    "s3:PutObjectTagging"
]
```

### Advanced options

#### `--worker-size`
The number of workers. Many workers can improve performance. But it also increases CPU and memory usage.  
Default: 16

If you specify many workers, you may need to increase the number of open files.  
For example, on Linux: `ulimit -n 8192`

#### `--force-retry-count`
s3sync forcibly retries the operation that AWS SDK for Rust cannot retry.  
For example, in the case of `connection reset by peer`, s3sync will retry the operation.

#### `--remove-modified-filter`
If you want to overwrite the existing object, specify the option.

#### `--put-last-modified-metadata`
This option adds user-defined metadata that contains the last-modified time of the source object.

user-defined metadata: `s3sync_origin_last_modified`

#### `--additional-checksum-algorithm`
If you want to use additional checksum for upload, specify the algorithm.

#### `--enable-additional-checksum`
If you want to use additional checksums for download, specify the option.

Warning: Even if the object was uploaded with additional checksum, without this option, s3sync does not verify additional checksum.

#### `--https-proxy`
You can specify the proxy server for https.

#### `--disable-multipart-verify`
When object is uploaded with multipart upload, its ETag may not match that of the target object.  
This can occur when the chunk size that the object was uploaded with is different. If you don't know the correct chunk size, you can disable the verification with this option.  
You can specify the chunk size with `--multipart-threshold` and `--multipart-chunksize` (Default: 8MiB).  

If extra API calls are allowed, you can use `--auto-chunksize` instead.  
However, please note that not all S3-compatible storage supports this option.   
**Warning: In case of S3 to S3, if the source object is uploaded with a large chunk size, s3sync will consume a lot of memory.**

#### `-v`
s3sync uses [tracing-subscriber](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/) for tracing.    
More occurrences increase the verbosity.  
For example, `-v`: show `info`, `-vv`: show `debug`, `-vvv`: show `trace`  
By default, s3sync shows warning and error messages.

`info` and `debug` messages are useful for troubleshooting. `trace` messages are useful for debugging.

Instead of `-v`, you can use `RUST_LOG` environment variable.

#### `--aws-sdk-tracing`
For troubleshooting, s3sync can output the AWS SDK for Rust's tracing information.  
Instead of `--aws-sdk-tracing`, you can use `RUST_LOG` environment variable.

#### `--filter-include-regex`, `--filter-exclude-regex`  
You can specify the regular expression to filter the source objects.  
The regular expression syntax is the same as [regex](https://docs.rs/regex/latest/regex/#syntax).

#### `--auto-complete-shell`
You can output the shell script to complete the command.

```bash
s3sync --auto-complete-shell bash
```

#### `-h/--help`
For more information, see `s3sync -h`.