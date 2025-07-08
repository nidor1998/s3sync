# s3sync

[![Crates.io](https://img.shields.io/crates/v/s3sync.svg)](https://crates.io/crates/s3sync)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![MSRV](https://img.shields.io/badge/msrv-1.85.0-red)
![CI](https://github.com/nidor1998/s3sync/workflows/CI/badge.svg) [![codecov](https://codecov.io/gh/nidor1998/s3sync/branch/main/graph/badge.svg?token=GO3DGS2BR4)](https://codecov.io/gh/nidor1998/s3sync)
[![DeepSource](https://app.deepsource.com/gh/nidor1998/s3sync.svg/?label=active+issues&show_trend=true&token=Q3EjeUmx8Fu-ndXKEG133W-t)](https://app.deepsource.com/gh/nidor1998/s3sync/?ref=repository-badge)

## Overview
s3sync is a reliable, very fast, and powerful synchronization tool for S3.  
It can be used to synchronize local directories with S3 bucket, and also to synchronize S3 to s3 bucket.
Supports multipart upload, versioning, metadata.  

This tool is designed solely for object storage data synchronization.


## As a library
s3sync can be used as a library.  
s3sync CLI is a very thin wrapper of the s3sync library. You can use all features of s3sync CLI in the library.  

See [docs.rs](https://docs.rs/s3sync/latest/s3sync/) for more information.


## Features
- Reliable: In-depth end-to-end object integrity check  
  s3sync calculates ETag(MD5 or equivalent) for each object and compares them with the ETag in the target.  
  An object that exists in the local disk is read from the disk and compared with the checksum in the source or target.    
  Even if the source object was uploaded with multipart upload, s3sync can calculate and compare ETag for each part and the entire object.(with `--auto-chunksize`)  
  Optionally, s3sync can also calculate and compare additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME) for each object.  
  Note: Amazon S3 Express One Zone does not support ETag as verification. But you can use additional checksum algorithm.

- Multiple ways
  - Local to S3(s3-compatible storage)
  - S3(s3-compatible storage) to Local
  - S3 to S3(cross-region, same-region, same-account, cross-account, from-to s3/s3-compatible storage)

- Very fast  
  s3sync implemented in Rust, using AWS SDK for Rust that uses multithreaded asynchronous I/O.  
  In my environment(`c7a.large`, with 256 workers), Local to S3, about 3,900 objects/sec (small objects 10KiB).
  The following is the benchmark result of `c7a.large(2vCPU, 4GB)/Amazon Linux 2023 AMI` instance on AWS on `ap-northeast-1`. And no special optimization is applied to the instance and network topology to s3 and anything else.
  You can reproduce the benchmark with the following commands.

  Note: The default s3sync setting uses `--worker-size 16` and `--max-parallel-uploads 16`. This is a moderate setting for most cases. If you want to improve performance, you can increase `--worker-size` and `--max-parallel-uploads`. But it will increase CPU and memory usage.


  Local to S3, `c7a.large(2vCPU, 4GB)` 100,000 objects(10KiB objects), 976.56 MiB | 38.88 MiB/sec, 25 seconds, and all objects are end-to-end integrity verified(MD5, SHA256).
  ```
  [ec2-user@aws-c7a-large s3sync]$ time s3sync --worker-size 256 --additional-checksum-algorithm SHA256 ~/testdata s3://c1b01a9a-5cea-4650-b3d6-16ac37aad03a/testdata/
  976.56 MiB | 38.88 MiB/sec,  transferred 100000 objects | 3,981 objects/sec,  etag verified 100000 objects,  checksum verified 100000 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 25 seconds
  
  real	0m25.135s
  user	0m29.255s
  sys	0m19.745s
  ```
  S3 to Local, `c7a.large(2vCPU, 4GB)` 100,000 objects(10KiB objects), 976.56 MiB | 25.97 MiB/sec, 38 seconds, and all objects are end-to-end integrity verified(MD5, SHA256).
  ```
  [ec2-user@aws-c7a-large s3sync]$ time s3sync --worker-size 256 --enable-additional-checksum  s3://c1b01a9a-5cea-4650-b3d6-16ac37aad03a/ ./download/
  976.56 MiB | 25.97 MiB/sec,  transferred 100000 objects | 2,659 objects/sec,  etag verified 100000 objects,  checksum verified 100000 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 38 seconds
  
  real	0m37.626s
  user	0m44.012s
  sys	0m27.000s
  ```
  Local to S3, `c7a.large(2vCPU, 4GB)` 16 objects(6GiB objects), 96.00 GiB | 287.91 MiB/sec, 5.41 minutes, and all objects are end-to-end integrity verified(MD5, SHA256).
  ```
  [ec2-user@aws-c7a-large s3sync]$ time s3sync --max-parallel-uploads 64 --additional-checksum-algorithm SHA256 ~/testdata s3://c1b01a9a-5cea-4650-b3d6-16ac37aad03a/testdata/
  96.00 GiB | 287.91 MiB/sec,  transferred  16 objects | 0 objects/sec,  etag verified 16 objects,  checksum verified 16 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 6 minutes
  
  real	5m41.643s
  user	9m36.389s
  sys	1m37.730s
  ```
  S3 to Local, `c7a.large(2vCPU, 4GB)` 16 objects(6GiB objects), 96.00 GiB | 41.67 MiB/sec, 39 minutes, and all objects are end-to-end integrity verified(MD5, SHA256).  
  ETag/additional checksum verification is costly in the case of S3 to Local. Because s3sync needs to read the entire downloaded object from local disk to calculate ETag/checksum.   
  You can disable it with `--disable-etag-verify` and remove `--enable-additional-checksum`. Without all verifications, the result was 96.00 GiB | 125.42 MiB/sec, 14 minutes.
  ```
  [ec2-user@aws-c7a-large s3sync]$ time s3sync --max-parallel-uploads 64 --enable-additional-checksum s3://c1b01a9a-5cea-4650-b3d6-16ac37aad03a/ ./download/
  96.00 GiB | 41.67 MiB/sec,  transferred  16 objects | 0 objects/sec,  etag verified 16 objects,  checksum verified 16 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 39 minutes
  
  real	39m19.500s
  user	9m30.455s
  sys	2m33.711s
  ```

- Any object size support
  s3sync can handle any object size. From 0 byte to 5TiB.

- Low memory usage  
  Memory usage is low and does not depend on the object size or number of objects.
  It mainly depends on the number of workers/max parallel uploads count and multipart chunk size.  
  The default setting uses about 1.2GB of maximum memory for any object size or number of objects.

- Incremental transfer (Normal transfer)  
  Transfer only modified objects. If the object modification time is newer than the target object, the object is transferred.
  Incremental transfer can be resumed from the last checkpoint.
  Checking of modified objects is very fast.

- ETag(MD5 or equivalent) based incremental transfer  
  If you want to ETag based incremental transfer, you can use `--check-etag` option.  
  It compares the ETag of the source object with the ETag of the target object and transfers only modified objects.  
  ETag is not always MD5. If the object is uploaded with multipart upload, ETag is not MD5 digest but the MD5 digest of these concatenated values.
  The default setting use `multipart-threshold`(default:8MiB) and `multipart-chunksize`(default:8MiB) and calculate ETag for each part based on these values.  
  with `--auto-chunksize`, s3sync can calculate and compare ETag for each part and the entire object based on the correct chunk size. It is useful if you don't know the correct chunk size. But it will need more API calls and time. see `--auto-chunksize` option.  
  with `--dry-run`, you can check the synchronization status without transferring the objects.  
  with `--json-tracing`, you can output the tracing information in JSON format.
  ```bash
  s3sync -vv --dry-run --check-etag --auto-chunksize testdata/ s3://XXXX/testdata/
  2024-06-15T01:23:50.632072Z DEBUG object filtered. ETags are same. name="HeadObjectChecker" source_e_tag="da6a0d097e307ac52ed9b4ad551801fc" target_e_tag="da6a0d097e307ac52ed9b4ad551801fc" source_last_modified="2024-06-15T01:01:48.687+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=6291456 target_size=6291456 key="dir1/data2.dat"
  2024-06-15T01:23:50.675831Z DEBUG object filtered. ETags are same. name="HeadObjectChecker" source_e_tag="d126ef08817d0490e207e456cb0ae080-2" target_e_tag="d126ef08817d0490e207e456cb0ae080-2" source_last_modified="2024-06-15T01:01:48.685+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=9437184 target_size=9437184 key="dir1/data1.dat"
  2024-06-15T01:23:50.683417Z DEBUG object filtered. ETags are same. name="HeadObjectChecker" source_e_tag="ebe97f2a4738800fe71edbe389c000a6-2" target_e_tag="ebe97f2a4738800fe71edbe389c000a6-2" source_last_modified="2024-06-15T01:01:48.690+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=12582912 target_size=12582912 key="dir1/data3.dat"
  0 B | 0 B/sec,  transferred   0 objects | 0 objects/sec,  etag verified 0 objects,  checksum verified 0 objects,  deleted 0 objects,  skipped 3 objects,  error 0 objects, warning 0 objects,  duration 0 seconds
  ```

  with `--check-mtime-and-etag` option, s3sync checks the modification time and ETag of the source and target objects. It is useful if you want to transfer only modified objects based on the modification time and ETag.

- Additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME) based incremental transfer  
  If you use Amazon S3 with additional checksum, you can use `--check-additional-checksum` option.  
  This option compares the checksum of the both source and target objects and transfer only modified objects.It costs extra API calls per object.  
  with `--dry-run`, you can check the synchronization status without transferring the objects.  
  with `--json-tracing`, you can output the tracing information in JSON format.
  ```bash
  s3sync -vv --dry-run --check-additional-checksum SHA256 --additional-checksum-algorithm SHA256 testdata/ s3://XXXX/testdata/
  2024-06-15T01:06:30.035362Z DEBUG object filtered. Checksums are same. name="HeadObjectChecker" checksum_algorithm="SHA256" source_checksum="tp2uVqFNGoMU7UBmTEAz6gpVDuomc+BN9CpmrGufryw=" target_checksum="tp2uVqFNGoMU7UBmTEAz6gpVDuomc+BN9CpmrGufryw=" source_last_modified="2024-06-15T01:01:48.687+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=6291456 target_size=6291456 key="dir1/data2.dat"
  2024-06-15T01:06:30.041354Z DEBUG object filtered. Checksums are same. name="HeadObjectChecker" checksum_algorithm="SHA256" source_checksum="zWifJvli3SaQ9LZtHxzpOjkUE9x4ovgJZ+34As/NMwc=-2" target_checksum="zWifJvli3SaQ9LZtHxzpOjkUE9x4ovgJZ+34As/NMwc=-2" source_last_modified="2024-06-15T01:01:48.685+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=9437184 target_size=9437184 key="dir1/data1.dat"
  2024-06-15T01:06:30.086455Z DEBUG object filtered. Checksums are same. name="HeadObjectChecker" checksum_algorithm="SHA256" source_checksum="MyTyVYvNthXQp4fOwy/IzuKgFGIzIHpP1DiTfjZoV0Q=-2" target_checksum="MyTyVYvNthXQp4fOwy/IzuKgFGIzIHpP1DiTfjZoV0Q=-2" source_last_modified="2024-06-15T01:01:48.690+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=12582912 target_size=12582912 key="dir1/data3.dat"
  0 B | 0 B/sec,  transferred   0 objects | 0 objects/sec,  etag verified 0 objects,  checksum verified 0 objects,  deleted 0 objects,  skipped 3 objects,  error 0 objects, warning 0 objects,  duration 0 seconds
  ```
  
  with `--check-mtime-and-additional-checksum` option, s3sync checks the modification time and additional checksum of the source and target objects. It is useful if you want to transfer only modified objects based on the modification time and additional checksum.

- Amazon S3 Express One Zone support  
  s3sync can be used with [Amazon S3 Express one Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-Endpoints.html).  
  AWS CLI does not support `aws s3 sync` with Amazon S3 Express One Zone. see [AWS S3 sync operations do not work with S3 directory buckets (S3 Express One Zone)](https://github.com/aws/aws-cli/issues/8470).

  s3sync does not depend on the sorting order of returned objects of ListObjectsV2 API.  
  s3sync gathers all objects in the target bucket at first step(not concurrently) and store the information with Map.  
  If you want to sync object with Amazon S3 Express One Zone, s3sync is one of the strong candidates.

  In S3 Express One Zone, ETag is not MD5. So, s3sync uses additional checksum algorithm for verification by default(CRC64NVME).

- Full object checksum(CRC32/CRC32C/CRC64NVME) support  
  with `--full-object-checksum`, s3sync can use full object checksum(CRC32/CRC32C/CRC64NVME) for each object.

  For more information, see  [Checking object integrity in Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html).

- Server-side copy support  
  If you transfer S3 to S3(same-region), server-side copy is good choice.
  Server-side copy is very fast and does not transfer data over the network.
  If you transfer S3 to S3(cross-region or different object storage), you cannot use server-side copy.

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
- x86_64 Linux (kernel 3.2 or later, glibc 2.17 or later)
- ARM64 Linux (kernel 4.1 or later, glibc 2.17 or later)
- x86_64 Windows 10 or later
- ARM64 Windows 11
- MacOS 11.0 or later

## Licence
This project is licensed under the Apache-2.0 License.

## Installation
Download the latest binary from [Releases](https://github.com/nidor1998/s3sync/releases)  
This binary cannot be run on a glibc version less than or equal to 2.17. (i.e. CentOS 7, etc.)

This binary is built without proxy support for security reasons.

You can also build from source following the instructions below.
### Install Rust
See [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)

### Build
s3sync requires Rust 1.85 or later.
```bash
cargo install s3sync
```

## Usage
AWS credentials are required to use s3sync. IAM Roles, AWC CLI Profile, environment variables, etc supported.  
By default s3sync access credentials from many locations(IAM Roles, environment variables, etc).  
For more information, see [SDK authentication with AWS](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html)

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

### S3 to S3(server-side copy, works only in same-region)
```bash
s3sync --server-side-copy s3://bucket-name1/prefix s3://bucket-name2/prefix
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

### [Amazon S3 Transfer Acceleration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/transfer-acceleration.html)
```bash
s3sync --source-accelerate --target-accelerate s3://bucket-name1/prefix s3://bucket-name2/prefix
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
Optionally, s3sync can also calculate and compare additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME) for each object.

ETag is not always MD5. If the object is uploaded with multipart upload, ETag is not MD5 digest but the MD5 digest of these concatenated values.  
Even if multipart upload is used, s3sync can calculate and compare ETag for each part and the entire object.(with `--auto-chunksize`)

If the object is uploaded with SSE-KMS/SSE-C/DSSE-KMS, ETag is not MD5. In this case, s3sync cannot calculate and compare checksums for the object.

s3sync always uses the following elements to verify the integrity of the object.
- `Content-MD5` header(End-to-end API level integrity check, without `--disable-content-md5` option)  
  Amazon S3 recommends using `Content-MD5` header for end-to-end integrity check.  
  Note: Amazon S3 Express One Zone storage class does not support `Content-MD5` header.

- `x-amz-content-sha256` Authentication header(without `--disable-payload-signing` option)  
  This header is SHA256 digest of the request payload.    
  Note: Some S3-compatible storage ignores this header.

- Additional checksum algorithm(Optional)  
  Even if the object is uploaded with SSE-KMS/SSE-C/DSSE-KMS, s3sync can calculate and compare additional checksum algorithm.  
  Note: As of writing this document, few S3-compatible storage supports additional checksum algorithm.

- TLS(If not explicitly disabled)

The multipart ETag does not always match that of the source object. But with above elements, it is almost safe to assume that the object is not corrupted.  
With `--auto-chunksize`, s3sync can calculate and compare ETag/checksum for each part and the entire object.   
If you want strict integrity check, use `--auto-chunksize`, but it will need more API calls and time.

For more information, see  [Checking object integrity in Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html).

### Matrix of s3sync end-to-end object integrity check
In the case of Local to S3 or S3 to Local, s3sync compares ETags and additional checksums of Local that s3sync calculates with those of S3.  
In the case of S3 to S3, s3sync just compares ETags and additional checksums that calculated by S3.

If an object is not verified, s3sync show a warning message in the terminal.

Amazon S3 Express One Zone storage class does not support ETag as verification. Instead, s3sync uses additional checksum algorithm for verification by default(CRC64NVME).

#### ETag(MD5 digest or equivalent): plain-text/SSE-S3
|                  | Local to S3   | S3 to Local                                                                                        | S3 to S3                                                                                           |
|------------------|---------------|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| multipart object | always verify | verify as much as possible(without `--auto-chunksize`)<br/> always verify(with `--auto-chunksize`) | verify as much as possible(without `--auto-chunksize`)<br/> always verify(with `--auto-chunksize`) |
| single object    | always verify | always verify                                                                                      | always verify                                                                                      |

#### ETag(MD5 digest or equivalent): SSE-KMS/SSE-C/DSSE-KMS
|                  | Local to S3 | S3 to Local | S3 to S3   |
|------------------|-------------|-------------|------------|
| multipart object | not verify  | not verify  | not verify |
| single object    | not verify  | not verify  | not verify |

#### Additional checksum: plain-text/SSE-S3/SSE-KMS/SSE-C/DSSE-KMS
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
- DSSE-KMS

### Memory usage
s3sync consumes memory for each worker.   
For single object, approximately `average size of the object * worker-size(default 16) * 2`.  
For multipart object, approximately `multipart chunksize(default 8MiB) * worker-size(default 16) * 2 + multipart chunksize(default 8MiB) * max-parallel-uploads(default 16)`.

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
    "s3express:CreateSession",
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
    "s3:PutObjectTagging",
    "s3express:CreateSession"
]
```

### Advanced options

#### `--worker-size`
The number of workers. Many workers can improve performance. But it also increases CPU and memory usage.  
Default: 16

If you specify many workers, you may need to increase the number of open files.  
For example, on Linux: `ulimit -n 8192`

#### `--max-parallel-uploads`
The maximum number of parallel uploads/downloads for objects larger than `multipart-threshold`.  
This feature is implemented by using HTTP `Range` header.  
Default: 16

#### `--force-retry-count`
s3sync forcibly retries the operation that AWS SDK for Rust cannot retry.  
For example, in the case of `connection reset by peer`, s3sync will retry the operation.

#### `--remove-modified-filter`
If you want to overwrite the existing objects, specify the option.

#### `--check-etag`
For incremental transfer, s3sync compares the ETag of the source object with the ETag of the target object. If the ETag is different, s3sync transfers the object.

with `--auto-chunksize`, s3sync can calculate and compare ETag for each part and the entire object. It is useful if you don't know the correct chunk size. But it will need more API calls and time.

If both sides are S3, s3sync only compare the ETag of the source object with the ETag of the target object. If either side is not S3, s3sync calculates ETag of the local object.

You will need to know about Amazon S3 ETag.  
See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

#### `--put-last-modified-metadata`
This option adds user-defined metadata that contains the last-modified time of the source object.

user-defined metadata: `s3sync_origin_last_modified`

#### `--additional-checksum-algorithm`
If you want to use additional checksum for upload, specify the algorithm.

#### `--enable-additional-checksum`
If you want to use additional checksums for download, specify the option.

Warning: Even if the object was uploaded with additional checksum, without this option, s3sync does not verify additional checksum.

#### `--https-proxy`
This option is disabled by default for security reasons.
To enable this, build with the `--features legacy_hyper014_feature` option.

You can specify the proxy server for https.

Proxy authentication is supported. Like `http(s)://user:password@proxy:port`.

**Warning: Proxy support is achieved by older version crates(hyper0.14/rustls/webpki/ring). So, It should be used with caution, especially from a security perspective.**

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