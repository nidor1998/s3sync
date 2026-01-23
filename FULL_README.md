# s3sync

[![Crates.io](https://img.shields.io/crates/v/s3sync.svg)](https://crates.io/crates/s3sync)
[![crates.io downloads](https://img.shields.io/crates/d/s3sync.svg)](https://crates.io/crates/s3sync)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![MSRV](https://img.shields.io/badge/msrv-1.88.0-red)
![CI](https://github.com/nidor1998/s3sync/actions/workflows/ci.yml/badge.svg?branch=main)
[![codecov](https://codecov.io/gh/nidor1998/s3sync/branch/main/graph/badge.svg?token=GO3DGS2BR4)](https://codecov.io/gh/nidor1998/s3sync)
[![dependency status](https://deps.rs/crate/s3sync/latest/status.svg)](https://deps.rs/crate/s3sync/)
[![DeepSource](https://app.deepsource.com/gh/nidor1998/s3sync.svg/?label=active+issues&show_trend=true&token=Q3EjeUmx8Fu-ndXKEG133W-t)](https://app.deepsource.com/gh/nidor1998/s3sync/?ref=repository-badge)

## Table of contents

<details>
<summary>Click to expand to view table of contents </summary>

- [Overview](#Overview)
- [Features](#Features)
    * [Object integrity check](#Object-Integrity-Check)
    * [Multiple ways](#Multiple-Ways)
    * [Fast](#Fast)
    * [Any object size support](#Any-object-size-support)
    * [Low memory usage](#Low-memory-usage)
    * [Easy to use](#Easy-to-use)
    * [Robust retry logic](#Robust-retry-logic)
    * [Metadata support](#Metadata-support)
    * [Tagging support](#Tagging-support)
    * [SSE support](#SSE-support)
    * [Flexible filtering](#Flexible-filtering)
    * [User-defined metadata filtering](#User-defined-metadata-filtering)
    * [Tagging filtering](#Tagging-filtering)
    * [Incremental transfer](#Incremental-transfer)
    * [ETag-based incremental transfer](#Etag-based-incremental-transfer)
    * [Additional checksum-based incremental transfer](#Additional-checksum-based-incremental-transfer)
    * [Proxy support](#Proxy-support)
    * [Amazon S3 Express One Zone support](#Amazon-S3-Express-One-Zone-support)
    * [Versioning support](#Versioning-support)
    * [Point-in-time snapshot](#Point-in-time-snapshot)
    * [Server-side copy support](#Server-side-copy-support)
    * [Full object checksum support](#Full-object-checksum-support)
    * [Rate limiting by objects/bandwidth](#Rate-limiting-by-objectsbandwidth)
    * [Sync statistics report](#Sync-statistics-report)
    * [--delete support](#--delete-support)
    * [Etag-based conditional write/delete support](#Etag-based-conditional-writedelete-support)
    * [Lua scripting support](#Lua-scripting-support)
    * [User-defined preprocessing callback](#User-defined-preprocessing-callback)
    * [User-defined event callback](#User-defined-event-callback)
    * [User-defined filter callback](#User-defined-filter-callback)
- [Requirements](#Requirements)
- [License](#License)
- [Installation](#Installation)
    * [Install Rust](#Install-Rust)
    * [Build](#Build)
    * [As a Rust library](#As-A-Rust-Library)
- [Usage](#Usage)
    * [Local to S3](#Local-to-S3)
    * [S3 to Local](#S3-to-Local)
    * [S3 to S3](#S3-to-S3)
    * [S3 to S3 server-side copy](#S3-to-S3-server-side-copy)
    * [Specify access key/secret access key](#Specify-access-keysecret-access-key)
    * [Specify profile](#Specify-profile)
    * [Specify region](#Specify-region)
    * [Versioning mode](#Versioning-mode)
    * [Custom endpoint](#Custom-endpoint)
    * [Amazon S3 Transfer Acceleration](#Amazon-S3-Transfer-Acceleration)
    * [Amazon S3 Multi-Region Access Points](#Amazon-S3-Multi-Region-Access-Points)
    * [Amazon S3 Access Points](#Amazon-S3-Access-Points)
    * [Dry run mode](#Dry-run-mode)
    * [--delete mode](#--delete-mode)
- [Detailed information](#Detailed-information)
    * [Object integrity check detail](#Object-integrity-check-detail)
    * [--auto-chunksize option](#--auto-chunksize-option)
    * [Retry logic detail](#Retry-logic-detail)
    * [Filtering order](#Filtering-order)
    * [Incremental transfer detail](#Incremental-transfer-detail)
    * [Versioning support detail](#Versioning-support-detail)
    * [Versioning and point-in-time snapshot](#Versioning-and-point-in-time-snapshot)
    * [Metadata support detail](#Metadata-support-detail)
    * [Website redirect location](#Website-redirect-location)
    * [Memory usage detail](#Memory-usage-detail)
    * [Parallel object listing](#Parallel-object-listing)
    * [--max-parallel-listing-max-depth option](#--max-parallel-listing-max-depth-option)
    * [S3 Permissions](#S3-Permissions)
        + [Source bucket](#Source-Bucket)
        + [Target bucket](#Target-Bucket)
    * [Lua VM](#Lua-VM)
    * [Lua VM security](#Lua-VM-security)
    * [Lua script error](#Lua-script-error)
    * [Lua version](#Lua-version)
    * [CLI process exit codes](#CLI-process-exit-codes)

- [Advanced options](#Advanced-options)
    * [--worker-size](#--worker-size)
    * [--max-parallel-uploads](#--max-parallel-uploads)
    * [--force-retry-count](#--force-retry-count)
    * [--remove-modified-filter](#--remove-modified-filter)
    * [--filter-include-regex/--filter-exclude-regex](#--filter-include-regex--filter-exclude-regex)
    * [--check-etag](#--check-etag)
    * [--put-last-modified-metadata](#--put-last-modified-metadata)
    * [--additional-checksum-algorithm](#--additional-checksum-algorithm)
    * [--enable-additional-checksum](#--enable-additional-checksum)
    * [--disable-multipart-verify](#--disable-multipart-verify)
    * [-v](#-v)
    * [--aws-sdk-tracing](#--aws-sdk-tracing)
    * [--auto-complete-shell](#--auto-complete-shell)
    * [-h/--help](#-h--help)

- [All command line options](#All-command-line-options)

</details>

## Overview

s3sync is a reliable, flexible, and fast synchronization tool for S3.  
It serves as an alternative to the AWS CLI’s `aws s3 sync` command, offering more powerful features and better
performance.

It can be used to synchronize local directories with an S3 bucket and synchronize between S3 buckets as well.  
It supports multipart uploads, versioning, metadata, and tagging.

This tool is designed solely for object storage (S3 or S3-compatible) data synchronization.

Demo: c8i.xlarge (4 vCPU, 8 GB), Local to S3, 50,000 Objects (10 KiB Each), End-to-End Integrity Verified (MD5 and
SHA256)
![demo](media/demo.webp)

## Features

### Object integrity check

s3sync calculates ETag(MD5 or equivalent) for each object and compares them with the ETag in the target.  
An object on the local disk is read from the disk and compared with the checksum in the source or
target.  
Even if the source object was uploaded with multipart upload, s3sync can calculate and compare ETag for each part and
the entire object.   
With `--auto-chunksize` option, s3sync automatically calculates the correct chunk size for multipart upload (But it will
need more API calls and time).  
In the case of S3 to S3, s3sync simply compares ETags that are calculated by S3.  
Optionally, s3sync can also calculate and compare additional checksum (SHA256/SHA1/CRC32/CRC32C/CRC64NVME) for each
object.  
If you want to obtain evidence of the integrity check, you can use `-vv` option to output the verification information.

  ```bash
  $ s3sync -vv --additional-checksum-algorithm SHA256 ./30 s3://xxxxx |rg '(verified|sync completed)'
  2025-07-11T00:48:43.946290Z DEBUG e_tag verified. key="30/30Mib.dat" source_e_tag="\"a81230a7666d413e511f9c2c2523947a-4\"" target_e_tag="\"a81230a7666d413e511f9c2c2523947a-4\""
  2025-07-11T00:48:43.946298Z DEBUG additional checksum verified. key="30/30Mib.dat" additional_checksum_algorithm="SHA256" target_checksum="5NrHBc0Z1wNCbADRDy8mJaIvc53oxncCrw/Fa48VhxY=-4" source_checksum="5NrHBc0Z1wNCbADRDy8mJaIvc53oxncCrw/Fa48VhxY=-4"
  2025-07-11T00:48:43.946319Z  INFO sync completed. key="30Mib.dat" source_version_id="" source_last_modified="2025-06-17T06:19:54.483+00:00" target_key="30/30Mib.dat" size=31457280
  30.00 MiB | 22.47 MiB/sec,  transferred   1 objects | 0 objects/sec,  etag verified 1 objects,  checksum verified 1 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 1 second
  $
  ```

Note: Amazon S3 Express One Zone does not support ETag as verification. But s3sync uses additional checksum algorithm
for verification by default(CRC64NVME).

### Multiple ways

- Local to S3(S3-compatible storage)
- S3(S3-compatible storage) to Local
- S3 to S3(cross-region, same-region, same-account, cross-account, from-to S3/S3-compatible storage)

### Fast

s3sync is implemented in Rust and uses the AWS SDK for Rust, which supports multithreaded asynchronous I/O.  
In my environment(`c7a.xlarge`, with 160 workers), uploading from local to S3 achieved about 4,300 objects/sec (small
objects 10KiB),  
The following are benchmark results on a
`s3sync 1.45.0(glibc)/c7a.xlarge(4vCPU, 8GB)/200GB IOPS SSD(io 1)/Amazon Linux 2023 AMI` instance on AWS in
`ap-northeast-1`. No special optimizations were applied to the instance, the network, or the S3 topology.  
You can reproduce the benchmark with the following commands.

Note: The default s3sync setting uses `--worker-size 16` and `--max-parallel-uploads 16`. This is a moderate setting for
most cases. If you want to improve performance, you can increase `--worker-size` and `--max-parallel-uploads`. But it
will increase CPU and memory usage.

Note: Increasing `--worker-size` and `--max-parallel-uploads` is not always beneficial. This depends on factors such as
instance type, network, data size, and number of objects.

Local to S3, `c7a.xlarge(4vCPU, 8GB)` 100,000 objects(10KiB objects), 976.56 MiB | 41.96 MiB/sec, 23 seconds, and all
objects are end-to-end integrity verified(MD5, SHA256).

  ```
  [ec2-user@aws-c7a-xlarge s3sync]$ time s3sync --worker-size 160 --additional-checksum-algorithm SHA256 ./test_data s3://2ef073f2-f779-4361-956d-052ad0e6e79b/test_data
  976.56 MiB | 41.96 MiB/sec,  transferred 100000 objects | 4,297 objects/sec,  etag verified 100000 objects,  checksum verified 100000 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 23 seconds

  real    0m23.287s
  user    0m31.859s
  sys     0m22.800s
  ```

Local to S3, `c7a.xlarge(4vCPU, 8GB)` 16 objects(6GiB objects), 96.00 GiB | 256.72 MiB/sec, 6.23 minutes, and all
objects are end-to-end integrity verified(MD5, SHA256).

Note: Calculating ETag/additional checksum is costly with large local objects.

Note: Above the case, the bottleneck is the disk I/O (Maximum throughput is 500MB/s).

  ```
  [ec2-user@aws-c7a-xlarge s3sync]$ time s3sync --max-parallel-uploads 48 --additional-checksum-algorithm SHA256 ./test_data s3://2ef073f2-f779-4361-956d-052ad0e6e79b/test_data
  96.00 GiB | 256.72 MiB/sec,  transferred  16 objects | 0 objects/sec,  etag verified 16 objects,  checksum verified 16 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 6 minutes

  real    6m23.024s
  user    7m26.504s
  sys     1m45.131s
  ```

S3 to Local, `c7a.xlarge(4vCPU, 8GB)` 16 objects(6GiB objects), 96.00 GiB | 166.63 MiB/sec, 10 minutes, and all objects
are end-to-end integrity verified(MD5, SHA256).  
ETag/additional checksum verification is costly in the case of S3 to Local. Because s3sync needs to read the entire
downloaded object from the local disk to calculate ETag/checksum.   
You can disable it with `--disable-etag-verify` and remove `--enable-additional-checksum`. Without all verifications,
the result was 96.00 GiB | 505.33 MiB/sec, 3.2 minutes.

Note: Above the case, the bottleneck is the disk I/O (Maximum throughput is 500MB/s).

  ```
  [ec2-user@aws-c7a-xlarge s3sync]$ time s3sync --max-parallel-uploads 48 --enable-additional-checksum s3://2ef073f2-f779-4361-956d-052ad0e6e79b/ ./download/
  96.00 GiB | 166.63 MiB/sec,  transferred  16 objects | 0 objects/sec,  etag verified 16 objects,  checksum verified 16 objects,  deleted 0 objects,  skipped 0 objects,  error 0 objects, warning 0 objects,  duration 10 minutes

  real    9m50.060s
  user    7m55.742s
  sys     2m23.921s
  ```

### Any object size support

s3sync can handle any object size. From 0 bytes to 50TiB.  
Multipart upload is used for objects larger than `--multipart-threshold`(default:8MiB).

### Low memory usage

Memory usage is low and does not depend on the object size or number of objects.
It mainly depends on the number of workers/max parallel uploads count and multipart chunk size.  
The default setting uses about 1.2GB of maximum memory for any object size or number of objects.

### Easy to use

s3sync is designed to be easy to use.  
s3sync has over 100 command line options, this is because there are many use cases.  
But the default settings are reasonable for most cases of reliable synchronization.

For example, In an IAM role environment, the following command will transfer all objects from the local directory to
the S3 bucket and verify the integrity of the transferred objects using ETag(MD5 or equivalent).  
If something goes wrong, s3sync will display a warning or error message to help you understand the issue.

  ```bash
  s3sync /path/to/local s3://bucket-name/prefix
  ```

### Robust retry logic

For long-running operations, s3sync has a robust original retry logic in addition to AWS SDK's retry logic.

For more information, see [Retry logic detail](#Retry-logic-detail).

### Metadata support

All metadata of the object can be synchronized. For example, `Content-Type`, `Content-Encoding`, `Cache-Control`,
user-defined metadata, etc.

### Tagging support

All tags of the object can be synchronized.

### SSE support

The following SSE modes are supported.

- SSE-S3
- SSE-KMS
- SSE-C
- DSSE-KMS

### Flexible filtering

- Key, `ContentType`, user-defined metadata and tagging by regular expression.
- Size and modified time
- Custom filtering with a Lua script or User-defined callback function (Rust)

### User-defined metadata filtering

You can filter objects based on user-defined metadata.  
The regular expression syntax is the same as [fancy_regex](https://docs.rs/fancy-regex/latest/fancy_regex/#syntax).  
Example: `--filter-include-metadata-regex 'key1=(value1|xxx),key2=value2'`,
`--filter-exclude-metadata-regex 'key1=(value1|xxx),key2=value2'`

Note: When using this option, Additional API calls may be required to get the metadata of each object.

### Tagging filtering

You can filter objects based on tags.  
The regular expression syntax is the same as [fancy_regex](https://docs.rs/fancy-regex/latest/fancy_regex/#syntax).  
This crate supports lookaround features.

For example, `'^(?!.*&test=true).*stage=first'` can be used to filter objects that do not contain `test=true` and that
contain `stage=first` in the tags.  
And you can also create regular expressions that combine multiple logical conditions with lookaround features.  
This feature reduces the need for Lua scripts or custom callback Rust codes (see below) to filter objects with complex
patterns.

Note: When using this option, Additional API calls are required to get the tags of each object.

### Incremental transfer

This is the default behavior.  
Transfer only modified objects. If the object modification time is newer than the target object, the object is
transferred.
Incremental transfer can be resumed from the last checkpoint.
Checking of modified objects is fast (In my environment, it takes about 1 second per 25,000 objects).

### ETag-based incremental transfer

If you want ETag-based incremental transfer, you can use `--check-etag` option.  
It compares the ETag of the source object with the ETag of the target object and transfers only modified objects.  
ETag is not always MD5. If the object is uploaded with multipart upload, ETag is not MD5 digest but the MD5 digest of
these concatenated values.
By default, s3sync uses `multipart-threshold`(default:8MiB) and `multipart-chunksize`(default:8MiB) and calculate ETag
for each part based on these values.  
With `--auto-chunksize`, s3sync can calculate and compare ETag for each part and the entire object based on the correct
chunk size. It is useful if you don't know the correct chunk size. But it will need more API calls and time. See
`--auto-chunksize` option.  
With `--dry-run`, you can check the synchronization status without transferring the objects.  
With `--json-tracing`, you can output the tracing information in JSON format.

  ```bash
  s3sync -vv --dry-run --check-etag --auto-chunksize testdata/ s3://XXXX/testdata/
  2024-06-15T01:23:50.632072Z DEBUG object filtered. ETags are same. name="HeadObjectChecker" source_e_tag="da6a0d097e307ac52ed9b4ad551801fc" target_e_tag="da6a0d097e307ac52ed9b4ad551801fc" source_last_modified="2024-06-15T01:01:48.687+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=6291456 target_size=6291456 key="dir1/data2.dat"
  2024-06-15T01:23:50.675831Z DEBUG object filtered. ETags are same. name="HeadObjectChecker" source_e_tag="d126ef08817d0490e207e456cb0ae080-2" target_e_tag="d126ef08817d0490e207e456cb0ae080-2" source_last_modified="2024-06-15T01:01:48.685+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=9437184 target_size=9437184 key="dir1/data1.dat"
  2024-06-15T01:23:50.683417Z DEBUG object filtered. ETags are same. name="HeadObjectChecker" source_e_tag="ebe97f2a4738800fe71edbe389c000a6-2" target_e_tag="ebe97f2a4738800fe71edbe389c000a6-2" source_last_modified="2024-06-15T01:01:48.690+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=12582912 target_size=12582912 key="dir1/data3.dat"
  0 B | 0 B/sec,  transferred   0 objects | 0 objects/sec,  etag verified 0 objects,  checksum verified 0 objects,  deleted 0 objects,  skipped 3 objects,  error 0 objects, warning 0 objects,  duration 0 seconds
  ```

With `--check-mtime-and-etag` option, s3sync first checks the modification time, then ETag of the source and target
objects. It
is useful if you want to transfer only modified objects based on the modification time and ETag.

Note: Calculating ETag is costly with local objects. Because s3sync needs to read the entire object from the local disk
to calculate ETag.

### Additional checksum-based incremental transfer

If you use Amazon S3 with additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME), you can use
`--check-additional-checksum` option.  
This option compares the checksum of both source and target objects and transfers only modified objects. It costs
extra API calls per object.  
with `--dry-run`, you can check the synchronization status without transferring the objects.  
with `--json-tracing`, you can output the tracing information in JSON format.

  ```bash
  s3sync -vv --dry-run --check-additional-checksum SHA256 --additional-checksum-algorithm SHA256 testdata/ s3://XXXX/testdata/
  2024-06-15T01:06:30.035362Z DEBUG object filtered. Checksums are same. name="HeadObjectChecker" checksum_algorithm="SHA256" source_checksum="tp2uVqFNGoMU7UBmTEAz6gpVDuomc+BN9CpmrGufryw=" target_checksum="tp2uVqFNGoMU7UBmTEAz6gpVDuomc+BN9CpmrGufryw=" source_last_modified="2024-06-15T01:01:48.687+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=6291456 target_size=6291456 key="dir1/data2.dat"
  2024-06-15T01:06:30.041354Z DEBUG object filtered. Checksums are same. name="HeadObjectChecker" checksum_algorithm="SHA256" source_checksum="zWifJvli3SaQ9LZtHxzpOjkUE9x4ovgJZ+34As/NMwc=-2" target_checksum="zWifJvli3SaQ9LZtHxzpOjkUE9x4ovgJZ+34As/NMwc=-2" source_last_modified="2024-06-15T01:01:48.685+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=9437184 target_size=9437184 key="dir1/data1.dat"
  2024-06-15T01:06:30.086455Z DEBUG object filtered. Checksums are same. name="HeadObjectChecker" checksum_algorithm="SHA256" source_checksum="MyTyVYvNthXQp4fOwy/IzuKgFGIzIHpP1DiTfjZoV0Q=-2" target_checksum="MyTyVYvNthXQp4fOwy/IzuKgFGIzIHpP1DiTfjZoV0Q=-2" source_last_modified="2024-06-15T01:01:48.690+00:00" target_last_modified="2024-06-15T01:02:27+00:00" source_size=12582912 target_size=12582912 key="dir1/data3.dat"
  0 B | 0 B/sec,  transferred   0 objects | 0 objects/sec,  etag verified 0 objects,  checksum verified 0 objects,  deleted 0 objects,  skipped 3 objects,  error 0 objects, warning 0 objects,  duration 0 seconds
  ```

With `--check-mtime-and-additional-checksum` option, s3sync first checks the modification time, then additional checksum
of the
source and target objects. It is useful if you want to transfer only modified objects based on the modification time and
additional checksum.

Note: Calculating additional checksum is costly with local objects. Because s3sync needs to read the entire object from
the local disk to calculate the checksum.

### Proxy support

s3sync supports for proxy environment variables (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`).   
s3sync automatically respects these proxy environment variables.

Proxy authentication is supported. Like `http(s)://user:password@proxy:port` .

### Amazon S3 Express One Zone support

s3sync can be used
with [Amazon S3 Express One Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-Endpoints.html).

s3sync does not depend on the sorting order of returned objects of ListObjectsV2 API.  
s3sync gathers all objects in the target bucket at first step (not concurrently) and stores the information with Map.

In S3 Express One Zone, ETag is not MD5. So, s3sync uses additional checksum algorithm for verification by default(
CRC64NVME).

### Versioning support

All versions of the object can be synchronized. (Except intermediate delete markers)  
To enable versioning, use `--enable-versioning` option.

### Point-in-time snapshot

With a versioning-enabled S3 bucket, you can transfer objects at a specific point in time.

### Server-side copy support

When transferring from S3 to S3 (same region), server-side copy (CopyObject/UploadPartCopy) is a good choice.  
Server-side copy is fast and does not transfer data over the network.
If you transfer S3 to S3(cross-region or different object storage), you cannot use server-side copy.

### Full object checksum support

with `--full-object-checksum`, s3sync can use full object checksum(CRC32/CRC32C/CRC64NVME) for each object.

For more information,
see [Checking object integrity in Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html).

### Rate limiting by objects/bandwidth

For example, you can limit the number of objects transferred per second to 1,000, and the bandwidth to 100MB/sec.

### Sync statistics report

s3sync can check and report the synchronization status at any time.  
It only checks the synchronization status without transferring any objects.  
For example, If you want to check whether all objects transferred by the AWS CLI were transferred correctly (checksum
based),
the following command will show the report.

  ```bash
  aws s3 sync test_data s3://xxxx/
  s3sync --check-additional-checksum CRC64NVME --json-tracing --report-sync-status test_data s3://xxxx/ |tail -2 |jq
  ```

The following is an example of the report (the last two lines of the above command).

  <details>
  <summary>Click to expand to view report </summary>

  ```json
  {
  "timestamp": "2025-07-17T22:41:51.457601Z",
  "level": "INFO",
  "fields": {
    "name": "SYNC_STATUS",
    "type": "CHECKSUM",
    "status": "MATCHES",
    "key": "dir7/data4.dat",
    "checksum_algorithm": "CRC64NVME",
    "source_checksum": "YE4jTLSB/cA=",
    "target_checksum": "YE4jTLSB/cA=",
    "source_version_id": "",
    "target_version_id": "",
    "source_last_modified": "2025-06-14T03:52:21.843+00:00",
    "target_last_modified": "2025-07-17T22:40:51+00:00",
    "source_size": 22020096,
    "target_size": 22020096
  }
}
{
  "timestamp": "2025-07-17T22:41:51.473349Z",
  "level": "INFO",
  "fields": {
    "name": "REPORT_SUMMARY",
    "number_of_objects": 40,
    "etag_matches": 0,
    "checksum_matches": 40,
    "metadata_matches": 0,
    "tagging_matches": 0,
    "not_found": 0,
    "etag_mismatch": 0,
    "checksum_mismatch": 0,
    "metadata_mismatch": 0,
    "tagging_mismatch": 0,
    "etag_unknown": 0,
    "checksum_unknown": 0
  }
}
  ```

  </details>

You can check the synchronization status of the object's tagging and metadata with `--report-metadata-sync-status` and
`--report-tagging-sync-status` options.

Note: For reporting, s3sync uses a special process exit code. `0` If all objects are synchronized correctly. `3` If some
objects are not synchronized correctly.

Note: It needs additional API calls to get the metadata, checksum, and tags of each object.

### --delete support

Objects that exist in the target but not in the source are deleted during synchronization.  
Exclude filters other than `--filter-exclude-regex` will not prevent an object from being deleted.  
Since this can cause data loss, test first with the `--dry-run` option.

With `--max-delete`, you can limit the maximum number of deletions for safety.

### Etag-based conditional write/delete support

You can use `--if-match` for conditional write and delete operations(PutObject/CompleteMultipartUpload/DeleteObject)
with `If-Match` header.
And you can use `--copy-source-if-match` for conditional copy operations(CopyObject/UploadPartCopy) with
`x-amz-copy-source-if-match` header.

Actually, it serves as like optimistic locking.  
This prevents race conditions that s3sync overwrites or deletes an object that has been modified by another process
after s3sync checks the object.

`--if-none-match` option is also available.  
This option guarantees that the object does not exist in the target bucket. Without this option, race conditions can
occur.

If precondition fails, s3sync skips the object and outputs a warning message.  
If you want to treat it as an error, you can use `--warn-as-error` option.

It is a challenging topic to understand, please refer
to [AWS document](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)

Note: few S3-compatible storage supports conditional requests.

### Lua scripting support

You can use a [Lua](https://www.lua.org) (5.4) script to implement custom filtering, event handling, preprocessing
before transferring objects
to S3.  
`--preprocess-callback-lua-script`, `--event-callback-lua-script`, `--filter-callback-lua-script` options are available
for this purpose.  
Lua is widely recognized as a fast scripting language. The Lua engine is embedded in s3sync, so you can use Lua script
without any additional dependencies.  
For example, you can use Lua script to implement custom preprocessing logic, such as dynamically modifying the object
attributes(e.g., metadata, tagging) before transferring it to S3.  
By default, Lua scripts run in safe mode, so they cannot use Lua’s OS or I/O library functions.  
If you want to allow more Lua libraries, you can use `--allow-lua-os-library`, `--allow-lua-unsafe-vm` option.  
With Lua third-party C libraries, you can use more complex logic, like querying databases, requesting web APIs, etc.

See [Lua script example](https://github.com/nidor1998/s3sync/tree/main/src/lua/script/)

Note: `--preprocess-callback-lua-script` can not modify the object's key or content itself.

### User-defined preprocessing callback

This feature is for advanced users not satisfied with the default Lua scripting.  
If you are familiar with Rust, you can use `UserDefinedPreprocessCallback` to dynamically modify the object attributes(
e.g. metadata, tagging) before transferring it to S3.  
Thanks to Rust's clear compiler error messages and robust language features, even software engineers unfamiliar with the
language can implement it easily.  
To use `UserDefinedPreprocessCallback`, you need to implement the `PreprocessCallback` trait and rebuild the s3sync
binary.  
See [UserDefinedPreprocessCallback source code](https://github.com/nidor1998/s3sync/tree/main/src/callback/user_defined_preprocess_callback.rs)
for more information.

Note: `User-defined preprocessing callback` can not modify the object's key or content itself.

### User-defined event callback

This feature is for advanced users not satisfied with the default Lua scripting.  
If you are familiar with Rust, you can use `UserDefinedEventCallback` to implement custom event handling logic, such as
logging or monitoring and custom actions before and after synchronization.  
Thanks to Rust's clear compiler error messages and robust language features, even software engineers unfamiliar with the
language can implement it easily.  
To use `UserDefinedEventCallback`, you need to implement the `EventCallback` trait and rebuild the s3sync binary.  
See [UserDefinedEventCallback source code](https://github.com/nidor1998/s3sync/tree/main/src/callback/user_defined_event_callback.rs)
for more information.

### User-defined filter callback

This feature is for advanced users not satisfied with the default Lua scripting.  
If you are familiar with Rust, you can use `UserDefinedFilterCallback` to implement custom filtering logic.  
Thanks to Rust's clear compiler error messages and robust language features, even software engineers unfamiliar with
the language can implement it easily.  
To use `UserDefinedFilterCallback`, you need to implement the `FilterCallback` trait and rebuild the s3sync binary.  
See [UserDefinedFilterCallback source code](https://github.com/nidor1998/s3sync/tree/main/src/callback/user_defined_filter_callback.rs)
for more information.

## Requirements

- x86_64 Linux (kernel 3.2 or later)
- ARM64 Linux (kernel 4.1 or later)
- x86_64 Windows 11
- ARM64 Windows 11
- macOS 11.0 or later

All features are tested on the above platforms.

## License

This project is licensed under the Apache-2.0 License.

## Installation

Download the latest binary from [GitHub Releases](https://github.com/nidor1998/s3sync/releases)

`s3sync-linux-glibc2.28` binary cannot be run on a glibc version less than or equal to 2.17. (i.e., CentOS 7, etc.)  
`s3sync-linux-musl` binary is statically linked version and can be run on any Linux distribution.  
The statically linked version is optimized for size and portability, but it may have slightly lower performance than the
glibc version.  
The glibc version is recommended if you don't have a specific requirement.

Note: The statically linked version cannot load Lua C modules.

You can also build from source following the instructions below.

### Install Rust

See [Install Rust](https://www.rust-lang.org/tools/install)

### Build

s3sync requires Rust 1.88 or later.

```bash
cargo install s3sync
```

Note: The above command ignores the project's configuration and builds with global configuration. If you want to use Lua
third-party C libraries, you need to manually download the source code and build. e.g. `cargo install --path .` .

## As a Rust library

s3sync can be used as a Rust library.   
The s3sync CLI is a very thin wrapper over the s3sync library. All CLI features are available in the library.

See [docs.rs](https://docs.rs/s3sync/latest/s3sync/) for more information.

## Usage

AWS credentials are required to use s3sync. IAM Roles, AWS CLI Profile, environment variables, etc supported.  
By default, s3sync obtains credentials from many locations (IAM Roles, environment variables, etc).  
For more information,
see [SDK authentication with AWS](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html)

You can also specify credentials in the command line options or environment variables.

Region is required. It can be specified in the profile, environment, or command line options.

A prefix is optional. If not specified, the entire bucket will be synchronized.

If you specify a prefix, **s3sync doesn't automatically add a trailing slash.**  
For example, if you specify `s3://bucket-name/prefix` as the target, s3sync will synchronize source objects named `foo`
to `s3://bucket-name/prefixfoo`.  
And if you specify `s3://bucket-name/prefix/` as the target, s3sync will synchronize source objects named `foo` to
`s3://bucket-name/prefix/foo`.

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

### S3 to S3 Server-side copy

Works only in the same region.

```bash
s3sync --server-side-copy s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Specify access key/secret access key

```bash
s3sync --source-access-key foo --source-secret-access-key bar s3://bucket-name/prefix /path/to/local
```

### Specify profile

```bash
s3sync --source-profile foo --target-profile bar s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Specify region

```bash
s3sync --source-profile foo --source-region ap-northeast-1 s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Versioning mode

```bash
s3sync --enable-versioning s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Custom endpoint

You can specify a S3-compatible storage endpoint.  
Warning: If you use a custom endpoint, you may need to specify `--source-force-path-style` or
`--target-force-path-style`.

```bash
s3sync --target-endpoint-url https://foo --target-force-path-style /path/to/local s3://bucket-name/prefix 
```

### Amazon S3 Transfer Acceleration

```bash
s3sync --source-accelerate --target-accelerate s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### Amazon S3 Multi-Region Access Points

```bash
s3sync /path/to/local s3://arn:aws:s3::000000000000:accesspoint/xxxxxxxxxxxxx/
```

### Amazon S3 Access Points

Access Point alias is also supported.

```bash
s3sync /path/to/local s3://arn:aws:s3:000000000000:accesspoint/xxxxxxxxxxxxx/
```

### Dry run mode

```bash
s3sync --dry-run s3://bucket-name1/prefix s3://bucket-name2/prefix
```

### --delete mode

```bash
s3sync --delete s3://bucket-name1/prefix s3://bucket-name2/prefix
```

## Detailed information

### Object integrity check detail

s3sync calculates ETag(MD5) for a source object and compares them with the checksums in the target.  
Optionally, s3sync can also calculate and compare additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME) for each
object.

ETag is not always MD5. If the object is uploaded with multipart upload, ETag is not MD5 digest but the MD5 digest of
these concatenated values.  
Even if multipart upload is used, s3sync can calculate and compare ETag for each part and the entire object.(with
`--auto-chunksize`)

If the object is uploaded with SSE-KMS/SSE-C/DSSE-KMS, ETag is not MD5. In this case, s3sync cannot calculate and
compare checksums for the object.

s3sync uses the following elements to verify the integrity of the object.

- `Content-MD5` header(End-to-end API level integrity check, without `--disable-content-md5` option)  
  Amazon S3 recommends using `Content-MD5` header for end-to-end integrity check.  
  Note: Amazon S3 Express One Zone storage class does not support `Content-MD5` header.

- `x-amz-content-sha256` Authentication header(without `--disable-payload-signing` option)  
  This header is SHA256 digest of the request payload.    
  Note: Some S3-compatible storage ignores this header.

- Additional checksum algorithm (Optional)  
  Amazon S3 uses this header to verify the integrity of the object and denies the request if the checksum does not
  match.   
  And s3sync can calculate and compare additional checksum algorithm that S3 returns.

  Note: As of writing this document, some S3-compatible storages do not support additional checksum algorithm.

- TLS(If not explicitly disabled)

The multipart ETag does not always match that of the source object. But with the above elements, it is almost safe to
assume that the object is not corrupted.  
With `--auto-chunksize`, s3sync can calculate and compare ETag/checksum for each part and the entire object.   
If you want strict integrity check, use `--auto-chunksize`, but it will need more API calls and time.

For more information,
see  [Checking object integrity in Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html).

#### Matrix of s3sync end-to-end object integrity check

In the case of Local to S3 or S3 to Local, s3sync compares ETags and additional checksums of Local that s3sync
calculates with those of S3.  
In the case of S3 to S3, s3sync just compares ETags and additional checksums that calculated by S3.

If an object is not verified, s3sync shows a warning message in the terminal.

Amazon S3 Express One Zone storage class does not support ETag as verification. Instead, s3sync uses additional checksum
algorithm for verification by default(CRC64NVME).

##### ETag(MD5 digest or equivalent): plain-text/SSE-S3

|                  | Local to S3   | S3 to Local                                                                                        | S3 to S3                                                                                           |
|------------------|---------------|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| multipart object | always verify | verify as much as possible(without `--auto-chunksize`)<br/> always verify(with `--auto-chunksize`) | verify as much as possible(without `--auto-chunksize`)<br/> always verify(with `--auto-chunksize`) |
| single object    | always verify | always verify                                                                                      | always verify                                                                                      |

##### ETag(MD5 digest or equivalent): SSE-KMS/SSE-C/DSSE-KMS

|                  | Local to S3 | S3 to Local | S3 to S3   |
|------------------|-------------|-------------|------------|
| multipart object | not verify  | not verify  | not verify |
| single object    | not verify  | not verify  | not verify |

##### Additional checksum: plain-text/SSE-S3/SSE-KMS/SSE-C/DSSE-KMS

|                  | Local to S3   | S3 to Local   | S3 to S3                                                                                            |
|------------------|---------------|---------------|-----------------------------------------------------------------------------------------------------|
| multipart object | always verify | always verify | verify as much as possible (without `--auto-chunksize`)<br/> always verify(with `--auto-chunksize`) |
| single object    | always verify | always verify | always verify                                                                                       |

Note: To enable end-to-end additional checksum verification, `--enable-additional-checksum` must be specified.  
Note: In the case of S3 to S3, the same checksum algorithm must be used for both source and target.

### --auto-chunksize option

If `--auto-chunksize` is specified, s3sync automatically calculates the correct chunk size for multipart upload.  
This is done by `HeadObject` API with `partNumber` request parameter and `x-amz-mp-parts-count` response header.
`--auto-chunksize` requires extra API calls(one per part).  
Remember that not all S3-compatible storage supports these parameters.  
If S3-compatible storage does not support these parameters, s3sync will show a warning message in the terminal.

See: [HeadObject Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax)

Note: If the chunk size for a multipart upload is unknown, verifying object integrity using ETag or composite checksum
is impossible (this applies to tools other than s3sync as well).

**Warning: In the case of S3 to S3, if the source object is uploaded with a large chunk size, s3sync will consume a lot
of
memory.**

### Retry logic detail

s3sync automatically retries with exponential backoff implemented in the AWS SDK for Rust.  
You can configure the retry behavior with `--aws-max-attempts` and `--initial-backoff-milliseconds` option.

s3sync uses another original retry logic `force retry`.  
In this logic, s3sync retries the operation that is retryable for synchronization purpose(e.g.,
`Connection reset by peer` for cases the AWS SDK for Rust does not retry even though the operation is idempotent).  
For example, if `--force-retry-count` is `5` (default) and `--aws-max-attempts` is `10` (default), s3sync retries the
operation up to total 50 times in the case of retryable error.  
If a single object fails to transfer after exceeding `--force-retry-count`, s3sync will stop the whole operation and
exit with error code `1`.

On the other hand, if an error that is not retryable occurs, s3sync will stop the whole operation immediately and exit
with error code `1`.

This behavior is useful for synchronization that is taking a long time to complete and hard to resume.  
In almost all cases, s3sync will recover from the error before the operation is stopped.

If you have a trouble with force retry(S3 SlowDown/503/429 response, etc.), you can disable it with
`--force-retry-count 0`.

If s3sync stops due to an error, you can resume the operation by running the same command again. s3sync uses incremental
transfer, so it will not transfer the same objects again.

### Filtering order

s3sync filters objects in the following order.  
You can specify multiple filters.

1. `--filter-mtime-before`
2. `--filter-mtime-after`
3. `--filter-smaller-size`
4. `--filter-larger-size`
5. `--filter-include-regex`
6. `--filter-exclude-regex`
7. `FilterCallback(--filter-callback-lua-script/UserDefinedFilterCallback)`
8. `Update check(Check the source object has been modified)`
9. `--filter-include-content-type-regex`
10. `--filter-exclude-content-type-regex`
11. `--filter-include-metadata-regex`
12. `--filter-exclude-metadata-regex`
13. `--filter-include-tag-regex`
14. `--filter-exclude-tag-regex`
15. `PreprocessCallback(--preprocess-callback-lua-script/UserDefinedPreprocessCallback)`

It is recommended to filter before `Update check`, because after that, s3sync needs to call extra API calls.  
But after `Update check`, you can get more information about the object, such as the additional checksum, user-defined
metadata, tagging, etc.

Note: `PreprocessCallback(--preprocess-callback-lua-script/UserDefinedPreprocessCallback)` is called just before
transferring the object to S3. So dry-run does not call this callback.

### Incremental transfer detail

s3sync transfers only modified objects. It checks the `LastModified` timestamp.

If `--check-size` is specified, s3sync only checks the size of the object.

At first, incremental transfer lists all objects in the target.  
Then, s3sync compares the objects and transfers only modified objects.

Therefore, incremental transfer takes time to start for large numbers of existing target objects.  
In my environment, Amazon S3 takes about 1 second per 50,000 objects to list objects in the target. (It depends on
remote
storage.)

If there are few objects in the target, incremental transfer starts immediately, regardless of the number of source
objects.

### Versioning support detail

s3sync uses user-defined metadata to check the version of the object.  
If `--enable-versioning` is specified, s3sync adds user-defined metadata to the object.
If you transfer to an existing bucket, because of the lack of user-defined metadata, s3sync will transfer all versions
of
the object.  
Generally, you should use `--enable-versioning` when you transfer to a new bucket.

With `--filter-mtime-before` and `--filter-mtime-after` options, you can get snapshots of the objects at a specific
period.

Intermediate delete markers are not synchronized. Latest version delete markers are synchronized.

user-defined metadata: `s3sync_origin_version_id`, `s3sync_origin_last_modified`

### Versioning and point-in-time snapshot

s3sync supports point-in-time recovery of objects.

You can use `--point-in-time` option to enable point-in-time synchronization.

For example, if you want to synchronize objects at `2025-07-16T06:37:59Z`, you can use the following command:

S3 to S3(In the same region):

```bash
s3sync --server-side-copy --point-in-time 2025-07-16T06:37:59Z s3://source_bucket s3://target_bucket
```

S3 to Local:

```bash
s3sync --point-in-time 2025-07-16T06:37:59Z s3://source_bucket /path/to/local
```

### Metadata support detail

The following metadata of the S3 object is synchronized.

- Content-Type
- Content-Encoding
- Cache-Control
- Content-Disposition
- Content-Language
- Expires
- User-defined metadata

### Website redirect location

If the source object has `x-amz-website-redirect-location` metadata, s3sync copies it to the target object.

And, you can specify `--website-redirect-location` option to set the redirect location for the target object.

### Memory usage detail

s3sync consumes memory for each worker.   
For single-object uploads, approximately `average size of the object * worker-size(default 16) * 2`.  
For multipart uploads, approximately
`multipart chunksize(default 8MiB) * worker-size(default 16) * 2 + multipart chunksize(default 8MiB) * max-parallel-uploads(default 16)`.

Because s3sync uses incremental transfer, it lists all objects in the target bucket and stores the result in memory.  
Therefore, if there are a large number of objects in the target bucket, s3sync can consume a lot of memory.  
If you do not use the `--delete` option, s3sync will consume about 100MB per 1,000,000 target objects.  
If you use the `--delete` option, s3sync will consume about 250MB per 1,000,000 target objects.

To reduce memory usage, you can divide the target objects by prefix and run s3sync multiple times or specify
`--remove-modified-filter` and `--head-each-target` options.  
You can also divide the target objects by filter and run s3sync multiple times.

The `--head-each-target` option calls the `HeadObject` API for each source object, but it is a trade-off between memory
usage and API calls.

### Parallel object listing

By default, s3sync lists objects in the source and target in parallel (default 16 workers).  
The parallel listing is enabled up to the second level of subdirectories or prefixes.  
The depth is configurable with `--max-parallel-listing-max-depth` option.

For example, if the source is `s3://bucket-name/prefix` and there are many objects under `prefix/dir1`,
`prefix/dir2`, ..., `prefix/dir16`, s3sync lists objects under these prefixes in parallel.

You can configure the number of parallel listing workers with `--max-parallel-listings` option.  
If set to `1`, parallel listing is disabled.

This feature can significantly improve performance with incremental transfer when there are many objects in the source
and target.

With express one zone storage class, parallel listing may return in progress multipart upload objects.   
So, parallel listing is disabled by default when the source or target bucket uses express one zone storage class.   
You can enable it with `--allow-parallel-listings-in-express-one-zone` option.

When `--enable-versioning` or `--point-in-time` option is specified, parallel listing is disabled.

Note: Parallel listing may use CPU and memory.

### --max-parallel-listing-max-depth option

By default, s3sync lists objects in the source and target in parallel up to the second level of
subdirectories or prefixes.  
And deeper levels are listed without parallelization.
This is because parallel listing at deeper levels may not improve incremental transfer performance.

But in some cases, parallel listing at deeper levels may improve incremental transfer performance.
You can configure the maximum depth of parallel listing workers with `--max-parallel-listing-max-depth` option.

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

### Lua VM

Each type of callback has its own Lua VM and memory limit.  
Lua VM is shared between workers and called serially.  
Each type of Lua script is loaded and compiled once at the CLI argument parsing stage and lives until the end of the
total synchronization operations.

### Lua VM security

By default, a Lua script runs in a safe mode.
Lua's [Operating System facilities](https://www.lua.org/manual/5.4/manual.html#6.9)
and [Input and Output Facilities](https://www.lua.org/manual/5.4/manual.html#6.8) are disabled by default.  
This is because these facilities can be used to execute arbitrary commands, which can be a security risk. (especially
set-uid/set-gid/fs-capability programs)  
Also, Lua VM is not allowed to load unsafe standard libraries or C modules.

If these restrictions are too strict, you can use `--allow-lua-os-library` or `--allow-lua-unsafe-vm` options to allow
Lua's OS facilities and unsafe standard libraries or C modules.

Note: The statically linked binary cannot load C modules.

### Lua script error

Generally, if a Lua script raises an error, s3sync will stop the operation and exit with error code `1`.  
But an event callback Lua script does not stop the operation, just shows the warning message.

### Lua version

s3sync uses [mlua](https://docs.rs/mlua/latest/mlua/) for Lua scripting support. By default, s3sync uses Lua 5.4.  
If you want to use `LuaJIT`, `luau-jit`, etc, you can modify the `Cargo.toml` file and set the `mlua` feature.  
For more information, see [mlua feature flags](https://github.com/mlua-rs/mlua#feature-flags).

### CLI process exit codes

- 0: Exit without error
- 1: Exit with error
- 2: Invalid arguments
- 3: Exit with warning
- 101: Abnormal termination (Rust panic, it's a bug of s3sync)

## Advanced options

### --worker-size

The number of workers. Many workers can improve performance. But it also increases CPU and memory usage.  
Default: 16

If you specify many workers, you may need to increase the number of open files.  
For example, on Linux: `ulimit -n 8192`

### --max-parallel-uploads

The maximum number of parallel uploads/downloads for objects larger than `multipart-threshold`.  
This feature is implemented by using HTTP `Range` header.  
Default: 16

**Note: Specifying a large number of parallel uploads(64 or more) is not recommended. It can cause OS to be unstable.**

### --force-retry-count

s3sync forcibly retries the operation that AWS SDK for Rust cannot retry.  
For example, in the case of `connection reset by peer`, s3sync will retry the operation.

### --remove-modified-filter

If you want to overwrite the existing objects, specify the option.

### --filter-include-regex/--filter-exclude-regex

You can specify the regular expression to filter the source objects.  
The regular expression syntax is the same as [fancy_regex](https://docs.rs/fancy-regex/latest/fancy_regex/#syntax).

### --check-etag

For incremental transfer, s3sync compares the ETag of the source object with the ETag of the target object. If the ETag
is different, s3sync transfers the object.

with `--auto-chunksize`, s3sync can calculate and compare ETag for each part and the entire object. It is useful if you
don't know the correct chunk size. But it will need more API calls and time.

If both sides are S3, s3sync only compares the ETag of the source object with the ETag of the target object. If either
side is not S3, s3sync calculates ETag of the local object.

You will need to know about Amazon S3 ETag.  
See: [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)

### --put-last-modified-metadata

This option adds user-defined metadata that contains the last-modified time of the source object.

user-defined metadata: `s3sync_origin_last_modified`

### --additional-checksum-algorithm

If you want to use additional checksum for upload, specify the algorithm.

Calculating an additional checksum is costly with large local objects.  
SHA256/SHA1 additional checksum, in particular, may require significant processing time.

### --enable-additional-checksum

If you want to use additional checksums for download, specify the option.

Calculating an additional checksum is costly with large local objects.  
SHA256/SHA1 additional checksum, in particular, may require significant processing time.

Warning: Even if the object was uploaded with additional checksum, without this option, s3sync does not verify
additional checksum.

### --disable-multipart-verify

When an object is uploaded with multipart upload, its ETag may not match that of the target object.  
This can occur when the chunk size that the object was uploaded with is different. If you don't know the correct chunk
size, you can disable the verification with this option.  
You can specify the chunk size with `--multipart-threshold` and `--multipart-chunksize` (Default: 8MiB).

If extra API calls are allowed, you can use `--auto-chunksize` instead.  
However, please note that not all S3-compatible storage supports this option.   
**Warning: In the case of S3 to S3, if the source object is uploaded with a large chunk size, s3sync will consume a lot
of
memory.**

### --disable-additional-checksum-verify

When `--additional-checksum-algorithm` is specified, s3sync verifies the additional checksum of the target object.  
But if source S3-compatible storage does not support additional checksum, s3sync cannot verify the additional
checksum.  
In this case, you can disable the verification with this option, while keeping the additional checksum in the target
object.

### -v

s3sync uses [tracing-subscriber](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/) for tracing.    
More occurrences increase the verbosity.  
For example, `-v`: show `info`, `-vv`: show `debug`, `-vvv`: show `trace`  
By default, s3sync shows warning and error messages.

`info` and `debug` messages are useful for troubleshooting. `trace` messages are useful for debugging.

Instead of `-v`, you can use `RUST_LOG` environment variable.

On the other hand, you can use `-q`, `-qq` to reduce the verbosity.

### --aws-sdk-tracing

For troubleshooting, s3sync can output the AWS SDK for Rust's tracing information.  
Instead of `--aws-sdk-tracing`, you can use `RUST_LOG` environment variable.

### --auto-complete-shell

You can output the shell script to complete the command.

```bash
s3sync --auto-complete-shell bash
```

### -h/--help

For more information, see `s3sync -h`.

## All command line options

<details>
<summary>Click to expand to view all command line options </summary>

```bash
$ s3sync -h
Usage: s3sync [OPTIONS] [SOURCE] [TARGET]

Arguments:
  [SOURCE]  s3://<BUCKET_NAME>[/prefix] or local path [env: SOURCE=]
  [TARGET]  s3://<BUCKET_NAME>[/prefix] or local path [env: TARGET=]

Options:
  -v, --verbose...  Increase logging verbosity
  -q, --quiet...    Decrease logging verbosity
  -h, --help        Print help (see more with '--help')
  -V, --version     Print version

General:
      --dry-run           A simulation mode. No actions will be performed [env: DRY_RUN=]
      --show-no-progress  Don't show the progress bar [env: SHOW_NO_PROGRESS=]
      --server-side-copy  Use server-side copy. This option is only available both source and target are S3 storage.
                          It cannot work with between different object storages or regions. [env: SERVER_SIDE_COPY=]

AWS Configuration:
      --aws-config-file <FILE>
          Location of the file that the AWS CLI uses to store configuration profiles [env: AWS_CONFIG_FILE=]
      --aws-shared-credentials-file <FILE>
          Location of the file that the AWS CLI uses to store access keys [env: AWS_SHARED_CREDENTIALS_FILE=]
      --source-profile <SOURCE_PROFILE>
          Source AWS CLI profile [env: SOURCE_PROFILE=]
      --source-access-key <SOURCE_ACCESS_KEY>
          Source access key [env: SOURCE_ACCESS_KEY=]
      --source-secret-access-key <SOURCE_SECRET_ACCESS_KEY>
          Source secret access key [env: SOURCE_SECRET_ACCESS_KEY=]
      --source-session-token <SOURCE_SESSION_TOKEN>
          Source session token [env: SOURCE_SESSION_TOKEN=]
      --target-profile <TARGET_PROFILE>
          Target AWS CLI profile [env: TARGET_PROFILE=]
      --target-access-key <TARGET_ACCESS_KEY>
          Target access key [env: TARGET_ACCESS_KEY=]
      --target-secret-access-key <TARGET_SECRET_ACCESS_KEY>
          Target secret access key [env: TARGET_SECRET_ACCESS_KEY=]
      --target-session-token <TARGET_SESSION_TOKEN>
          Target session token [env: TARGET_SESSION_TOKEN=]

Source Options:
      --source-region <SOURCE_REGION>
          Source region [env: SOURCE_REGION=]
      --source-endpoint-url <SOURCE_ENDPOINT_URL>
          Source endpoint url [env: SOURCE_ENDPOINT_URL=]
      --source-accelerate
          Use Amazon S3 Transfer Acceleration for the source bucket [env: SOURCE_ACCELERATE=]
      --source-request-payer
          Use request payer for the source bucket [env: SOURCE_REQUEST_PAYER=]
      --source-force-path-style
          Force path-style addressing for source endpoint [env: SOURCE_FORCE_PATH_STYLE=]

Target Options:
      --target-region <TARGET_REGION>
          Target region [env: TARGET_REGION=]
      --target-endpoint-url <TARGET_ENDPOINT_URL>
          Target endpoint url [env: TARGET_ENDPOINT_URL=]
      --target-accelerate
          Use Amazon S3 Transfer Acceleration for the target bucket [env: TARGET_ACCELERATE=]
      --target-request-payer
          Use request payer for the target bucket [env: TARGET_REQUEST_PAYER=]
      --target-force-path-style
          Force path-style addressing for target endpoint [env: TARGET_FORCE_PATH_STYLE=]
      --storage-class <STORAGE_CLASS>
          Type of storage to use for the target object.
          Valid choices: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONE-ZONE_IA | INTELLIGENT_TIERING | GLACIER |
                         DEEP_ARCHIVE | GLACIER_IR | EXPRESS_ONEZONE [env: STORAGE_CLASS=]

Filtering:
      --filter-mtime-before <FILTER_MTIME_BEFORE>
          Sync only objects older than given time (RFC3339 datetime).
          Example: 2023-02-19T12:00:00Z [env: FILTER_MTIME_BEFORE=]
      --filter-mtime-after <FILTER_MTIME_AFTER>
          Sync only objects newer than OR EQUAL TO given time (RFC3339 datetime).
          Example: 2023-02-19T12:00:00Z [env: FILTER_MTIME_AFTER=]
      --filter-smaller-size <FILTER_SMALLER_SIZE>
          Sync only objects smaller than given size.
          Allow suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB [env: FILTER_SMALLER_SIZE=]
      --filter-larger-size <FILTER_LARGER_SIZE>
          Sync only objects larger than OR EQUAL TO given size.
          Allow suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB [env: FILTER_LARGER_SIZE=]
      --filter-include-regex <FILTER_INCLUDE_REGEX>
          Sync only objects that match a given regular expression [env: FILTER_INCLUDE_REGEX=]
      --filter-exclude-regex <FILTER_EXCLUDE_REGEX>
          Do not sync objects that match a given regular expression [env: FILTER_EXCLUDE_REGEX=]
      --filter-include-content-type-regex <FILTER_INCLUDE_CONTENT_TYPE_REGEX>
          Sync only objects that have Content-Type matching a given regular expression.
          If the source is local storage, Content-Type is guessed by the file extension,
          Unless --no-guess-mime-type is specified.
          It may take an extra API call to get Content-Type of the object.
           [env: FILTER_INCLUDE_CONTENT_TYPE_REGEX=]
      --filter-exclude-content-type-regex <FILTER_EXCLUDE_CONTENT_TYPE_REGEX>
          Do not sync objects that have Content-Type matching a given regular expression.
          If the source is local storage, Content-Type is guessed by the file extension,
          Unless --no-guess-mime-type is specified.
          It may take an extra API call to get Content-Type of the object.
           [env: FILTER_EXCLUDE_CONTENT_TYPE_REGEX=]
      --filter-include-metadata-regex <FILTER_INCLUDE_METADATA_REGEX>
          Sync only objects that have metadata matching a given regular expression.
          Keys(lowercase) must be sorted in alphabetical order, and comma separated.
          This filter is applied after all other filters(except tag filters).
          It may take an extra API call to get metadata of the object.

          Example: "key1=(value1|value2),key2=value2" [env: FILTER_INCLUDE_METADATA_REGEX=]
      --filter-exclude-metadata-regex <FILTER_EXCLUDE_METADATA_REGEX>
          Do not sync objects that have metadata matching a given regular expression.
          Keys(lowercase) must be sorted in alphabetical order, and comma separated.
          This filter is applied after all other filters(except tag filters).
          It may take an extra API call to get metadata of the object.

          Example: "key1=(value1|value2),key2=value2" [env: FILTER_EXCLUDE_METADATA_REGEX=]
      --filter-include-tag-regex <FILTER_INCLUDE_TAG_REGEX>
          Sync only objects that have tag matching a given regular expression.
          Keys must be sorted in alphabetical order, and '&' separated.
          This filter is applied after all other filters.
          It takes an extra API call to get tags of the object.

          Example: "key1=(value1|value2)&key2=value2" [env: FILTER_INCLUDE_TAG_REGEX=]
      --filter-exclude-tag-regex <FILTER_EXCLUDE_TAG_REGEX>
          Do not sync objects that have tag matching a given regular expression.
          Keys must be sorted in alphabetical order, and '&' separated.
          This filter is applied after all other filters.
          It takes an extra API call to get tags of the object.

          Example: "key1=(value1|value2)&key2=value2" [env: FILTER_EXCLUDE_TAG_REGEX=]

Update Checking:
      --remove-modified-filter
          Do not update checking(ListObjectsV2) for modification in the target storage [env: REMOVE_MODIFIED_FILTER=]
      --check-size
          Use object size for update checking [env: CHECK_SIZE=]
      --check-etag
          Use ETag for update checking [env: CHECK_ETAG=]
      --check-mtime-and-etag
          Use the modification time and ETag for update checking.
          If the source modification date is newer, check the ETag.
           [env: CHECK_MTIME_AND_ETAG=]
      --check-additional-checksum <CHECK_ADDITIONAL_CHECKSUM>
          Use additional checksum for update checking [env: CHECK_ADDITIONAL_CHECKSUM=]
      --check-mtime-and-additional-checksum <CHECK_MTIME_AND_ADDITIONAL_CHECKSUM>
          Use the modification time and additional checksum for update checking.
          If the source modification date is newer, check the additional checksum.
           [env: CHECK_MTIME_AND_ADDITIONAL_CHECKSUM=]

Verification:
      --additional-checksum-algorithm <ADDITIONAL_CHECKSUM_ALGORITHM>
          Additional checksum algorithm for upload [env: ADDITIONAL_CHECKSUM_ALGORITHM=]
      --full-object-checksum
          Use full object checksum for verification. CRC64NVME automatically use full object checksum.
          This option cannot be used with SHA1/SHA256 additional checksum. [env: FULL_OBJECT_CHECKSUM=]
      --enable-additional-checksum
          Enable additional checksum for download [env: ENABLE_ADDITIONAL_CHECKSUM=]
      --disable-multipart-verify
          Disable multipart upload verification with ETag/additional checksum [env: DISABLE_MULTIPART_VERIFY=]
      --disable-etag-verify
          Disable ETag verification [env: DISABLE_ETAG_VERIFY=]
      --disable-additional-checksum-verify
          Disable additional checksum verification
          But use additional checksum for upload (The hash value is stored in the target object). [env: DISABLE_ADDITIONAL_CHECKSUM_VERIFY=]

Performance:
      --worker-size <WORKER_SIZE>
          Number of workers for synchronization [env: WORKER_SIZE=] [default: 16]
      --max-parallel-uploads <MAX_PARALLEL_UPLOADS>
          Maximum number of parallel multipart uploads/downloads [env: MAX_PARALLEL_UPLOADS=] [default: 16]
      --max-parallel-listings <MAX_PARALLEL_LISTINGS>
          Maximum number of parallel listings of objects. [env: MAX_PARALLEL_LISTINGS=] [default: 16]
      --max-parallel-listing-max-depth <MAX_PARALLEL_LISTING_MAX_DEPTH>
          Maximum depth(sub directroy/prefix) of parallel listings. [env: MAX_PARALLEL_LISTING_MAX_DEPTH=] [default: 2]
      --object-listing-queue-size <OBJECT_LISTING_QUEUE_SIZE>
          Queue size for object listings [env: OBJECT_LISTING_QUEUE_SIZE=] [default: 200000]
      --allow-parallel-listings-in-express-one-zone
          Allow parallel listings in express one zone storage class.
          It may include multipart upload in progress objects in the listing result. [env: ALLOW_PARALLEL_LISTINGS_IN_EXPRESS_ONE_ZONE=]
      --rate-limit-objects <RATE_LIMIT_OBJECTS>
          Rate limit objects per second [env: RATE_LIMIT_OBJECTS=]
      --rate-limit-bandwidth <RATE_LIMIT_BANDWIDTH>
          Rate limit bandwidth(bytes per sec). Allow suffixes: MB, MiB, GB, GiB [env: RATE_LIMIT_BANDWIDTH=]

Multipart Settings:
      --multipart-threshold <MULTIPART_THRESHOLD>
          Object size threshold that s3sync uses for multipart upload
          Allow suffixes: MB, MiB, GB, GiB.
          The larger the size, the larger the memory usage. [env: MULTIPART_THRESHOLD=] [default: 8MiB]
      --multipart-chunksize <MULTIPART_CHUNKSIZE>
          Chunk size that s3sync uses for multipart upload of individual files
          Allow suffixes: MB, MiB, GB, GiB.
          The larger the size, the larger the memory usage. [env: MULTIPART_CHUNKSIZE=] [default: 8MiB]
      --auto-chunksize
          Automatically adjusts a chunk size to match the source or target.
          It takes extra HEAD requests(1 API call per part). [env: AUTO_CHUNKSIZE=]

Metadata/Headers:
      --cache-control <CACHE_CONTROL>
          Cache-Control HTTP header to set on the target object [env: CACHE_CONTROL=]
      --content-disposition <CONTENT_DISPOSITION>
          Content-Disposition HTTP header to set on the target object [env: CONTENT_DISPOSITION=]
      --content-encoding <CONTENT_ENCODING>
          Content-Encoding HTTP header to set on the target object [env: CONTENT_ENCODING=]
      --content-language <CONTENT_LANGUAGE>
          Content-Language HTTP header to set on the target object [env: CONTENT_LANGUAGE=]
      --content-type <CONTENT_TYPE>
          Content-Type HTTP header to set on the target object [env: CONTENT_TYPE=]
      --expires <EXPIRES>
          Expires HTTP header to set on the target object(RFC3339 datetime)
          Example: 2023-02-19T12:00:00Z [env: EXPIRES=]
      --metadata <METADATA>
          Metadata to set on the target object
          Example: key1=value1,key2=value2 [env: METADATA=]
      --website-redirect <WEBSITE_REDIRECT>
          x-amz-website-redirect-location header to set on the target object [env: WEBSITE_REDIRECT=]
      --no-sync-system-metadata
          Do not sync system metadata
          System metadata: content-disposition, content-encoding, content-language, content-type,
                           cache-control, expires, website-redirect [env: NO_SYNC_SYSTEM_METADATA=]
      --no-sync-user-defined-metadata
          Do not sync user-defined metadata [env: NO_SYNC_USER_DEFINED_METADATA=]

Tagging:
      --tagging <TAGGING>    Tagging to set on the target object.
                             Key/value must be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.

                             Example: key1=value1&key2=value2 [env: TAGGING=]
      --disable-tagging      Do not copy tagging [env: DISABLE_TAGGING=]
      --sync-latest-tagging  Copy the latest tagging from the source if necessary.
                             If this option is enabled, the --remove-modified-filter and
                             --head-each-target options are automatically enabled. [env: SYNC_LATEST_TAGGING=]

Versioning:
      --enable-versioning              Sync all version objects in the source storage to the target versioning storage.
                                         [env: ENABLE_VERSIONING=]
      --point-in-time <POINT_IN_TIME>  Sync only objects at a specific point in time (RFC3339 datetime).
                                       The source storage must be a versioning enabled S3 bucket.
                                       By default, the target storage's objects will always be overwritten with the source's objects.

                                       Example: 2025-07-16T12:00:00Z [env: POINT_IN_TIME=]

Encryption:
      --sse <SSE>
          Server-side encryption. Valid choices: AES256 | aws:kms | aws:kms:dsse [env: SSE=]
      --sse-kms-key-id <SSE_KMS_KEY_ID>
          SSE KMS ID key [env: SSE_KMS_KEY_ID=]
      --source-sse-c <SOURCE_SSE_C>
          Source SSE-C algorithm. Valid choices: AES256 [env: SOURCE_SSE_C=]
      --source-sse-c-key <SOURCE_SSE_C_KEY>
          Source SSE-C customer-provided encryption key(256bit key. must be base64 encoded) [env: SOURCE_SSE_C_KEY=]
      --source-sse-c-key-md5 <SOURCE_SSE_C_KEY_MD5>
          Source base64 encoded MD5 digest of source_sse_c_key [env: SOURCE_SSE_C_KEY_MD5=]
      --target-sse-c <TARGET_SSE_C>
          Target SSE-C algorithm. Valid choices: AES256 [env: TARGET_SSE_C=]
      --target-sse-c-key <TARGET_SSE_C_KEY>
          Target SSE-C customer-provided encryption key(256bit key. must be base64 encoded) [env: TARGET_SSE_C_KEY=]
      --target-sse-c-key-md5 <TARGET_SSE_C_KEY_MD5>
          Target base64 encoded MD5 digest of target-sse-c-key [env: TARGET_SSE_C_KEY_MD5=]

Reporting:
      --report-sync-status           Report sync status to the target storage.
                                     Default verification is for ETag. For additional checksum, use --check-additional-checksum.
                                     For more precise control, use with --auto-chunksize. [env: REPORT_SYNC_STATUS=]
      --report-metadata-sync-status  Report metadata sync status to the target storage.
                                     It must be used with --report-sync-status.
                                     Note: s3sync generated user-defined metadata(s3sync_origin_version_id/s3sync_origin_last_modified) were ignored.
                                           Because they are usually different from the source storage. [env: REPORT_METADATA_SYNC_STATUS=]
      --report-tagging-sync-status   Report tagging sync status to the target storage.
                                     It must be used with --report-sync-status. [env: REPORT_TAGGING_SYNC_STATUS=]

Tracing/Logging:
      --json-tracing           Show trace as json format [env: JSON_TRACING=]
      --aws-sdk-tracing        Enable aws sdk tracing [env: AWS_SDK_TRACING=]
      --span-events-tracing    Show span event tracing [env: SPAN_EVENTS_TRACING=]
      --disable-color-tracing  Disable ANSI terminal colors [env: DISABLE_COLOR_TRACING=]

Retry Options:
      --aws-max-attempts <max_attempts>
          Maximum retry attempts that s3sync retry handler use [env: AWS_MAX_ATTEMPTS=] [default: 10]
      --initial-backoff-milliseconds <initial_backoff>
          A multiplier value used when calculating backoff times as part of an exponential backoff with jitter strategy.
           [env: INITIAL_BACKOFF_MILLISECONDS=] [default: 100]
      --force-retry-count <FORCE_RETRY_COUNT>
          Maximum force retry attempts that s3sync retry handler use [env: FORCE_RETRY_COUNT=] [default: 5]
      --force-retry-interval-milliseconds <force_retry_interval>
          Sleep interval (milliseconds) between s3sync force retries on error.
           [env: FORCE_RETRY_INTERVAL_MILLISECONDS=] [default: 1000]

Timeout Options:
      --operation-timeout-milliseconds <operation_timeout>
          Operation timeout (milliseconds). For details, see the AWS SDK for Rust TimeoutConfig documentation.
          The default has no timeout. [env: OPERATION_TIMEOUT_MILLISECONDS=]
      --operation-attempt-timeout-milliseconds <operation_attempt_timeout>
          Operation attempt timeout (milliseconds). For details, see the AWS SDK for Rust TimeoutConfig documentation.
          The default has no timeout. [env: OPERATION_ATTEMPT_TIMEOUT_MILLISECONDS=]
      --connect-timeout-milliseconds <connect_timeout>
          Connect timeout (milliseconds).The default has AWS SDK default timeout (Currently 3100 milliseconds).
           [env: CONNECT_TIMEOUT_MILLISECONDS=]
      --read-timeout-milliseconds <read_timeout>
          Read timeout (milliseconds). The default has no timeout. [env: READ_TIMEOUT_MILLISECONDS=]

Advanced:
      --warn-as-error
          Treat warnings as errors(except for the case of ETag/checksum mismatch, etc.) [env: WARN_AS_ERROR=]
      --ignore-symlinks
          Ignore symbolic links [env: IGNORE_SYMLINKS=]
      --head-each-target
          HeadObject is used to check whether an object has been modified in the target storage.
          It reduces the possibility of race condition issue [env: HEAD_EACH_TARGET=]
      --acl <ACL>
          ACL for the objects
          Valid choices: private | public-read | public-read-write | authenticated-read | aws-exec-read |
                         bucket-owner-read | bucket-owner-full-control [env: ACL=]
      --no-guess-mime-type
          Do not try to guess the mime type of local file [env: NO_GUESS_MIME_TYPE=]
      --max-keys <MAX_KEYS>
          Maximum number of objects returned in a single list object request [env: MAX_KEYS=] [default: 1000]
      --put-last-modified-metadata
          Put last modified of the source to metadata [env: PUT_LAST_MODIFIED_METADATA=]
      --auto-complete-shell <SHELL>
          Generate a auto completions script.
          Valid choices: bash, fish, zsh, powershell, elvish. [env: AUTO_COMPLETE_SHELL=]
      --disable-stalled-stream-protection
          Disable stalled stream protection [env: DISABLE_STALLED_STREAM_PROTECTION=]
      --disable-payload-signing
          Disable payload signing for object uploads [env: DISABLE_PAYLOAD_SIGNING=]
      --disable-content-md5-header
          Disable Content-MD5 header for object uploads. It disables the ETag verification for the uploaded object.
           [env: DISABLE_CONTENT_MD5_HEADER=]
      --disable-express-one-zone-additional-checksum
          Disable default additional checksum verification in Express One Zone storage class.
            [env: DISABLE_EXPRESS_ONE_ZONE_ADDITIONAL_CHECKSUM=]
      --delete-excluded
          When used in combination with --delete options, supplied --filter-exclude-regex patterns will not prevent an object from being deleted.
           [env: DELETE_EXCLUDED=]
      --if-match
          Add an If-Match header for PutObject/CompleteMultipartUpload/DeleteObject requests.
          This is for like an optimistic lock. [env: IF_MATCH=]
      --copy-source-if-match
          Add an x-amz-copy-source-if-match header for CopyObject/UploadPartCopy requests.
          This is for like an optimistic lock. [env: COPY_SOURCE_IF_MATCH=]
      --if-none-match
          Uploads the object only if the object key name does not already exist in the specified bucket.
          This is for like an optimistic lock. [env: IF_NONE_MATCH=]
      --max-delete <MAX_DELETE>
          Don't delete more than a specified number of objects [env: MAX_DELETE=]
      --ignore-glacier-warnings
          Suppress warnings related to Amazon S3 Glacier storage class objects during GetObject requests [env: IGNORE_GLACIER_WARNINGS=]

Lua scripting support:
      --preprocess-callback-lua-script <PREPROCESS_CALLBACK_LUA_SCRIPT>
          Path to the Lua script that is executed as preprocess callback [env: PREPROCESS_CALLBACK_LUA_SCRIPT=]
      --event-callback-lua-script <EVENT_CALLBACK_LUA_SCRIPT>
          Path to the Lua script that is executed as event callback [env: EVENT_CALLBACK_LUA_SCRIPT=]
      --filter-callback-lua-script <FILTER_CALLBACK_LUA_SCRIPT>
          Path to the Lua script that is executed as filter callback [env: FILTER_CALLBACK_LUA_SCRIPT=]
      --allow-lua-os-library
          Allow Lua OS and I/O library functions in the Lua script. [env: ALLOW_LUA_OS_LIBRARY=]
      --lua-vm-memory-limit <LUA_VM_MEMORY_LIMIT>
          Memory limit for the Lua VM. Allow suffixes: KB, KiB, MB, MiB, GB, GiB.
          Zero means no limit.
          If the memory limit is exceeded, the whole process will be terminated. [env: LUA_VM_MEMORY_LIMIT=] [default: 64MiB]

Dangerous:
      --allow-lua-unsafe-vm  Allow unsafe Lua VM functions in the Lua script.
                             It allows the Lua script to load unsafe standard libraries or C modules. [env: ALLOW_LUA_UNSAFE_VM=]
      --delete               Delete objects that exist in the target but not in the source.
                             Exclude filters other than --filter-exclude-regex will not prevent an object from being deleted.
                             [Warning] Since this can cause data loss, test first with the --dry-run option.
                               [env: DELETE=]
$
```

</details>

## About testing

**Supported target: Amazon S3 only.**

Support for S3-compatible storage is on a best-effort basis and may behave differently.   
s3sync has been tested with Amazon S3. s3sync has many e2e tests and unit tests and runs every time when a new version
is released.  
S3-compatible storage is not tested when a new version is released (I test only when making major changes).  
This is because S3-compatible storage may have different behaviors and features.  
Since there is no official certification for S3-compatible storage, comprehensive testing is not possible.

## Contributing

While this project began as a personal hobby, it has been built with careful attention to production-quality standards.

- Suggestions and bug reports are welcome, but responses are not guaranteed.
- Pull requests for new features are generally not accepted, as they may conflict with the design philosophy.
- If you find this project useful, feel free to fork and modify it as you wish.

🔒 I consider this project to be “complete” and will maintain it only minimally going forward.  
However, I intend to keep the AWS SDK for Rust and other dependencies up to date monthly.