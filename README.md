# s3sync

[![Crates.io](https://img.shields.io/crates/v/s3sync.svg)](https://crates.io/crates/s3sync)
[![crates.io downloads](https://img.shields.io/crates/d/s3sync.svg)](https://crates.io/crates/s3sync)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![MSRV](https://img.shields.io/badge/msrv-1.86.0-red)
![CI](https://github.com/nidor1998/s3sync/actions/workflows/ci.yml/badge.svg?branch=main)
[![codecov](https://codecov.io/gh/nidor1998/s3sync/branch/main/graph/badge.svg?token=GO3DGS2BR4)](https://codecov.io/gh/nidor1998/s3sync)
[![DeepSource](https://app.deepsource.com/gh/nidor1998/s3sync.svg/?label=active+issues&show_trend=true&token=Q3EjeUmx8Fu-ndXKEG133W-t)](https://app.deepsource.com/gh/nidor1998/s3sync/?ref=repository-badge)

## Overview
s3sync is a reliable, very fast, and powerful synchronization tool for S3.

**This document is a summary of s3sync. For more detailed information, please refer to the [full README](https://github.com/nidor1998/s3sync/blob/main/FULL_README.md).**

## Who is this for?

s3sync is designed for users who need to synchronize data with S3 or S3-compatible object storage.  
This tool is specifically tailored for those who require reliable synchronization capabilities and get evidence of data integrity.

If you don't use s3sync for synchronization, you can use s3sync for checking the integrity of objects that have been transferred by other tools, such as AWS CLI, Rclone, s5cmd, and other S3 storage tools.  
In general, it makes sense to verify with other tools, if you want to ensure that the objects have been transferred correctly.

## As a Rust library
s3sync can be used as a Rust library.  
s3sync CLI is a very thin wrapper of the s3sync library. You can use all features of s3sync CLI in the library.

See [docs.rs](https://docs.rs/s3sync/latest/s3sync/) for more information.

## Features highlights

- Reliable: In-depth end-to-end object integrity check  
  s3sync calculates ETag(MD5 or equivalent) for each object and compares them with the ETag in the target.  
  An object that exists in the local disk is read from the disk and compared with the checksum in the source or target.  
  Optionally, s3sync can also calculate and compare additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME) for each object.  

  s3sync always shows the integrity check result, so you can verify that the synchronization was successful.

  ```bash
  $ s3sync --additional-checksum-algorithm SHA256 test_data s3://xxxxx
  772.00 KiB | 772.00 KiB/sec,  transferred 100 objects | 100 objects/sec,  etag verified 100 objects,  checksum verified 100 objects ...(omitted)
  ```
  
  `transferred 100 objects | 100 objects/sec,  etag verified 100 objects,  checksum verified 100 objects` means that all objects have been transferred and ETag(MD5 or equivalent) and additional checksum(SHA256 in this case) have been verified successfully.  
  If you want to get detailed evidence of the integrity check, you can use `Sync statistics report` feature(see below).

- Sync statistics report  
  s3sync can check and report the synchronization status at any time.  
  Sync statistics report feature supports objects that any tools have transferred, such as AWS CLI, Rclone, s5cmd, and other S3 storage tools.  
  And s3sync supports multipart upload and guesses chunk size automatically and verifies the integrity of the uploaded objects. (`--auto-chunksize` option).  
  `--auto-chunksize` option is useful for S3 compatible storage that does not support additional checksum.

  For example, If you want to know all the objects transferred by AWS CLI(of course, you can use s3sync) have been transferred correctly(checksum based), the following command will show the report.
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

  You can check the synchronization status of the object's tagging and metadata with `--report-metadata-sync-status` and `--report-tagging-sync-status` option.

- Easy to use  
  s3sync is designed to be easy to use.  
  s3sync has many options, but the default settings are reasonable for most cases of reliable synchronization.

  For example, In the IAM role environment, the following command will transfer all objects from the local directory to the S3 bucket and verify the integrity of the transferred objects using ETag(MD5 or equivalent).  
  If something goes wrong, s3sync will show a warning or error message, so you can understand what went wrong.

  ```bash
  s3sync /path/to/local s3://bucket-name/prefix
  ```

- Very fast  
  s3sync implemented in Rust, using AWS SDK for Rust that uses multithreaded asynchronous I/O.  
  In my environment(`c7a.large`, with 256 workers), Local to S3, about 3,900 objects/sec (small objects 10KiB),  
  in the case of large objects(6GiB object, total 96GiB, `--max-parallel-uploads 64`), about 280MB/sec, 5.5 minutes,

- Multiple ways
  - Local to S3(S3-compatible storage)
  - S3(S3-compatible storage) to Local
  - S3 to S3(cross-region, same-region, same-account, cross-account, from-to S3/S3-compatible storage)
  
- Multiple platforms support  
  Linux(x86_64, aarch64), macOS(aarch64), Windows(x86_64, aarch64) are fully tested and supported.  
  s3sync ia a single binary with no dependencies, so it can be easily run on the above platforms.

- Incremental transfer  
  There are many ways to transfer objects:
    - Modified time based(default)
    - Size-based
    - ETag(MD5 or equivalent) based
    - Additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME) based

- Flexible filtering  
  - key, `ContentType`, user-defined metadata, tagging, by regular expression.
  - size, modified time

- Versioning support  
  All versions of the object can be synchronized. (Except intermediate delete markers)

- Point-in-time snapshot  
  With versioning enabled S3 bucket, you can transfer objects at a specific point in time.

- Robust retry logic  
  For long time running operations, s3sync has a robust original retry logic in addition to AWS SDK's retry logic.

- [Lua](https://www.lua.org) scripting support   
  You can use Lua(5.4) script to implement custom filtering, event handling, preprocessing before transferring objects to S3.  
  `--preprocess-callback-lua-script`, `--event-callback-lua-script`, `--filter-callback-lua-script` options are available for this purpose.  
  Lua is generally recognized as a fast scripting language. Lua engine is embedded in s3sync, so you can use Lua script without any additional dependencies.  
  For example, you can use Lua script to implement custom preprocessing logic, such as dynamically modifying the object attributes(e.g., metadata, tagging) before transferring it to S3.  
  By default, Lua script run as safe mode, so it cannot use Lua os library functions.   
  If you want to allow more Lua libraries, you can use `--allow-lua-os-library`, `--allow-lua-unsafe-vm` option.  
  See [Lua script example](https://github.com/nidor1998/s3sync/tree/main/src/lua/script/)

- Amazon S3 Express One Zone(Directory bucket) support  
  s3sync can be used with [Amazon S3 Express one Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-Endpoints.html).

- CI/CD friendly  
  s3sync is designed to be used in CI/CD pipelines.
  - JSON tracing(logging) support(`--json-tracing` option)
  - Explicit exit code  
    `0` for success, `1` for error, `2` for invalid arguments, `3` for warnings(e.g., ETag mismatch).
  - Supports all options via environment variables

## More information
For more information, please refer to the [full README](https://github.com/nidor1998/s3sync/blob/main/FULL_README.md)

## Contributing

While this project began as a personal hobby, it has been built with careful attention to production-quality standards.

- Suggestions and bug reports are welcome, but responses are not guaranteed.
- Pull requests for new features are generally not accepted, as they may conflict with the design philosophy.
- If you find this project useful, feel free to fork and modify it as you wish.

🔒 I consider this project to be “complete” and will maintain it only minimally going forward.  
However, I intend to keep the AWS SDK for Rust and other dependencies up to date regularly.
