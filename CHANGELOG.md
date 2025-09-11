# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.43.1] - 2025-09-11

### Changed
- Improved error handling in async functions
- Updated dependencies
- Updated docs

## [1.43.0] - 2025-09-10

### Changed
- Added proxy support  
  s3sync supports for proxy environment variables (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`).  
  s3sync automatically respects these proxy environment variables.
- [Breaking change] Removed `legacy_hyper014_feature`  
  This feature is no longer needed because AWS SDK for Rust natively supports proxy.
- aws-sdk-s3 = "1.105.0"
- Updated dependencies
- Updated docs

## [1.42.2] - 2025-09-08

### Changed
- Updated docs
- Updated dependencies


## [1.42.1] - 2025-09-07

### Changed
- Improved tests
- Changed the coverage analysis tool used from cargo-tarpaulin (ptrace) to cargo-llvm-cov  
  This is because recently analysis with cargo-tarpaulin (ptrace) started to fail with this binary.  
  But the LLVM-based coverage result is significantly different from the ptrace-based one.  
  LLVM-based coverage analysis ignores some codes regardless of their execution status.  
  So, the coverage percentage may be lower than before.
- Updated dependencies


## [1.42.0] - 2025-09-04

### Changed
- Added `--disable-additional-checksum-verify` option  
This option disables additional checksum verification after uploading to S3.   
But the hash of the object has been stored in the metadata of the object.
- Improve dry-run logic for some S3 compatible storage
- Updated dependencies


## [1.41.0] - 2025-08-31

### Changed
- Refined release build optimization settings  
  Setting lto = "fat" and strip = "symbols" in Cargo.toml for `release` build.  
  This significantly reduces the binary size at the cost of longer build time and higher memory usage during CLI binary release build.  
  And stripping symbols may make CLI binary debugging more difficult. (rarely needed)
- Updated dependencies

## [1.40.1] - 2025-08-30

### Changed
- Updated dependencies
- aws-sdk-s3 = "1.104.0"

### Fixed
- [Security] [tracing-subscriber 0.3.20](https://github.com/tokio-rs/tracing/releases/tag/tracing-subscriber-0.3.20)  
  ANSI Escape Sequence Injection (CVE-2025-58160)

## [1.40.0] - 2025-08-27

### Changed
- [Breaking change] `--delete-excluded` option  
  Earlier, `--delete` option deleted objects that are not in the source, ignoring exclude filtering options.  
  Now, `--filter-exclude-regex` filtering option is applied to `--delete` option.  
  Exclude filters other than `--filter-exclude-regex` will not prevent an object from being deleted.  
  If you want to delete objects that are not in the source, ignoring `--filter-exclude-regex`, use `--delete-excluded` option.

- Updated dependencies.

## [1.39.0] - 2025-08-25

### Changed
- Added `--max-parallel-listing-max-depth` option  
  By default, s3sync lists objects in the source and target buckets/local files in parallel up to the second level of subdirectories or prefixes.  
  And deeper levels are listed without parallelization.
  This is because parallel listing at deeper levels may not improve performance.  
  But in some cases, parallel listing at deeper levels may improve performance.
  You can configure the maximum depth of parallel listing workers with `--max-parallel-listing-max-depth` option.

- Updated dependencies.

## [1.38.0] - 2025-08-24

### Changed
- Support sync summary log  
  At the end of the sync, s3sync logs a summary of the sync statistics.  
  To log the summary, use `-v` option.
  ```bash
  $ s3sync --additional-checksum-algorithm SHA256 -v --json-tracing test_data s3://xxxxx |tail -1 |jq
  {
    "timestamp": "2025-08-24T08:36:35.034711Z",
    "level": "INFO",
    "fields": {
      "message": "sync summary",
      "transferred_byte": 464519168,
      "transferred_byte_per_sec": 77385200,
      "transferred_object": 40,
      "transferred_object_per_sec": 6,
      "etag_verified": 40,
      "checksum_verified": 40,
      "deleted": 0,
      "skipped": 0,
      "error": 0,
      "warning": 0,
      "duration_sec": 6.002687375
    }
  }
  $
  ```
- Updated dependencies.

## [1.37.0] - 2025-08-24

### Changed
- Support parallel object listing  
  By default, s3sync lists objects in the source and target buckets/local in parallel (default 16 workers).  
  The parallel listing is enabled when the root has subdirectories or prefixes.  
  For example, if the source is `s3://bucket-name/prefix` and there are many objects under `prefix/dir1`, `prefix/dir2`, ..., `prefix/dir16`, s3sync lists objects under these prefixes in parallel.
  But If the source has only one subdirectory under `prefix/`, s3sync does not list objects in parallel.
  
  You can configure the number of parallel listing workers with `--max-parallel-listings` option.  
  If set to `1`, parallel listing is disabled.

  This feature can significantly improve performance with incremental transfer when there are many objects in the source and target bucket/local.

  With express one zone storage class, parallel listing may return in progress multipart upload objects.   
  So, parallel listing is disabled by default when the source or target bucket uses express one zone storage class.   
  You can enable it with `--allow-parallel-listings-in-express-one-zone` option.  
  
  When `--enable-versioning` or `--point-in-time` option is specified, parallel listing is disabled.  
  Note: Parallel listing may use CPU and memory.

- Updated dependencies.

### Fixed
- Fixed a bug that does not log sync complete message.

## [1.36.0] - 2025-08-17

### Changed
- Support `SYNC_WRITE` event in the event callback system.  
  This event is triggered when an object (or part of an object) is written to a target.
- Support `SYNC_FILTERED` event in the event callback system.  
  If an object is filtered out, this event is triggered. EventData.message will contain the reason for filtering.
- Updated dependencies.

## [1.35.0] - 2025-08-15

### Changed
- Support Look-around with regular expressions.   
  Look-around features are invaluable for filtering objects with complex patterns.  
  For example, `'^(?!.*&test=true).*stage=first'` can be used to filter objects that do not contain `test=true` in the tag and contain `stage=first` in the tag.  
  And you can also create regular expressions that combine multiple logical conditions with look-around features.  
  This feature reduces the need for Lua scripts or custom callback Rust codes to filter objects with complex patterns.  
  Note: This feature has been implemented with [fancy-regex](https://github.com/fancy-regex/fancy-regex) crate, which falls back to regex crate if a given regex is not `fancy`.
- Updated documentation.
- Updated dependencies.
- aws-sdk-s3 = "1.103.0"

## [1.34.1] - 2025-08-14

### Changed
- Improved x86_64 linux performance in the highly loaded environment.
- Updated documentation.
- Updated dependencies.

### Fixed
- [Security] Fixed a bug that caused the `--allow-lua-os-library` is not enough to disable command execution in a Lua script.  
  Now, `--allow-lua-os-library` limits the use of Lua [Input and Output Facilities](https://www.lua.org/manual/5.4/manual.html#6.8).

## [1.34.0] - 2025-08-13

### Changed

- [Library] Lua scripting support  
  You can specify Lua scripting support CLI arguments in the library.
- aws-sdk-s3 = "1.102.0"
- Updated documentation.
- Updated dependencies.


## [1.33.0] - 2025-08-12

### Changed

- Lua scripting support  
  You can use Lua script to implement custom filtering, event handling, preprocessing before transferring objects to S3.  
  `--preprocess-callback-lua-script`, `--event-callback-lua-script`, `--filter-callback-lua-script` options are available for this purpose.  
  Lua is generally recognized as a fast scripting language. Lua engine is embedded in s3sync, so you can use Lua script without any additional dependencies.  
  For example, you can use Lua script to implement custom preprocessing logic, such as dynamically modifying the object attributes(e.g., metadata, tagging) before transferring it to S3.  
  By default, Lua script run as safe mode, so it cannot use Lua os library functions.   
  If you want to allow more Lua libraries, you can use `--allow-lua-os-library`, `--allow-lua-unsafe-vm` option.  
  See [Lua script example](https://github.com/nidor1998/s3sync/tree/main/src/lua/script/)
- Updated dependencies.

## [1.32.0] - 2025-08-10

### Changed

- Support a user defined callback while listing the source 
- Updated dependencies.


## [1.31.0] - 2025-08-08

### Changed

- Support Amazon S3 Access Points ARNs  
  Access Point alias is also supported (As before).
- aws-sdk-s3 = "1.101.0"
- Updated dependencies.


## [1.30.0] - 2025-08-06

### Changed

- Support user-defined event callback within s3sync CLI  
  If you are familiar with Rust, you can use `UserDefinedEventCallback` to implement custom event handling logic, such as logging or monitoring and custom actions before and after synchronization.  
  Thanks to Rust's clear compiler error messages and robust language features, even software engineers unfamiliar with the language can implement it easily.  
  To use `UserDefinedEventCallback`, you need to implement the `EventCallback` trait and rebuild the s3sync binary.


## [1.29.0] - 2025-08-04

### Changed

- Support a preprocess callback before uploading to S3 within the library  
  We can use this to modify the object attributes(e.g. metadata, tagging) dynamically before uploading to s3.  
  See [docs.rs](https://docs.rs/s3sync/latest/s3sync/) for more information.
- Updated dependencies.


## [1.28.0] - 2025-08-03

### Changed

- Support an event callback system within the library  
  See [docs.rs](https://docs.rs/s3sync/latest/s3sync/) for more information.
- Updated dependencies.

## [1.27.0] - 2025-07-27

### Changed

- Added Multi-Region Access Point support.
- aws-sdk-s3 = "1.100.0"
- Updated dependencies.


## [1.26.1] - 2025-07-21

### Changed

- Updated documentation.


## [1.26.0] - 2025-07-20

### Changed

- Rust Edition 2024 build
- Updated documentation.
- Updated dependencies.


## [1.25.2] - 2025-07-20

### Changed

- Updated help message.


## [1.25.1] - 2025-07-19

### Changed

- Updated documentation.
- MSRV = 1.86.0(From 1.25.0)


## [1.25.0] - 2025-07-18

### Changed

- Added `--report-sync-status` option.
- Added `--report-metadata-sync-status` option.
- Added `--report-tagging-sync-status` option.
- aws-sdk-s3 = "1.98.0"
- Updated dependencies.


## [1.24.0] - 2025-07-16

### Changed

- Added `--point-in-time` option.
- Updated dependencies.

### Fixed

- Fixed statistics issue when `--server-side-copy`.


## [1.23.0] - 2025-07-15

### Changed

- Added `--filter-include-content-type-regex` option.
- Added `--filter-exclude-content-type-regex` option.

## [1.22.1] - 2025-07-14

### Changed

- Improved a help message.
- Updated dependencies.

### Fixed
- Fixed the skip statistics issue when tag/metadata-based filtering is used.

## [1.22.0] - 2025-07-12

### Changed

- Added `--filter-include-metadata-regex` option.
- Added `--filter-exclude-metadata-regex` option.
- Added `--filter-include-tag-regex` option.
- Added `--filter-exclude-tag-regex` option.
- Added `--no-sync-system-metadata` option.
- Added `--no-sync-user-defined-metadata` option.
- Updated dependencies.

## [1.21.1] - 2025-07-12

### Changed

- Improved tests.

## [1.21.0] - 2025-07-11

### Changed

- Added `--operation-timeout-milliseconds` option.
- Added `--operation-attempt-timeout-milliseconds` option.
- Added `--connect-timeout-milliseconds` option.
- Added `--read-timeout-milliseconds` option.
- Improved socket error handling.

## [1.20.0] - 2025-07-11

### Changed

- Add help message grouping using help_heading in CLI  
  https://github.com/nidor1998/s3sync/issues/90
- updated dependencies.


## [1.19.0] - 2025-07-10

### Changed

- Added `--website-redirect` option.
- Improved tests.
- Refactored.
- updated dependencies.

### Fixed

- Fixed `--tagging` option bug that does not work with `--server-side-copy` option.


## [1.18.0] - 2025-07-09

### Changed

- [Breaking change] Distinct exit code for warnings. https://github.com/nidor1998/s3sync#cli-process-exit-codes
- Added `--server-side-copy` option.
- updated dependencies.

## [1.17.0] - 2025-07-06

### Changed

- Added `--check-mtime-and-etag` option.
- Added `--check-mtime-and-additional-checksum` option.
- Added `--source-request-payer`, `--target-request-payer` options.
- updated dependencies.


## [1.16.0] - 2025-07-03

### Changed

- [Breaking change]Disabled proxy support by default. https://github.com/nidor1998/s3sync/issues/78
- aws-sdk-s3 = "1.96.0"
- updated dependencies.
- MSRV = 1.85.0

### Fixed

- Fixed unnecessary etag check in dry-run mode.


## [1.15.0] - 2025-06-22

### Changed

- Added Amazon S3 Transfer Acceleration support.

## [1.14.0] - 2025-06-22

### Changed

- Multipart Upload/download large files from/to S3 in parallel
- aws-sdk-s3 = "1.93.0"
- updated dependencies.

### Fixed

- Fixed `--auto-chunksize` bug that warn E-tag mismatch.
- Fixed an issue where bandwidth restrictions did not function correctly during high loads, causing applications to freeze.

## [1.13.4] - 2025-06-08

### Changed

- Refactored. Removed unnecessary unwrap().
- aws-sdk-s3 = "1.91.0"
- updated dependencies.

## [1.13.3] - 2025-05-28

### Changed

- Improved tests.
- Refactored

## [1.13.2] - 2025-05-26

### Changed

- aws-sdk-s3 = "1.88.0"
- Refactored

## [1.13.1] - 2025-05-21

### Fixed
- fixed exit code when s3sync fails. https://github.com/nidor1998/s3sync/issues/68
- fixed latest version of clippy warnings.

### Changed

- MSRV = 1.82.0
- aws-sdk-s3 = "1.86.0"


## [1.13.0] - 2025-04-07

### Changed

Added default additional checksum verification for Express One Zone.

see https://github.com/nidor1998/s3sync/issues/65

### Changed

- aws-sdk-s3 = "1.82.0"

## [1.12.3] - 2025-03-27

### Changed

- Improved tests.
- Refactored


## [1.12.2] - 2025-03-24

### Changed

- Improved tests.
- Refactored

## [1.12.1] - 2025-03-17

### Changed

Improved e2e test.

## [1.12.0] - 2025-03-15

### Changed

Changed to using Hyper 1.x internally, Some features are not available in Hyper 1.x, so separated as `legacy_hyper014_feature` feature.

`--no-verify-ssl`, `--http-proxy` `--https-proxy` options require `legacy_hyper014_feature` feature.It is enabled by default.

see https://github.com/nidor1998/s3sync/issues/59.

## [1.11.0] - 2025-03-12

### Added

Support DSSE-KMS encryption with `--sse aws:kms:dsse` option.

### Changed

- aws-sdk-s3 = "1.79.0"

## [1.10.3] - 2025-03-08

### Fixed

- Fix: `--auto-chunksize` bug.　#55

### Changed

- aws-sdk-s3 = "1.76.0"

## [1.10.2] - 2025-02-14

### Changed

- Updated README.md

## [1.10.1] - 2025-02-13

### Fixed

- Fix: `--enable-additional-checksum` bug. #51

## [1.10.0] - 2025-02-10

### Added

Support CRC32/CRC32C full object checksum with `--full-object-checksum` option.

## [1.9.0] - 2025-02-07

### Added

Added `--disable-content-md5-header` option. It disables the ETag verification for the uploaded object.

### Changed

- aws-sdk-s3 = "1.74.0"

## [1.8.0] - 2025-02-05

### Added

- Added CRC64NVME checksum support.
  - Currently, Only CRC64NVME checksum is supported for full object checksum. 

### Changed

- aws-sdk-s3 = "1.72.0"

## [1.7.2] - 2025-01-25

### Changed

- aws-sdk-s3 = "1.71.0"

## [1.7.1] - 2024-12-25

### Fixed

- Fix conditional compilation of build script without version feature #42

## [1.7.0] - 2024-12-21

### Added

- Added disable payload signing `--disable-payload-signing` option.

### Changed

- aws-sdk-s3 = "1.66.0"
- Supported Rust Versions (MSRV) = 1.81.0

## [1.6.2] - 2024-11-23

### Changed

- aws-sdk-s3 = "1.62.0"


## [1.6.1] - 2024-09-23

### Changed

- aws-sdk-s3 = "1.51.0"
- Supported Rust Versions (MSRV) = 1.78.0


## [1.6.0] - 2024-07-20

### Fixed

- `--delete` option does not stop the pipeline after a failure
- `--disable-stalled-stream-protection` not working #32

### Changed

- aws-sdk-s3 = "1.41.0"


## [1.5.0] - 2024-06-15

### Added

- Added Additional checksum(SHA256/SHA1/CRC32/CRC32C) based incremental transfer `--check-additional-checksum` option.
- Added `get_errors_and_consume()` to `Pipeline` to get errors.

### Changed

- aws-sdk-s3 = "1.36.0"

## [1.4.0] - 2024-06-02

### Added

- Added HTTPS proxy authentication support.
- Added `--check-etag` and `--check-etag` & `--auto-chunksize` option.

### Changed

- aws-sdk-s3 = "1.32.0"

## [1.3.0] - 2024-04-28

### Added

- Added [Stalled-stream protection](https://github.com/awslabs/aws-sdk-rust/discussions/956) support(enabled by default).
- Added Express One Zone integration tests.

### Changed

- aws-sdk-s3 = "1.24.0"

## [1.2.0] - 2024-03-28

### Added

- Added Amazon S3 Express One Zone storage class support.

### Changed

- aws-sdk-s3 = "1.21.0"

## [1.1.0] - 2023-12-25

### Fixed

- fixed `--aws-sdk-tracing` bug.

### Changed

- aws-sdk-s3 = "1.11.0"

## [1.0.0] - 2023-11-28

### Changed

- Initial release.
- aws-sdk-s3 = "1.1.0" 