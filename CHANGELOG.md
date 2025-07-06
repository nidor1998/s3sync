# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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