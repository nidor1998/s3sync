# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

s3sync is a Rust CLI tool and library for reliable S3 synchronization. It supports Local→S3, S3→Local, and S3→S3 transfers with integrity verification (ETag/MD5, SHA256, SHA1, CRC32, CRC32C, CRC64NVME). Edition 2024, MSRV 1.91.0.

## Build & Development Commands

```bash
# Build
cargo build                          # Debug build (all default features)
cargo build --release                # Release build
cargo build --no-default-features    # Without optional features (version, lua_support)

# Lint & Format
cargo fmt --all --check --verbose    # Check formatting
cargo clippy -- -D warnings          # Lint (warnings are errors)
cargo deny -L error check            # License/vulnerability audit

# Test
cargo test --all-features --verbose  # All unit tests

# Run a single test by name
cargo test <test_name> -- --nocapture

# E2E tests (require AWS credentials via `aws configure --profile s3sync-e2e-test`)
RUST_MIN_STACK=20000000 RUSTFLAGS="--cfg e2e_test" cargo test --all-features -- --nocapture --test-threads=1

# E2E with dangerous simulations
RUST_MIN_STACK=20000000 RUSTFLAGS="--cfg e2e_test --cfg e2e_test_dangerous_simulations" cargo test --all-features -- --nocapture --test-threads=1
```

## Architecture

### Pipeline-Based Data Flow

The core architecture is a multi-stage async pipeline connected via `async-channel`:

```
Lister → DiffLister → [Filters] → Syncer → Deleter → Terminator
```

1. **Lister** (`src/pipeline/lister.rs`) — enumerates source objects
2. **DiffLister** (`src/pipeline/diff_lister.rs`) — incremental sync by comparing source/target
3. **Diff Detectors** (`src/pipeline/diff_detector/`) — strategy pattern with 5 implementations: standard (mtime), etag, checksum, size, always-different
4. **Filters** (`src/pipeline/filter/`) — include/exclude by regex, size, time; plus user-defined filters
5. **Syncer** (`src/pipeline/syncer.rs`) — core sync logic (largest file ~128KB), handles download/upload, checksums, multipart
6. **Deleter** (`src/pipeline/deleter.rs`) — handles `--delete` mode
7. **Terminator** (`src/pipeline/terminator.rs`) — pipeline shutdown and stats reporting

### Storage Abstraction

`StorageTrait` (`src/storage/mod.rs`) abstracts storage backends with `DynClone` for trait object cloning:
- `src/storage/s3/` — S3 implementation (client creation, error handling)
- `src/storage/local/` — Local filesystem implementation

### Callback System

`src/callback/` provides three callback types, each with Lua and user-defined implementations:
- **Event callbacks** — pipeline lifecycle events (PIPELINE_START, PIPELINE_END, etc.)
- **Filter callbacks** — custom object filtering logic
- **Preprocess callbacks** — modify object metadata before upload

### Lua Scripting

Optional feature (`lua_support`). Uses MLua 0.11.6 with Lua 5.4 in safe mode (no OS/IO libs). Scripts in `src/lua/script/`.

### Configuration

`src/config/` — `Config` struct with 140+ fields, built from CLI args via `clap` with `TryFrom` conversion. Immutable after creation.

### Types

`src/types/` — core data types including `ObjectKey` (Local/S3), `S3syncObject` (polymorphic), `SyncStatsReport`, and security-sensitive types with `zeroize`.

## Feature Flags

- `version` (default) — embeds build metadata via `shadow-rs`
- `lua_support` (default) — enables Lua scripting via `mlua`

## Test Configuration

- `#[cfg(e2e_test)]` — E2E tests requiring AWS credentials
- `#[cfg(e2e_test_dangerous_simulations)]` — destructive E2E tests
- `#[cfg(coverage_nightly)]` — coverage exclusions
- `rusty_fork_test!` macro used for process isolation in some unit tests
- Integration tests in `tests/` require a shared semaphore to prevent concurrent S3 operations

## CI Targets

Builds and tests on: x86_64/aarch64 Linux (glibc + musl), macOS ARM64, Windows x86_64/ARM64.

## Exit Codes

0 = success, 1 = error, 2 = invalid args, 3 = warnings, 101 = abnormal termination.
