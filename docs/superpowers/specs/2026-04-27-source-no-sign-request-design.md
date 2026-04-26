# `--source-no-sign-request` design

**Date:** 2026-04-27
**Status:** approved (pending implementation)

## Goal

Add a `--source-no-sign-request` CLI flag (and `SOURCE_NO_SIGN_REQUEST` env var) that makes the source-side S3 client issue **unsigned**, anonymous requests. This matches the AWS CLI's `--no-sign-request` and is the standard way to pull objects from a public S3 bucket without configuring credentials.

Out of scope: a target-side equivalent (`--target-no-sign-request`). Anonymous writes against AWS S3 don't have a real-world use case; if one emerges later, the design extends symmetrically.

## CLI surface

One new boolean flag, declared in `src/config/args/mod.rs` alongside the other source credential options, in the **AWS Configuration** help group:

```rust
/// Do not sign requests for the source bucket (anonymous access for public buckets)
#[arg(
    long, env, default_value_t = false,
    conflicts_with_all = [
        "source_profile",
        "source_access_key",
        "source_secret_access_key",
        "source_session_token",
        "source_request_payer",
    ],
    help_heading = "AWS Configuration",
)]
source_no_sign_request: bool,
```

- The `env` attribute auto-derives `SOURCE_NO_SIGN_REQUEST`, consistent with every other arg.
- `conflicts_with_all` lets `clap` reject the combinations declaratively. The list covers every source-side credential option plus `--source-request-payer` (Requester-Pays demands a signed identity, so the combination is meaningless).

## Data model

`src/types/mod.rs`: add a fourth variant to `S3Credentials`:

```rust
#[derive(Debug, Clone)]
pub enum S3Credentials {
    Profile(String),
    Credentials { access_keys: AccessKeys },
    FromEnvironment,
    NoSignRequest,            // ← new
}
```

`src/config/args/mod.rs::build_client_configs` — the source-credential branch becomes:

```rust
let source_credential = if self.source_no_sign_request {
    Some(S3Credentials::NoSignRequest)
} else if let Some(source_profile) = self.source_profile.clone() {
    Some(S3Credentials::Profile(source_profile))
} else if self.source_access_key.is_some() {
    /* unchanged Credentials { access_keys: ... } construction */
} else {
    Some(S3Credentials::FromEnvironment)
};
```

The target-credential branch is untouched. `clap`'s conflict rules guarantee at most one of `{no_sign_request, profile, access_key}` is set; the `if/else if` ordering is just for tidiness, not for disambiguation.

Adding a variant breaks every exhaustive `match` on `S3Credentials`. That's intentional — the compiler enumerates each call site (`client_builder.rs` plus a handful of test helpers in `src/config/args/tests/`) and forces the author to handle the new mode rather than letting it fall through silently.

## Client builder

`src/storage/s3/client_builder.rs::load_config_credential` — add a new arm that calls the SDK's `ConfigLoader::no_credentials()`:

```rust
fn load_config_credential(&self, mut config_loader: ConfigLoader) -> ConfigLoader {
    match &self.credential {
        S3Credentials::Credentials { access_keys } => { /* unchanged */ }
        S3Credentials::Profile(profile_name)       => { /* unchanged */ }
        S3Credentials::FromEnvironment             => {}
        S3Credentials::NoSignRequest => {
            config_loader = config_loader.no_credentials();
        }
    }
    config_loader
}
```

`ConfigLoader::no_credentials()` (aws-config 1.8.16, `src/lib.rs:525`) explicitly disables credential resolution; the SDK then issues unsigned requests — the same semantics as the AWS CLI's `--no-sign-request`.

### Region provider

`build_region_provider` currently special-cases `S3Credentials::FromEnvironment` (uses the SDK's default region chain) versus anything else (chains a `ProfileFileRegionProvider`). With `NoSignRequest` there is no profile to read a region from, so it must follow the `FromEnvironment` path. Concretely:

```rust
let provider_region = if matches!(
    &self.credential,
    S3Credentials::FromEnvironment | S3Credentials::NoSignRequest,
) {
    RegionProviderChain::first_try(self.region.clone().map(Region::new))
        .or_default_provider()
} else {
    RegionProviderChain::first_try(self.region.clone().map(Region::new))
        .or_else(builder.build())
};
```

A user running anonymously will get region resolution from `--source-region` first, then `AWS_REGION` / `AWS_DEFAULT_REGION` / IMDS — never from a profile.

## Validation

`--source-no-sign-request` only makes sense when the source is an S3 path. Following the precedent set by `--source-endpoint-url` (which errors when the source is local — `src/config/args/mod.rs:1309`), add an analogous check:

```rust
if matches!(source, StoragePath::Local(_)) && self.source_no_sign_request {
    return Err(/* constant string */);
}
```

Add a matching constant near the existing `LOCAL_STORAGE_SPECIFIED` / `"with --source-endpoint-url, source storage must be s3://"` constants at the top of the file, e.g.:

```rust
const SOURCE_NO_SIGN_REQUEST_REQUIRES_S3: &str =
    "with --source-no-sign-request, source storage must be s3://\n";
```

That is the only new explicit validation. `clap`'s `conflicts_with_all` covers credential-mutual-exclusion and the request-payer conflict declaratively.

## Tests

**Unit tests** (`src/config/args/tests/`):

- `parse_credentials.rs`: assert that `--source-no-sign-request` produces `S3Credentials::NoSignRequest`. Use the same env-var unset discipline already established in that file.
- `build_config.rs`: assert the resulting `ClientConfig.credential` for `--source-no-sign-request`.
- A negative case for the section-4 validation: `s3sync --source-no-sign-request /tmp/src s3://b/` returns the expected `Err`.
- A negative case asserting that `clap` rejects `--source-no-sign-request --source-profile foo` (and the other conflicts).

**Client-builder tests** (`src/storage/s3/client_builder.rs`):

- A test that constructs a `ClientConfig { credential: NoSignRequest, ... }` and successfully calls `create_client().await`. Existing tests at lines 215+ are the template. The test exists to lock in that the new variant wires through without panicking and produces a valid `Client` — it does **not** assert that the wire request is unsigned (that's an SDK behavior we trust).

**E2E tests** (`#[cfg(e2e_test)]`):

- Add one test that lists or copies from a known public bucket using `--source-no-sign-request`. Match whatever public-bucket convention the existing E2E suite uses; if none exists, pick a small canonical AWS public bucket. Test runs only with `RUSTFLAGS="--cfg e2e_test"`, in line with the rest of the E2E suite.

## Documentation

- `FULL_README.md`: add a new use-case subsection near the existing "Specify profile" / "Specify region" entries (around line 670), e.g. "**Anonymous access to a public bucket**" with one example invocation. Also update the embedded `--help` dump (AWS Configuration block, around line 1230) by appending the new flag's entry — that block is hand-maintained, not auto-generated, so it needs an explicit edit.
- `README.md`: if it lists AWS-configuration flags, mirror the new entry there in the same format.
- `CHANGELOG.md`: add an entry under the next version's `### Added` heading following the existing Keep-a-Changelog style — e.g. `Added \`--source-no-sign-request\` flag for anonymous access to public source buckets`.

## Files touched

- `src/config/args/mod.rs` — new arg, conflict list, validation, branch in `build_client_configs`.
- `src/types/mod.rs` — new enum variant.
- `src/storage/s3/client_builder.rs` — match arm in `load_config_credential`, condition update in `build_region_provider`, new unit test.
- `src/config/args/tests/parse_credentials.rs` — new test cases.
- `src/config/args/tests/build_config.rs` — new test cases.
- `tests/` — one new E2E test (gated on `e2e_test` cfg).
- `FULL_README.md`, `README.md`, `CHANGELOG.md`.

No public library API removal; the new variant is additive on the existing public `S3Credentials` enum.
