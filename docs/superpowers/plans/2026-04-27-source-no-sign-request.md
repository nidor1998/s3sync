# `--source-no-sign-request` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--source-no-sign-request` flag (and `SOURCE_NO_SIGN_REQUEST` env var) that issues unsigned, anonymous requests to the source S3 bucket — the AWS CLI's `--no-sign-request` semantics.

**Architecture:** A new `S3Credentials::NoSignRequest` variant on the existing public credential enum maps to the AWS SDK's `ConfigLoader::no_credentials()`. A new `bool` clap arg with `conflicts_with_all` for credential and request-payer flags drives the variant. A validation function rejects the combination with a local source.

**Tech Stack:** Rust 2024 edition, clap 4 derive macros, aws-config 1.8.16, aws-sdk-s3 1.131.0, tokio test harness.

**Spec:** [`docs/superpowers/specs/2026-04-27-source-no-sign-request-design.md`](../specs/2026-04-27-source-no-sign-request-design.md)

---

## File Structure

| File | Change |
|---|---|
| `src/types/mod.rs` | Add `NoSignRequest` variant to `S3Credentials` enum (line ~465). |
| `src/storage/s3/client_builder.rs` | New match arm in `load_config_credential` (line ~50). Update `build_region_provider` (line ~76). New unit test. |
| `src/config/args/mod.rs` | New const error string (top, ~line 132). New arg `source_no_sign_request` (line ~265). New branch in `build_client_configs` (line ~1592). New `check_no_sign_request_conflict` validator + wiring in `validate_storage_config`. |
| `src/config/args/tests/parse_credentials.rs` | New test asserting variant. |
| `src/config/args/tests/build_config.rs` | New test asserting `ClientConfig.credential`. |
| `src/config/args/tests/mod.rs` (or a new file) | Tests for the validation error and the clap conflict rejections. |
| `tests/source_no_sign_request.rs` | New `#![cfg(e2e_test)]` integration test. |
| `FULL_README.md` | Use-case section + AWS Configuration help block entry. |
| `README.md` | Mirror new flag in any AWS-flag listing. |
| `CHANGELOG.md` | Add entry under next version's `### Added`. |

---

## Conventions

- **Commit style:** Sentence case, no Conventional Commits prefix (matches recent git log: "Add X", "Refactor Y", "Fix Z").
- **Trailer:** Every commit ends with `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
- **CI gates** that must stay green: `cargo fmt --all --check`, `cargo clippy -- -D warnings`, `cargo deny -L error check`, `cargo test --all-features`.

---

### Task 1: Add `NoSignRequest` variant + wire match arms

Adding the variant breaks every exhaustive `match` on `S3Credentials`. The variant and the match arms must ship together so the codebase keeps compiling. We test-drive this end-to-end with a client-builder unit test that constructs the new variant.

**Files:**
- Modify: `src/types/mod.rs:465-469`
- Modify: `src/storage/s3/client_builder.rs:49-74` (`load_config_credential`)
- Modify: `src/storage/s3/client_builder.rs:76-98` (`build_region_provider`)
- Modify: `src/storage/s3/client_builder.rs` test module (append a new `#[tokio::test]`)

- [ ] **Step 1: Write the failing test (in `src/storage/s3/client_builder.rs`)**

Append inside `mod tests { ... }`, after the existing `create_client_from_credentials_with_default_region` test:

```rust
#[tokio::test]
async fn create_client_no_sign_request() {
    init_dummy_tracing_subscriber();

    let client_config = ClientConfig {
        client_config_location: ClientConfigLocation {
            aws_config_file: None,
            aws_shared_credentials_file: None,
        },
        credential: crate::types::S3Credentials::NoSignRequest,
        region: Some("us-east-1".to_string()),
        endpoint_url: None,
        force_path_style: false,
        retry_config: crate::config::RetryConfig {
            aws_max_attempts: 10,
            initial_backoff_milliseconds: 100,
        },
        cli_timeout_config: crate::config::CLITimeoutConfig {
            operation_timeout_milliseconds: None,
            operation_attempt_timeout_milliseconds: None,
            connect_timeout_milliseconds: None,
            read_timeout_milliseconds: None,
        },
        disable_stalled_stream_protection: false,
        request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
        parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
        accelerate: false,
        request_payer: None,
    };

    let client = client_config.create_client().await;

    assert_eq!(
        client.config().region().unwrap().to_string(),
        "us-east-1".to_string()
    );
}
```

- [ ] **Step 2: Run the test, expect compile failure**

Run: `cargo test --all-features --lib storage::s3::client_builder::tests::create_client_no_sign_request`
Expected: compile error — `no variant or associated item named 'NoSignRequest' found for enum 'S3Credentials'`.

- [ ] **Step 3: Add the variant**

In `src/types/mod.rs`, change the `S3Credentials` enum (currently at line 464–469):

```rust
#[derive(Debug, Clone)]
pub enum S3Credentials {
    Profile(String),
    Credentials { access_keys: AccessKeys },
    FromEnvironment,
    NoSignRequest,
}
```

- [ ] **Step 4: Run `cargo build --all-features`, expect non-exhaustive-match errors**

Expected: errors in `src/storage/s3/client_builder.rs::load_config_credential` and `::build_region_provider`. (Other match sites — `parse_credentials.rs`, `build_config.rs` — use `if let` and don't break.)

- [ ] **Step 5: Add the match arm in `load_config_credential`**

In `src/storage/s3/client_builder.rs`, replace the `match` body in `load_config_credential` (lines 50–72) with:

```rust
match &self.credential {
    crate::types::S3Credentials::Credentials { access_keys } => {
        let credentials = aws_sdk_s3::config::Credentials::new(
            access_keys.access_key.to_string(),
            access_keys.secret_access_key.to_string(),
            access_keys.session_token.clone(),
            None,
            "",
        );
        config_loader = config_loader.credentials_provider(credentials);
    }
    crate::types::S3Credentials::Profile(profile_name) => {
        let mut builder = aws_config::profile::ProfileFileCredentialsProvider::builder();

        if let Some(profile_files) = self.build_profile_files() {
            builder = builder.profile_files(profile_files)
        }

        config_loader =
            config_loader.credentials_provider(builder.profile_name(profile_name).build());
    }
    crate::types::S3Credentials::FromEnvironment => {}
    crate::types::S3Credentials::NoSignRequest => {
        config_loader = config_loader.no_credentials();
    }
}
```

- [ ] **Step 6: Update `build_region_provider` to treat `NoSignRequest` like `FromEnvironment`**

`NoSignRequest` has no profile to consult for a region, so it must use the SDK's default region chain. In `src/storage/s3/client_builder.rs`, replace the `provider_region` block (lines 86–95) with:

```rust
let provider_region = if matches!(
    &self.credential,
    crate::types::S3Credentials::FromEnvironment | crate::types::S3Credentials::NoSignRequest
) {
    RegionProviderChain::first_try(self.region.clone().map(Region::new))
        .or_default_provider()
} else {
    RegionProviderChain::first_try(self.region.clone().map(Region::new))
        .or_else(builder.build())
};
```

- [ ] **Step 7: Run the test, expect pass**

Run: `cargo test --all-features --lib storage::s3::client_builder::tests::create_client_no_sign_request`
Expected: `1 passed`.

- [ ] **Step 8: Run the full library test suite to confirm nothing broke**

Run: `cargo test --all-features --lib`
Expected: all pre-existing tests pass.

- [ ] **Step 9: Commit**

```bash
git add src/types/mod.rs src/storage/s3/client_builder.rs
git commit -m "$(cat <<'EOF'
Add S3Credentials::NoSignRequest variant for unsigned S3 requests

Wires the new variant to aws-config's ConfigLoader::no_credentials()
and treats it like FromEnvironment in the region-provider chain (no
profile to consult for a region).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Add the `--source-no-sign-request` CLI flag

Pure declarative addition — the field will be unused until Task 3 wires it into `build_client_configs`.

**Files:**
- Modify: `src/config/args/mod.rs:281-285` (insert immediately after `source_request_payer`).

- [ ] **Step 1: Add the arg field**

In `src/config/args/mod.rs`, insert this block immediately after the existing `source_request_payer` field (currently lines 279–281):

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

- [ ] **Step 2: Verify it compiles**

Run: `cargo build --all-features`
Expected: compile succeeds. `clap` may warn about unused field — that resolves in Task 3.

- [ ] **Step 3: Confirm `--help` advertises the flag**

Run: `cargo run --quiet --all-features -- --help 2>&1 | grep -A1 source-no-sign-request`
Expected: a line like `--source-no-sign-request  Do not sign requests for the source bucket (anonymous access for public buckets) [env: SOURCE_NO_SIGN_REQUEST=]`

- [ ] **Step 4: Commit**

```bash
git add src/config/args/mod.rs
git commit -m "$(cat <<'EOF'
Add --source-no-sign-request CLI flag

Declarative-only: the flag is unused until the credential-build branch
is wired in the next commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Wire the flag into `build_client_configs` and add a parse-level test

**Files:**
- Modify: `src/config/args/mod.rs:1592-1610` (`build_client_configs`, source branch).
- Modify: `src/config/args/tests/parse_credentials.rs` (append new test).

- [ ] **Step 1: Write the failing test (in `src/config/args/tests/parse_credentials.rs`)**

Append inside `mod tests { ... }` after `parse_from_args_both_access_keys`:

```rust
#[test]
fn parse_from_args_source_no_sign_request() {
    init_dummy_tracing_subscriber();

    let args = vec![
        "s3sync",
        "--source-no-sign-request",
        "s3://source-bucket",
        "s3://target-bucket",
    ];

    match parse_from_args(args) {
        Ok(config_args) => {
            let (source_config_result, _) =
                config_args.build_client_configs(RequestChecksumCalculation::WhenRequired);

            match source_config_result.unwrap().credential {
                S3Credentials::NoSignRequest => {}
                _ => {
                    // skipcq: RS-W1021
                    assert!(false, "expected NoSignRequest credential");
                }
            }
        }
        _ => {
            // skipcq: RS-W1021
            assert!(false, "error occurred.");
        }
    }
}
```

- [ ] **Step 2: Run the test, expect failure**

Run: `cargo test --all-features --lib config::args::tests::parse_credentials::tests::parse_from_args_source_no_sign_request`
Expected: assertion failure — the source credential will currently be `FromEnvironment` because the flag is not yet consumed.

- [ ] **Step 3: Wire the flag into `build_client_configs`**

In `src/config/args/mod.rs`, replace the source-credential branch (currently lines 1592–1610) with:

```rust
let source_credential = if self.source_no_sign_request {
    Some(S3Credentials::NoSignRequest)
} else if let Some(source_profile) = self.source_profile.clone() {
    Some(S3Credentials::Profile(source_profile))
} else if self.source_access_key.is_some() {
    self.source_access_key
        .clone()
        .map(|access_key| S3Credentials::Credentials {
            access_keys: AccessKeys {
                access_key,
                secret_access_key: self
                    .source_secret_access_key
                    .as_ref()
                    .unwrap()
                    .to_string(),
                session_token: self.source_session_token.clone(),
            },
        })
} else {
    Some(S3Credentials::FromEnvironment)
};
```

- [ ] **Step 4: Run the test, expect pass**

Run: `cargo test --all-features --lib config::args::tests::parse_credentials::tests::parse_from_args_source_no_sign_request`
Expected: `1 passed`.

- [ ] **Step 5: Run the full library test suite**

Run: `cargo test --all-features --lib`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/config/args/mod.rs src/config/args/tests/parse_credentials.rs
git commit -m "$(cat <<'EOF'
Map --source-no-sign-request to S3Credentials::NoSignRequest

Adds a priority branch in build_client_configs and an args-layer test
asserting the variant the flag produces.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add a `build_config` test asserting `ClientConfig.credential`

This locks in the end-to-end behavior at the `Config` level (one layer above `build_client_configs`).

**Files:**
- Modify: `src/config/args/tests/build_config.rs` (append).

- [ ] **Step 1: Write the failing test**

Append inside `mod tests { ... }`, modeled on `build_from_profile_with_default_value`:

```rust
#[test]
fn build_from_source_no_sign_request() {
    init_dummy_tracing_subscriber();

    let args = vec![
        "s3sync",
        "--source-no-sign-request",
        "s3://source-bucket/source_key",
        "s3://target-bucket/target_key",
    ];

    let config = build_config_from_args(args).unwrap();

    if let S3Credentials::NoSignRequest =
        &config.source_client_config.as_ref().unwrap().credential
    {
        // ok
    } else {
        // skipcq: RS-W1021
        assert!(false, "expected NoSignRequest source credential");
    }

    // Target side falls through to the default FromEnvironment.
    if let S3Credentials::FromEnvironment =
        &config.target_client_config.as_ref().unwrap().credential
    {
        // ok
    } else {
        // skipcq: RS-W1021
        assert!(false, "expected FromEnvironment target credential");
    }
}
```

- [ ] **Step 2: Run, expect pass (Task 3 already satisfies the assertion)**

Run: `cargo test --all-features --lib config::args::tests::build_config::tests::build_from_source_no_sign_request`
Expected: `1 passed`. The test exists to lock in this layer of the contract — failure here would indicate a regression.

If it fails: revisit Task 3 — `build_client_configs` is not setting the variant.

- [ ] **Step 3: Commit**

```bash
git add src/config/args/tests/build_config.rs
git commit -m "$(cat <<'EOF'
Test --source-no-sign-request reaches Config.source_client_config

Locks in the end-to-end mapping from CLI flag through to the resolved
ClientConfig credential.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Reject `--source-no-sign-request` with a local source

Anonymous credentials only make sense for an S3 source. Mirrors the existing `check_endpoint_url_conflict` (line 1307).

**Files:**
- Modify: `src/config/args/mod.rs:130` (add error constant after the existing endpoint-url constants).
- Modify: `src/config/args/mod.rs:941-981` (`validate_storage_config` — add call).
- Modify: `src/config/args/mod.rs` (insert new validator method, e.g. after `check_endpoint_url_conflict` at line ~1319).
- Modify: `src/config/args/tests/build_config.rs` (append).

- [ ] **Step 1: Write the failing validation test**

Append in `src/config/args/tests/build_config.rs`:

```rust
#[test]
fn build_from_source_no_sign_request_with_local_source_errors() {
    init_dummy_tracing_subscriber();

    let args = vec![
        "s3sync",
        "--source-no-sign-request",
        "/tmp/some_local_dir",
        "s3://target-bucket/target_key",
    ];

    let result = build_config_from_args(args);
    assert!(
        result.is_err(),
        "expected an error when --source-no-sign-request is paired with a local source",
    );

    let msg = result.err().unwrap();
    assert!(
        msg.contains("--source-no-sign-request"),
        "error message should mention the flag, got: {msg}",
    );
}
```

- [ ] **Step 2: Run the test, expect failure**

Run: `cargo test --all-features --lib config::args::tests::build_config::tests::build_from_source_no_sign_request_with_local_source_errors`
Expected: assertion failure (the build currently succeeds because no validation rejects this combination).

- [ ] **Step 3: Add the error constant**

In `src/config/args/mod.rs`, immediately after the existing `TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL` constant (line 131–132), add:

```rust
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SIGN_REQUEST: &str =
    "with --source-no-sign-request, source storage must be s3://\n";
```

- [ ] **Step 4: Add the validator method**

In `src/config/args/mod.rs`, immediately after `check_endpoint_url_conflict` (around line 1319), add:

```rust
fn check_no_sign_request_conflict(&self) -> Result<(), String> {
    if !self.source_no_sign_request {
        return Ok(());
    }

    let source = storage_path::parse_storage_path(&self.source);
    if matches!(source, StoragePath::Local(_)) {
        return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SIGN_REQUEST.to_string());
    }

    Ok(())
}
```

- [ ] **Step 5: Wire the validator into `validate_storage_config`**

In `src/config/args/mod.rs::validate_storage_config` (around line 961, immediately after `self.check_endpoint_url_conflict()?;`), add:

```rust
self.check_no_sign_request_conflict()?;
```

- [ ] **Step 6: Run the test, expect pass**

Run: `cargo test --all-features --lib config::args::tests::build_config::tests::build_from_source_no_sign_request_with_local_source_errors`
Expected: `1 passed`.

- [ ] **Step 7: Run full library test suite**

Run: `cargo test --all-features --lib`
Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/config/args/mod.rs src/config/args/tests/build_config.rs
git commit -m "$(cat <<'EOF'
Reject --source-no-sign-request when source is local storage

Anonymous credentials only apply to S3 sources. Mirrors the existing
check_endpoint_url_conflict validation pattern.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Test that clap rejects the credential conflicts

`conflicts_with_all` rejects the bad combinations declaratively. We add tests so the conflict list can't silently regress.

**Files:**
- Modify: `src/config/args/tests/build_config.rs` (append).

- [ ] **Step 1: Add the conflict tests**

Append in `src/config/args/tests/build_config.rs`:

```rust
#[test]
fn source_no_sign_request_conflicts_with_source_profile() {
    init_dummy_tracing_subscriber();

    let args = vec![
        "s3sync",
        "--source-no-sign-request",
        "--source-profile",
        "some_profile",
        "s3://source-bucket",
        "s3://target-bucket",
    ];

    assert!(parse_from_args(args).is_err());
}

#[test]
fn source_no_sign_request_conflicts_with_source_access_key() {
    init_dummy_tracing_subscriber();

    let args = vec![
        "s3sync",
        "--source-no-sign-request",
        "--source-access-key",
        "ak",
        "--source-secret-access-key",
        "sk",
        "s3://source-bucket",
        "s3://target-bucket",
    ];

    assert!(parse_from_args(args).is_err());
}

#[test]
fn source_no_sign_request_conflicts_with_source_request_payer() {
    init_dummy_tracing_subscriber();

    let args = vec![
        "s3sync",
        "--source-no-sign-request",
        "--source-request-payer",
        "s3://source-bucket",
        "s3://target-bucket",
    ];

    assert!(parse_from_args(args).is_err());
}
```

- [ ] **Step 2: Run the tests, expect pass**

Run: `cargo test --all-features --lib config::args::tests::build_config::tests::source_no_sign_request_conflicts`
Expected: 3 passed. (`clap`'s `conflicts_with_all` from Task 2 already produces these errors.)

If any test fails: the `conflicts_with_all` list on the arg in Task 2 is missing the corresponding entry — fix the list and rerun.

- [ ] **Step 3: Commit**

```bash
git add src/config/args/tests/build_config.rs
git commit -m "$(cat <<'EOF'
Test clap rejects --source-no-sign-request credential conflicts

Locks in the conflicts_with_all list so a future edit can't silently
allow ambiguous credential combinations.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: E2E test — anonymous read from a public bucket

Creates a temp bucket, makes it publicly readable (relaxes Block Public Access at the bucket level, then sets a public-read policy), uploads an object, runs `s3sync` with `--source-no-sign-request` to copy it to a local dir, and asserts the file matches.

> **Note on AWS-account requirements:** This test requires the test AWS account to permit public-read at the bucket level. If the account has org-wide Block Public Access enforced (SCP), the test will need to be skipped on that account. The implementation calls `PutPublicAccessBlock` with all four flags `false` to relax the bucket-level guard, then a public-read bucket policy. If your account-level Block Public Access is enabled, the `PutBucketPolicy` call will fail with `AccessDenied` — surface that as the test failure rather than silently passing.

**Files:**
- Create: `tests/source_no_sign_request.rs`
- Modify: `tests/common/mod.rs` (add helpers `disable_block_public_access` and `put_bucket_policy_public_read_get_object`).

- [ ] **Step 1: Add helpers to `tests/common/mod.rs`**

In `tests/common/mod.rs`, find the `GET_OBJECT_DENY_BUCKET_POLICY` constant (around line 123) and add immediately after it:

```rust
const GET_OBJECT_PUBLIC_READ_BUCKET_POLICY: &str = r#"{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::{{ bucket }}/*"
        },
        {
            "Sid": "PublicListBucket",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::{{ bucket }}"
        }
    ]
}"#;
```

Then, find `put_bucket_policy_deny_get_object` (around line 1908) and add immediately after that function:

```rust
pub async fn disable_block_public_access(&self, bucket: &str) {
    self.client
        .put_public_access_block()
        .bucket(bucket)
        .public_access_block_configuration(
            aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
                .block_public_acls(false)
                .ignore_public_acls(false)
                .block_public_policy(false)
                .restrict_public_buckets(false)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

pub async fn put_bucket_policy_public_read_get_object(&self, bucket: &str) {
    let policy = GET_OBJECT_PUBLIC_READ_BUCKET_POLICY.replace("{{ bucket }}", bucket);

    self.client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy)
        .send()
        .await
        .unwrap();
}
```

- [ ] **Step 2: Create the E2E test file**

Create `tests/source_no_sign_request.rs` with:

```rust
#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn source_no_sign_request_reads_public_bucket() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        helper.create_bucket(&bucket, REGION).await;
        helper.disable_block_public_access(&bucket).await;
        helper
            .put_object_with_metadata(&bucket, "data1", "./test_data/e2e_test/case1/data1")
            .await;
        helper
            .put_bucket_policy_public_read_get_object(&bucket)
            .await;

        {
            let source_bucket_url = format!("s3://{}", bucket);
            let args = vec![
                "s3sync",
                "--source-no-sign-request",
                "--source-region",
                REGION,
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error(), "anonymous source sync errored");

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);

            assert!(TestHelper::verify_file_md5_digest(
                "./test_data/e2e_test/case1/data1",
                &TestHelper::md5_digest(&format!("{}/data1", &download_dir)),
            ));
        }

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&download_dir);
    }
}
```

- [ ] **Step 3: Verify it compiles under the e2e cfg**

Run: `RUSTFLAGS="--cfg e2e_test" cargo build --all-features --tests`
Expected: build succeeds.

- [ ] **Step 4: Run the new E2E test (requires `s3sync-e2e-test` AWS profile + permissive account public-access settings)**

Run: `RUST_MIN_STACK=20000000 RUSTFLAGS="--cfg e2e_test" cargo test --all-features --test source_no_sign_request -- --nocapture --test-threads=1`
Expected: `1 passed`.

If the test fails on `PutBucketPolicy` with `AccessDenied`: the AWS account has account-level Block Public Access enabled. Disable it on the test account, or skip this test on that environment.

- [ ] **Step 5: Commit**

```bash
git add tests/source_no_sign_request.rs tests/common/mod.rs
git commit -m "$(cat <<'EOF'
Add e2e test for --source-no-sign-request against a public bucket

Creates a temp bucket, relaxes Block Public Access, uploads an object,
applies a public-read policy, then syncs it down anonymously. Asserts
the downloaded file matches the source.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Update `FULL_README.md`

Two edits: a use-case section near the existing "Specify profile" / "Specify region" entries, and a new line in the embedded `--help` output for AWS Configuration.

**Files:**
- Modify: `FULL_README.md` (around line 678–684 for the use case, around line 1242 for the help block).

- [ ] **Step 1: Add the use-case subsection**

Find the existing block (around line 678–683):

```markdown
### Specify region

```bash
s3sync --source-profile foo --source-region ap-northeast-1 s3://bucket-name1/prefix s3://bucket-name2/prefix
```
```

Immediately after the closing triple-backtick of that block, insert:

```markdown
### Anonymous access to a public source bucket

```bash
s3sync --source-no-sign-request s3://public-bucket-name/prefix /path/to/local
```

This issues unsigned requests for the source side, mirroring the AWS CLI's `--no-sign-request`. Conflicts with `--source-profile`, the source access-key flags, and `--source-request-payer`.
```

- [ ] **Step 2: Update the embedded AWS Configuration help block**

Find the AWS Configuration block (around line 1242–1250). Immediately after the `--target-session-token` entry, insert:

```
      --source-no-sign-request
          Do not sign requests for the source bucket (anonymous access for public buckets) [env: SOURCE_NO_SIGN_REQUEST=]
```

(Note: the help block is hand-maintained, not auto-generated.)

- [ ] **Step 3: Commit**

```bash
git add FULL_README.md
git commit -m "$(cat <<'EOF'
Document --source-no-sign-request in FULL_README

Adds a use-case section and the entry in the embedded AWS
Configuration help output.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Update `README.md` if it lists AWS-configuration flags

`README.md` may or may not enumerate flags. Check; mirror the `FULL_README.md` entry if it does.

**Files:**
- Possibly modify: `README.md`.

- [ ] **Step 1: Inspect README.md**

Run: `grep -n "source-profile\|AWS Configuration\|source-no-sign" README.md`

- [ ] **Step 2: If README.md lists AWS-config flags, mirror the new entry**

Insert the same line and prose used in Task 8, in the matching section.

- [ ] **Step 3: If no flag listing exists in README.md, skip — no change needed**

Document this decision in the commit if you skip:

```bash
# (skip)
```

- [ ] **Step 4: Commit (only if you modified README.md)**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
Document --source-no-sign-request in README

Mirrors the entry already added to FULL_README for users browsing on
crates.io.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Update `CHANGELOG.md`

The changelog uses Keep-a-Changelog conventions. The current top entry is `## [1.57.1] - 2026-03-28`. The branch is `update/v1.58.0` and `Cargo.toml` is at 1.58.0, so a `[1.58.0]` entry should already exist or needs adding.

**Files:**
- Modify: `CHANGELOG.md`.

- [ ] **Step 1: Check whether `## [1.58.0]` exists**

Run: `grep -n '\[1\.58\.0\]' CHANGELOG.md`

- [ ] **Step 2: If `[1.58.0]` exists, add an `### Added` line under it; otherwise create the section above the existing `[1.57.1]` block**

If the section exists and already has an `### Added` heading, add this line:

```markdown
- Added `--source-no-sign-request` flag for anonymous access to public source buckets
```

If `[1.58.0]` does not exist, insert above `## [1.57.1] - 2026-03-28` (use today's date, `2026-04-27`):

```markdown
## [1.58.0] - 2026-04-27

### Added

- Added `--source-no-sign-request` flag for anonymous access to public source buckets

```

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "$(cat <<'EOF'
Document --source-no-sign-request in CHANGELOG

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Final verification — fmt, clippy, full test suite, deny

CI gates: any one failing means the work isn't done.

- [ ] **Step 1: Format check**

Run: `cargo fmt --all --check --verbose`
Expected: clean exit.
If diffs print: run `cargo fmt --all` and amend the most recent commit (`git commit --amend --no-edit`) **only if the formatting fix is on files in that commit**; otherwise create a small follow-up commit `Run cargo fmt`.

- [ ] **Step 2: Clippy with warnings as errors**

Run: `cargo clippy --all-features --all-targets -- -D warnings`
Expected: clean exit. Fix any lint at the source (don't add `#[allow(...)]` unless the existing code does the same in similar contexts).

- [ ] **Step 3: cargo deny**

Run: `cargo deny -L error check`
Expected: clean exit. (No new dependencies were added by this change, so this should pass without action.)

- [ ] **Step 4: Full unit & integration test suite (no e2e cfg)**

Run: `cargo test --all-features`
Expected: all tests pass.

- [ ] **Step 5: Confirm the new help text ships**

Run: `cargo run --quiet --all-features -- --help 2>&1 | grep source-no-sign-request`
Expected: a line containing the flag and `[env: SOURCE_NO_SIGN_REQUEST=]`.

- [ ] **Step 6: Final commit (only if Step 1 produced fixes)**

```bash
git add -A
git commit -m "$(cat <<'EOF'
Run cargo fmt

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

If no fmt/clippy fixes were needed, this step is a no-op — skip it.

---

## Self-review notes

- **Spec coverage:** every section of the spec maps to a task — CLI surface (Task 2), data model (Task 1, 3), client builder (Task 1), validation (Task 5), tests (Tasks 1, 3, 4, 5, 6, 7), docs (Tasks 8, 9, 10).
- **No placeholders:** every code block is concrete; no "TBD", "similar to above", "add appropriate validation".
- **Type/name consistency:** `source_no_sign_request` (snake_case field), `--source-no-sign-request` (CLI), `S3Credentials::NoSignRequest` (variant), `SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SIGN_REQUEST` (error const), `check_no_sign_request_conflict` (validator) — used consistently across all tasks.
- **Ordering safety:** Task 1 ships variant + match arms together so the codebase compiles after each commit. Task 2's flag is unused but harmless. Task 3 wires it. Tasks 5–6 add validation and conflict tests. Tasks 7–10 are documentation/E2E and don't affect library consumers.
