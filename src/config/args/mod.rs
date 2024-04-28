use std::ffi::OsString;
use std::path::PathBuf;
use std::str::FromStr;

use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ObjectCannedAcl, ServerSideEncryption, StorageClass,
};
use chrono::{DateTime, Utc};
use clap::builder::{ArgPredicate, NonEmptyStringValueParser};
use clap::Parser;
use clap_verbosity_flag::{Verbosity, WarnLevel};
use regex::Regex;
use shadow_rs::shadow;

use crate::config::args::value_parser::{
    canned_acl, checksum_algorithm, human_bytes, metadata, sse, storage_class, storage_path,
    tagging, url,
};
use crate::config::{
    ClientConfig, FilterConfig, ForceRetryConfig, RetryConfig, TracingConfig, TransferConfig,
};
use crate::types::{
    AccessKeys, ClientConfigLocation, S3Credentials, SseCustomerKey, SseKmsKeyId, StoragePath,
};
use crate::Config;

mod tests;
mod value_parser;

const DEFAULT_WORKER_SIZE: u16 = 16;
const DEFAULT_AWS_MAX_ATTEMPTS: u32 = 10;
const DEFAULT_FORCE_RETRY_COUNT: u32 = 5;
const DEFAULT_FORCE_RETRY_INTERVAL_MILLISECONDS: u64 = 1000;
const DEFAULT_INITIAL_BACKOFF_MILLISECONDS: u64 = 100;
const DEFAULT_JSON_TRACING: bool = false;
const DEFAULT_AWS_SDK_TRACING: bool = false;
const DEFAULT_SPAN_EVENTS_TRACING: bool = false;
const DEFAULT_DISABLE_COLOR_TRACING: bool = false;
const DEFAULT_MULTIPART_THRESHOLD: &str = "8MiB";
const DEFAULT_MULTIPART_CHUNKSIZE: &str = "8MiB";
const DEFAULT_AUTO_CHUNKSIZE: bool = false;
const DEFAULT_WARN_AS_ERROR: bool = false;
const DEFAULT_IGNORE_SYMLINKS: bool = false;
const DEFAULT_FORCE_PATH_STYLE: bool = false;
const DEFAULT_HEAD_EACH_TARGET: bool = false;
const DEFAULT_ENABLE_VERSIONING: bool = false;
const DEFAULT_REMOVE_MODIFIED_FILTER: bool = false;
const DEFAULT_CHECK_SIZE: bool = false;
const DEFAULT_SYNC_WITH_DELETE: bool = false;
const DEFAULT_DISABLE_TAGGING: bool = false;
const DEFAULT_SYNC_LATEST_TAGGING: bool = false;
const DEFAULT_NO_GUESS_MIME_TYPE: bool = false;
const DEFAULT_DISABLE_MULTIPART_VERIFY: bool = false;
const DEFAULT_DISABLE_ETAG_VERIFY: bool = false;
const DEFAULT_ENABLE_ADDITIONAL_CHECKSUM: bool = false;
const DEFAULT_DRY_RUN: bool = false;
const DEFAULT_NO_VERIFY_SSL: bool = false;
const DEFAULT_MAX_KEYS: i32 = 1000;
const DEFAULT_PUT_LAST_MODIFIED_METADATA: bool = false;
const DEFAULT_DISABLE_STALLED_STREAM_PROTECTION: bool = false;

const NO_S3_STORAGE_SPECIFIED: &str = "either SOURCE or TARGET must be s3://\n";
const LOCAL_STORAGE_SPECIFIED: &str =
    "with --enable-versioning/--sync-latest-tagging, both storage must be s3://\n";
const LOCAL_STORAGE_SPECIFIED_WITH_STORAGE_CLASS: &str =
    "with --storage-class, target storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_SSE: &str =
    "with --sse/--sse-kms-key-id, target storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ACL: &str = "with --acl, target storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENABLE_ADDITIONAL_CHECKSUM: &str =
    "with --enable-additional-checksum, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ADDITIONAL_CHECKSUM_ALGORITHM: &str =
    "with --additional-checksum-algorithm, target storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_AUTO_CHUNKSIZE: &str =
    "with --auto-chunksize, source storage must be s3://\n";
const SOURCE_REMOTE_STORAGE_SPECIFIED_WITH_IGNORE_SYMLINKS: &str =
    "with --ignore-symlinks, source storage must be local storage\n";
const SOURCE_REMOTE_STORAGE_SPECIFIED_WITH_NO_GUESS_MIME_TYPE: &str =
    "with --no-guess-mime-type, source storage must be local storage\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_METADATA_OPTION: &str =
    "with metadata related option, target storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL: &str =
    "with --source-endpoint-url, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL: &str =
    "with --target-endpoint-url, target storage must be s3://\n";
const CHECK_SIZE_CONFLICT: &str =
    "--head-each-target is required for --check-size, or remove --remove-modified-filter\n";
const SOURCE_LOCAL_STORAGE_DIR_NOT_FOUND: &str = "directory must be specified as a source\n";
const TARGET_LOCAL_STORAGE_INVALID: &str = "invalid target path\n";
const SSE_KMS_KEY_ID_ARGUMENTS_CONFLICT: &str =
    "--sse-kms-key-id must be used with --sse aws:kms\n";
const LOCAL_STORAGE_SPECIFIED_WITH_SSE_C: &str =
    "with --source-sse-c/--target-sse-c, remote storage must be s3://\n";

const NO_SOURCE_CREDENTIAL_REQUIRED: &str = "no source credential required\n";
const NO_TARGET_CREDENTIAL_REQUIRED: &str = "no target credential required\n";

shadow!(build);

#[derive(Parser, Clone, Debug)]
#[command(version=format!("{} ({} {}), {}", build::PKG_VERSION, build::SHORT_COMMIT, build::BUILD_TARGET, build::RUST_VERSION))]
pub struct CLIArgs {
    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix] or local path", value_parser = storage_path::check_storage_path, default_value_if("auto_complete_shell", ArgPredicate::IsPresent, "s3://ignored"), required = false)]
    source: String,

    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix] or local path", value_parser = storage_path::check_storage_path, default_value_if("auto_complete_shell", ArgPredicate::IsPresent, "s3://ignored"), required = false)]
    target: String,

    /// location of the file that the AWS CLI uses to store configuration profiles
    #[arg(long, env, value_name = "FILE")]
    aws_config_file: Option<PathBuf>,

    /// location of the file that the AWS CLI uses to store access keys
    #[arg(long, env, value_name = "FILE")]
    aws_shared_credentials_file: Option<PathBuf>,

    /// source AWS CLI profile
    #[arg(long, env, conflicts_with_all = ["source_access_key", "source_secret_access_key", "source_session_token"])]
    source_profile: Option<String>,

    /// source access key
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_secret_access_key")]
    source_access_key: Option<String>,

    /// source secret access key
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_access_key")]
    source_secret_access_key: Option<String>,

    /// source session token
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_access_key")]
    source_session_token: Option<String>,

    /// source region
    #[arg(long, env, value_parser = NonEmptyStringValueParser::new())]
    source_region: Option<String>,

    /// source endpoint url
    #[arg(long, env, value_parser = url::check_scheme)]
    source_endpoint_url: Option<String>,

    /// force path-style addressing for source endpoint
    #[arg(long, env, default_value_t = DEFAULT_FORCE_PATH_STYLE)]
    source_force_path_style: bool,

    /// target AWS CLI profile
    #[arg(long, env, conflicts_with_all = ["target_access_key", "target_secret_access_key", "target_session_token"])]
    target_profile: Option<String>,

    /// target access key
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_secret_access_key")]
    target_access_key: Option<String>,

    /// target secret access key
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key")]
    target_secret_access_key: Option<String>,

    /// target session token
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key")]
    target_session_token: Option<String>,

    /// target region
    #[arg(long, env, value_parser = NonEmptyStringValueParser::new())]
    target_region: Option<String>,

    /// target endpoint url
    #[arg(long, env, value_parser = url::check_scheme)]
    target_endpoint_url: Option<String>,

    /// force path-style addressing for target endpoint
    #[arg(long, env, default_value_t = DEFAULT_FORCE_PATH_STYLE)]
    target_force_path_style: bool,

    /// maximum retry attempts that s3sync retry handler use
    #[arg(long, env, default_value_t = DEFAULT_AWS_MAX_ATTEMPTS, value_name = "max_attempts")]
    aws_max_attempts: u32,

    /// a multiplier value used when calculating backoff times as part of an exponential backoff with jitter strategy.
    #[arg(long, env, default_value_t = DEFAULT_INITIAL_BACKOFF_MILLISECONDS, value_name = "initial_backoff")]
    initial_backoff_milliseconds: u64,

    /// maximum force retry attempts that s3sync retry handler use
    #[arg(long, env, default_value_t = DEFAULT_FORCE_RETRY_COUNT)]
    force_retry_count: u32,

    /// sleep interval (milliseconds) between s3sync force retries on error
    #[arg(long, env, default_value_t = DEFAULT_FORCE_RETRY_INTERVAL_MILLISECONDS, value_name = "force_retry_interval")]
    force_retry_interval_milliseconds: u64,

    /// trace verbosity(-v: show info, -vv: show debug, -vvv show trace)
    #[clap(flatten)]
    verbosity: Verbosity<WarnLevel>,

    /// show trace as json format
    #[arg(long, env, default_value_t = DEFAULT_JSON_TRACING)]
    json_tracing: bool,

    /// enable aws sdk tracing
    #[arg(long, env, default_value_t = DEFAULT_AWS_SDK_TRACING)]
    aws_sdk_tracing: bool,

    /// show span event tracing
    #[arg(long, env, default_value_t = DEFAULT_SPAN_EVENTS_TRACING)]
    span_events_tracing: bool,

    /// disable ANSI terminal colors
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_COLOR_TRACING)]
    disable_color_tracing: bool,

    /// object size threshold that s3sync uses for multipart upload, Allow suffixes: MB, MiB, GB, GiB.
    /// the larger the size, the larger the memory usage.
    #[arg(long, env, conflicts_with_all = ["auto_chunksize"], default_value = DEFAULT_MULTIPART_THRESHOLD, value_parser = human_bytes::check_human_bytes)]
    multipart_threshold: String,

    /// chunk size that s3sync uses for multipart upload of individual files, Allow suffixes: MB, MiB, GB, GiB.
    /// the larger the size, the larger the memory usage.
    #[arg(long, env, conflicts_with_all = ["auto_chunksize"], default_value = DEFAULT_MULTIPART_CHUNKSIZE, value_parser = human_bytes::check_human_bytes)]
    multipart_chunksize: String,

    /// automatically adjusts a chunk size to match the source. It takes extra HEAD requests(1 API call per part).
    #[arg(long, env, conflicts_with_all = ["multipart_threshold", "multipart_chunksize"], default_value_t = DEFAULT_AUTO_CHUNKSIZE)]
    auto_chunksize: bool,

    /// proxy server to use for HTTPS
    #[arg(long, env, value_parser = url::check_scheme)]
    https_proxy: Option<String>,

    /// proxy server to use for HTTP
    #[arg(long, env, value_parser = url::check_scheme)]
    http_proxy: Option<String>,

    /// number of workers for synchronization
    #[arg(long, env, default_value_t = DEFAULT_WORKER_SIZE, value_parser = clap::value_parser!(u16).range(1..))]
    worker_size: u16,

    /// treat warnings as errors(except for the case of etag/checksum mismatch, etc.)
    #[arg(long, env, default_value_t = DEFAULT_WARN_AS_ERROR)]
    warn_as_error: bool,

    /// ignore symbolic links
    #[arg(long, env, default_value_t = DEFAULT_IGNORE_SYMLINKS)]
    ignore_symlinks: bool,

    /// HeadObject is used to check whether an object has been modified in the target storage
    /// it reduces the possibility of race condition issue
    #[arg(long, env, conflicts_with_all = ["enable_versioning"], default_value_t = DEFAULT_HEAD_EACH_TARGET)]
    head_each_target: bool,

    /// sync all version objects in the source storage to the target versioning storage
    #[arg(long, env, conflicts_with_all = ["delete", "head_each_target", "remove_modified_filter"], default_value_t = DEFAULT_ENABLE_VERSIONING)]
    enable_versioning: bool,

    /// Cache-Control HTTP header to set on the target object
    #[arg(long, env)]
    cache_control: Option<String>,

    /// Content-Disposition HTTP header to set on the target object
    #[arg(long, env)]
    content_disposition: Option<String>,

    /// Content-Encoding HTTP header to set on the target object
    #[arg(long, env)]
    content_encoding: Option<String>,

    /// Content-Language HTTP header to set on the target object
    #[arg(long, env)]
    content_language: Option<String>,

    /// Content-Type HTTP header to set on the target object
    #[arg(long, env)]
    content_type: Option<String>,

    /// Expires HTTP header to set on the target object(RFC3339 datetime such as 2023-02-19T12:00:00Z)
    #[arg(long, env)]
    expires: Option<DateTime<Utc>>,

    /// metadata to set on the target object. e.g. --metadata "key1=value1,key2=value2".
    #[arg(long, env, value_parser = metadata::check_metadata)]
    metadata: Option<String>,

    /// tagging to set on the target object. e.g. --tagging "key1=value1&key2=value2". must be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.
    #[arg(long, env, conflicts_with_all = ["disable_tagging", "sync_latest_tagging"], value_parser = tagging::parse_tagging)]
    tagging: Option<String>,

    /// sync only objects older than given time (RFC3339 datetime such as 2023-02-19T12:00:00Z)
    #[arg(long, env)]
    filter_mtime_before: Option<DateTime<Utc>>,

    /// sync only objects newer than OR EQUAL TO given time (RFC3339 datetime such as 2023-02-19T12:00:00Z)
    #[arg(long, env)]
    filter_mtime_after: Option<DateTime<Utc>>,

    /// sync only objects that match given regular expression
    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex)]
    filter_include_regex: Option<String>,

    /// do not sync objects that match given regular expression
    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex)]
    filter_exclude_regex: Option<String>,

    /// sync only objects smaller than given size, Allow suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB
    #[arg(long, env, value_parser = human_bytes::check_human_bytes_without_limit)]
    filter_smaller_size: Option<String>,

    /// sync only objects larger than OR EQUAL TO given size, Allow suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB
    #[arg(long, env, value_parser = human_bytes::check_human_bytes_without_limit)]
    filter_larger_size: Option<String>,

    /// do not check(ListObjectsV2) for modification in the target storage
    #[arg(long, env, conflicts_with_all = ["enable_versioning"], default_value_t = DEFAULT_REMOVE_MODIFIED_FILTER)]
    remove_modified_filter: bool,

    /// use object size for update checking
    #[arg(long, env, conflicts_with_all = ["enable_versioning"], default_value_t = DEFAULT_CHECK_SIZE)]
    check_size: bool,

    /// delete objects that exist in the target but not in the source.
    /// [Warning] Since this can cause data loss, test first with the --dry-run option
    #[arg(long, env, conflicts_with_all = ["enable_versioning"], default_value_t = DEFAULT_SYNC_WITH_DELETE)]
    delete: bool,

    /// do not copy tagging.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_TAGGING)]
    disable_tagging: bool,

    /// copy the latest tagging from the source if necessary. If this option is enabled, the --remove-modified-filter and --head-each-target options are automatically enabled.
    #[arg(long, env, conflicts_with_all = ["enable_versioning", "disable_tagging"], default_value_t = DEFAULT_SYNC_LATEST_TAGGING)]
    sync_latest_tagging: bool,

    /// type of storage to use for the target object.
    /// valid choices: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONE-ZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | GLACIER_IR | EXPRESS_ONEZONE
    #[arg(long, env, value_parser = storage_class::parse_storage_class)]
    storage_class: Option<String>,

    /// server-side encryption. valid choices: AES256 | aws:kms
    #[arg(long, env, value_parser = sse::parse_sse)]
    sse: Option<String>,

    /// SSE KMS ID key
    #[arg(long, env)]
    sse_kms_key_id: Option<String>,

    /// source SSE-C algorithm. valid choices: AES256
    #[arg(long, env, conflicts_with_all = ["sse", "sse_kms_key_id"], requires = "source_sse_c_key", value_parser = sse::parse_sse_c)]
    source_sse_c: Option<String>,

    /// source SSE-C customer-provided encryption key(256bit key. must be base64 encoded)
    #[arg(long, env, requires = "source_sse_c_key_md5")]
    source_sse_c_key: Option<String>,

    /// source base64 encoded MD5 digest of source_sse_c_key
    #[arg(long, env, requires = "source_sse_c")]
    source_sse_c_key_md5: Option<String>,

    /// target SSE-C algorithm. valid choices: AES256
    #[arg(long, env, conflicts_with_all = ["sse", "sse_kms_key_id"], requires = "target_sse_c_key", value_parser = sse::parse_sse_c)]
    target_sse_c: Option<String>,

    /// target SSE-C customer-provided encryption key(256bit key. must be base64 encoded)
    #[arg(long, env, requires = "target_sse_c_key_md5")]
    target_sse_c_key: Option<String>,

    /// target base64 encoded MD5 digest of source-sse-c-key
    #[arg(long, env, requires = "target_sse_c")]
    target_sse_c_key_md5: Option<String>,

    /// ACL for the objects
    /// valid choices: private | public-read | public-read-write | authenticated-read | aws-exec-read | bucket-owner-read | bucket-owner-full-control
    #[arg(long, env, value_parser = canned_acl::parse_canned_acl)]
    acl: Option<String>,

    /// do not try to guess the mime type of local file
    #[arg(long, env, default_value_t = DEFAULT_NO_GUESS_MIME_TYPE)]
    no_guess_mime_type: bool,

    /// disable multipart upload verification with ETag/additional checksum
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_MULTIPART_VERIFY)]
    disable_multipart_verify: bool,

    /// disable etag verification
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_ETAG_VERIFY)]
    disable_etag_verify: bool,

    /// additional checksum algorithm for upload
    #[arg(long, env, value_parser = checksum_algorithm::parse_checksum_algorithm)]
    additional_checksum_algorithm: Option<String>,

    /// enable additional checksum for download
    #[arg(long, env, default_value_t = DEFAULT_ENABLE_ADDITIONAL_CHECKSUM)]
    enable_additional_checksum: bool,

    /// A simulation mode. no actions will be performed
    #[arg(long, env, default_value_t = DEFAULT_DRY_RUN)]
    dry_run: bool,

    /// rate limit objects per second
    #[arg(long, env,  value_parser = clap::value_parser!(u32).range(10..))]
    rate_limit_objects: Option<u32>,

    /// rate limit bandwidth(bytes per sec). Allow suffixes: MB, MiB, GB, GiB
    #[arg(long, env, value_parser = human_bytes::check_human_bandwidth)]
    rate_limit_bandwidth: Option<String>,

    /// [dangerous] disable to verify SSL certificates.
    #[arg(long, env, conflicts_with_all = ["https_proxy", "http_proxy"], default_value_t = DEFAULT_NO_VERIFY_SSL)]
    no_verify_ssl: bool,

    /// maximum number of objects returned in a single list object request
    #[arg(long, env, default_value_t = DEFAULT_MAX_KEYS, value_parser = clap::value_parser!(i32).range(1..=32767))]
    max_keys: i32,

    /// put last modified of the source to metadata
    #[arg(long, env, default_value_t = DEFAULT_PUT_LAST_MODIFIED_METADATA)]
    put_last_modified_metadata: bool,

    /// unit test purpose only
    #[arg(long, hide = true, default_value_t = false)]
    allow_both_local_storage: bool,

    /// generate a auto completions script. Valid values: bash, fish, zsh, powershell, elvish.
    #[arg(long, env, value_name = "SHELL", value_parser = clap_complete::shells::Shell::from_str)]
    auto_complete_shell: Option<clap_complete::shells::Shell>,

    /// disable stalled stream protection
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_STALLED_STREAM_PROTECTION)]
    disable_stalled_stream_protection: bool,
}

pub fn parse_from_args<I, T>(args: I) -> Result<CLIArgs, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    CLIArgs::try_parse_from(args)
}

pub fn build_config_from_args<I, T>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let config_args = CLIArgs::try_parse_from(args).map_err(|e| e.to_string())?;
    crate::Config::try_from(config_args)
}

impl CLIArgs {
    fn validate_storage_config(&self) -> Result<(), String> {
        self.check_source_local_storage()?;
        self.check_target_local_storage()?;
        self.check_storage_conflict()?;
        self.check_versioning_option_conflict()?;
        self.check_tagging_option_conflict()?;
        self.check_storage_class_conflict()?;
        self.check_storage_credentials_conflict()?;
        self.check_sse_conflict()?;
        self.check_sse_c_conflict()?;
        self.check_acl_conflict()?;
        self.check_enable_additional_checksum_conflict()?;
        self.check_additional_checksum_algorithm_conflict()?;
        self.check_auto_chunksize_conflict()?;
        self.check_metadata_conflict()?;
        self.check_check_size_conflict()?;
        self.check_ignore_symlinks_conflict()?;
        self.check_no_guess_mime_type_conflict()?;
        self.check_endpoint_url_conflict()?;

        Ok(())
    }

    fn check_source_local_storage(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);

        if let StoragePath::Local(path) = source {
            if !path.is_dir() {
                return Err(SOURCE_LOCAL_STORAGE_DIR_NOT_FOUND.to_string());
            }
        }

        Ok(())
    }

    fn check_target_local_storage(&self) -> Result<(), String> {
        let target = storage_path::parse_storage_path(&self.target);

        if let StoragePath::Local(path) = target {
            let exist_result = path.try_exists();
            if exist_result.is_err() {
                return Err(TARGET_LOCAL_STORAGE_INVALID.to_string());
            }
        }
        Ok(())
    }

    fn check_storage_conflict(&self) -> Result<(), String> {
        if self.allow_both_local_storage {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        let target = storage_path::parse_storage_path(&self.target);

        if storage_path::is_both_storage_local(&source, &target) {
            return Err(NO_S3_STORAGE_SPECIFIED.to_string());
        }

        Ok(())
    }

    fn check_versioning_option_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        let target = storage_path::parse_storage_path(&self.target);

        if self.enable_versioning && !storage_path::is_both_storage_s3(&source, &target) {
            return Err(LOCAL_STORAGE_SPECIFIED.to_string());
        }

        Ok(())
    }

    fn check_tagging_option_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        let target = storage_path::parse_storage_path(&self.target);

        if self.sync_latest_tagging && !storage_path::is_both_storage_s3(&source, &target) {
            return Err(LOCAL_STORAGE_SPECIFIED.to_string());
        }

        Ok(())
    }

    fn check_storage_class_conflict(&self) -> Result<(), String> {
        let target = storage_path::parse_storage_path(&self.target);

        if self.storage_class.is_some() && matches!(target, StoragePath::Local(_)) {
            return Err(LOCAL_STORAGE_SPECIFIED_WITH_STORAGE_CLASS.to_string());
        }

        Ok(())
    }

    fn check_storage_credentials_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        let target = storage_path::parse_storage_path(&self.target);

        if matches!(source, StoragePath::Local(_))
            && (self.source_profile.is_some() || self.source_access_key.is_some())
        {
            return Err(NO_SOURCE_CREDENTIAL_REQUIRED.to_string());
        }

        if matches!(target, StoragePath::Local(_))
            && (self.target_profile.is_some() || self.target_access_key.is_some())
        {
            return Err(NO_TARGET_CREDENTIAL_REQUIRED.to_string());
        }

        Ok(())
    }

    fn check_sse_conflict(&self) -> Result<(), String> {
        if self.sse.is_none() && self.sse_kms_key_id.is_none() {
            return Ok(());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_SSE.to_string());
        }

        if self.sse_kms_key_id.is_some()
            && (self.sse.is_none()
                || ServerSideEncryption::from_str(self.sse.as_ref().unwrap()).unwrap()
                    != ServerSideEncryption::AwsKms)
        {
            return Err(SSE_KMS_KEY_ID_ARGUMENTS_CONFLICT.to_string());
        }

        Ok(())
    }

    fn check_sse_c_conflict(&self) -> Result<(), String> {
        if self.source_sse_c.is_none() && self.target_sse_c.is_none() {
            return Ok(());
        }

        if self.source_sse_c.is_some() {
            let source = storage_path::parse_storage_path(&self.source);
            if matches!(source, StoragePath::Local(_)) {
                return Err(LOCAL_STORAGE_SPECIFIED_WITH_SSE_C.to_string());
            }
        }

        if self.target_sse_c.is_some() {
            let target = storage_path::parse_storage_path(&self.target);
            if matches!(target, StoragePath::Local(_)) {
                return Err(LOCAL_STORAGE_SPECIFIED_WITH_SSE_C.to_string());
            }
        }

        Ok(())
    }

    fn check_acl_conflict(&self) -> Result<(), String> {
        if self.acl.is_none() {
            return Ok(());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ACL.to_string());
        }

        Ok(())
    }

    fn check_additional_checksum_algorithm_conflict(&self) -> Result<(), String> {
        if self.additional_checksum_algorithm.is_none() {
            return Ok(());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(
                TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ADDITIONAL_CHECKSUM_ALGORITHM.to_string(),
            );
        }

        Ok(())
    }

    fn check_enable_additional_checksum_conflict(&self) -> Result<(), String> {
        if !self.enable_additional_checksum {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENABLE_ADDITIONAL_CHECKSUM.to_string());
        }

        Ok(())
    }

    fn check_auto_chunksize_conflict(&self) -> Result<(), String> {
        if !self.auto_chunksize {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);

        if matches!(source, StoragePath::Local(_)) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_AUTO_CHUNKSIZE.to_string());
        }

        Ok(())
    }

    fn check_metadata_conflict(&self) -> Result<(), String> {
        if self.cache_control.is_none()
            && self.content_disposition.is_none()
            && self.content_encoding.is_none()
            && self.content_language.is_none()
            && self.content_type.is_none()
            && self.expires.is_none()
            && self.tagging.is_none()
            && !self.put_last_modified_metadata
        {
            return Ok(());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_METADATA_OPTION.to_string());
        }

        Ok(())
    }

    fn check_check_size_conflict(&self) -> Result<(), String> {
        if self.check_size && self.remove_modified_filter && !self.head_each_target {
            return Err(CHECK_SIZE_CONFLICT.to_string());
        }

        Ok(())
    }

    fn check_ignore_symlinks_conflict(&self) -> Result<(), String> {
        if !self.ignore_symlinks {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::S3 { .. }) {
            return Err(SOURCE_REMOTE_STORAGE_SPECIFIED_WITH_IGNORE_SYMLINKS.to_string());
        }

        Ok(())
    }

    fn check_no_guess_mime_type_conflict(&self) -> Result<(), String> {
        if !self.no_guess_mime_type {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::S3 { .. }) {
            return Err(SOURCE_REMOTE_STORAGE_SPECIFIED_WITH_NO_GUESS_MIME_TYPE.to_string());
        }

        Ok(())
    }

    fn check_endpoint_url_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.source_endpoint_url.is_some() {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL.to_string());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) && self.target_endpoint_url.is_some() {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL.to_string());
        }

        Ok(())
    }

    fn build_client_configs(&self) -> (Option<ClientConfig>, Option<ClientConfig>) {
        let source_credential = if let Some(source_profile) = self.source_profile.clone() {
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

        let target_credential = if let Some(target_profile) = self.target_profile.clone() {
            Some(S3Credentials::Profile(target_profile))
        } else if self.target_access_key.is_some() {
            self.target_access_key
                .clone()
                .map(|access_key| S3Credentials::Credentials {
                    access_keys: AccessKeys {
                        access_key,
                        secret_access_key: self
                            .target_secret_access_key
                            .as_ref()
                            .unwrap()
                            .to_string(),
                        session_token: self.target_session_token.clone(),
                    },
                })
        } else {
            Some(S3Credentials::FromEnvironment)
        };

        let source_client_config = source_credential.map(|source_credential| ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: self.aws_config_file.clone(),
                aws_shared_credentials_file: self.aws_shared_credentials_file.clone(),
            },
            credential: source_credential,
            region: self.source_region.clone(),
            endpoint_url: self.source_endpoint_url.clone(),
            force_path_style: self.source_force_path_style,
            retry_config: RetryConfig {
                aws_max_attempts: self.aws_max_attempts,
                initial_backoff_milliseconds: self.initial_backoff_milliseconds,
            },
            https_proxy: self.https_proxy.clone(),
            http_proxy: self.http_proxy.clone(),
            no_verify_ssl: self.no_verify_ssl,
            disable_stalled_stream_protection: self.disable_stalled_stream_protection,
        });

        let target_client_config = target_credential.map(|target_credential| ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: self.aws_config_file.clone(),
                aws_shared_credentials_file: self.aws_shared_credentials_file.clone(),
            },
            credential: target_credential,
            region: self.target_region.clone(),
            endpoint_url: self.target_endpoint_url.clone(),
            force_path_style: self.target_force_path_style,
            retry_config: RetryConfig {
                aws_max_attempts: self.aws_max_attempts,
                initial_backoff_milliseconds: self.initial_backoff_milliseconds,
            },
            https_proxy: self.https_proxy.clone(),
            http_proxy: self.http_proxy.clone(),
            no_verify_ssl: self.no_verify_ssl,
            disable_stalled_stream_protection: self.disable_stalled_stream_protection,
        });

        (source_client_config, target_client_config)
    }
}

impl TryFrom<CLIArgs> for Config {
    type Error = String;

    fn try_from(value: CLIArgs) -> Result<Self, Self::Error> {
        value.validate_storage_config()?;

        let (source_client_config, target_client_config) = value.build_client_configs();

        let mut tracing_config = value.verbosity.log_level().map(|log_level| TracingConfig {
            tracing_level: log_level,
            json_tracing: value.json_tracing,
            aws_sdk_tracing: value.aws_sdk_tracing,
            span_events_tracing: value.span_events_tracing,
            disable_color_tracing: value.disable_color_tracing,
        });

        if value.dry_run {
            if tracing_config.is_none() {
                tracing_config = Some(TracingConfig {
                    tracing_level: log::Level::Info,
                    json_tracing: DEFAULT_JSON_TRACING,
                    aws_sdk_tracing: DEFAULT_AWS_SDK_TRACING,
                    span_events_tracing: DEFAULT_SPAN_EVENTS_TRACING,
                    disable_color_tracing: DEFAULT_DISABLE_COLOR_TRACING,
                });
            } else if tracing_config.unwrap().tracing_level < log::Level::Info {
                tracing_config = Some(TracingConfig {
                    tracing_level: log::Level::Info,
                    json_tracing: tracing_config.unwrap().json_tracing,
                    aws_sdk_tracing: tracing_config.unwrap().aws_sdk_tracing,
                    span_events_tracing: tracing_config.unwrap().span_events_tracing,
                    disable_color_tracing: tracing_config.unwrap().disable_color_tracing,
                });
            }
        }

        let storage_class = value
            .storage_class
            .map(|storage_class| StorageClass::from_str(&storage_class).unwrap());

        let sse = value
            .sse
            .map(|sse| ServerSideEncryption::from_str(&sse).unwrap());

        let canned_acl = value
            .acl
            .map(|acl| ObjectCannedAcl::from_str(&acl).unwrap());

        let include_regex = value
            .filter_include_regex
            .map(|regex| Regex::new(&regex).unwrap());

        let exclude_regex = value
            .filter_exclude_regex
            .map(|regex| Regex::new(&regex).unwrap());

        let rate_limit_bandwidth = value
            .rate_limit_bandwidth
            .map(|bandwidth| human_bytes::parse_human_bandwidth(&bandwidth).unwrap());

        let additional_checksum_algorithm = value
            .additional_checksum_algorithm
            .map(|algorithm| ChecksumAlgorithm::from(algorithm.as_str()));

        let checksum_mode = if value.enable_additional_checksum {
            Some(ChecksumMode::Enabled)
        } else {
            None
        };

        let tagging = value
            .tagging
            .map(|tagging| tagging::parse_tagging(&tagging).unwrap());

        let filter_larger_size = value
            .filter_larger_size
            .map(|human_bytes| human_bytes::parse_human_bytes_without_limit(&human_bytes).unwrap());
        let filter_smaller_size = value
            .filter_smaller_size
            .map(|human_bytes| human_bytes::parse_human_bytes_without_limit(&human_bytes).unwrap());

        let metadata = if value.metadata.is_some() {
            Some(metadata::parse_metadata(&value.metadata.unwrap())?)
        } else {
            None
        };

        Ok(Config {
            source: storage_path::parse_storage_path(&value.source),
            target: storage_path::parse_storage_path(&value.target),

            source_client_config,
            target_client_config,

            tracing_config,

            force_retry_config: ForceRetryConfig {
                force_retry_count: value.force_retry_count,
                force_retry_interval_milliseconds: value.force_retry_interval_milliseconds,
            },

            transfer_config: TransferConfig {
                multipart_threshold: human_bytes::parse_human_bytes(&value.multipart_threshold)
                    .unwrap(),
                multipart_chunksize: human_bytes::parse_human_bytes(&value.multipart_chunksize)
                    .unwrap(),
                auto_chunksize: value.auto_chunksize,
            },

            worker_size: value.worker_size,

            warn_as_error: value.warn_as_error,
            follow_symlinks: !value.ignore_symlinks,
            head_each_target: value.head_each_target,
            sync_with_delete: value.delete,
            disable_tagging: value.disable_tagging,
            sync_latest_tagging: value.sync_latest_tagging,
            no_guess_mime_type: value.no_guess_mime_type,
            disable_multipart_verify: value.disable_multipart_verify,
            disable_etag_verify: value.disable_etag_verify,
            enable_versioning: value.enable_versioning,
            storage_class,
            sse,
            sse_kms_key_id: SseKmsKeyId {
                id: value.sse_kms_key_id,
            },
            source_sse_c: value.source_sse_c,
            source_sse_c_key: SseCustomerKey {
                key: value.source_sse_c_key,
            },
            source_sse_c_key_md5: value.source_sse_c_key_md5,
            target_sse_c: value.target_sse_c,
            target_sse_c_key: SseCustomerKey {
                key: value.target_sse_c_key,
            },
            target_sse_c_key_md5: value.target_sse_c_key_md5,
            canned_acl,
            additional_checksum_algorithm,
            additional_checksum_mode: checksum_mode,
            dry_run: value.dry_run,
            rate_limit_objects: value.rate_limit_objects,
            rate_limit_bandwidth,
            cache_control: value.cache_control,
            content_disposition: value.content_disposition,
            content_encoding: value.content_encoding,
            content_language: value.content_language,
            content_type: value.content_type,
            expires: value.expires,
            metadata,
            tagging,
            filter_config: FilterConfig {
                before_time: value.filter_mtime_before,
                after_time: value.filter_mtime_after,
                remove_modified_filter: value.remove_modified_filter,
                check_size: value.check_size,
                include_regex,
                exclude_regex,
                larger_size: filter_larger_size,
                smaller_size: filter_smaller_size,
            },
            max_keys: value.max_keys,
            put_last_modified_metadata: value.put_last_modified_metadata,
            auto_complete_shell: value.auto_complete_shell,
        })
    }
}
