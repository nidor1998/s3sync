use crate::Config;
use crate::callback::event_manager::EventManager;
use crate::callback::filter_manager::FilterManager;
use crate::callback::preprocess_manager::PreprocessManager;
#[allow(unused_imports)]
use crate::config::args::value_parser::{
    canned_acl, checksum_algorithm, file_exist, human_bytes, metadata, sse, storage_class,
    storage_path, tagging, url,
};
use crate::config::{
    CLITimeoutConfig, ClientConfig, FilterConfig, ForceRetryConfig, RetryConfig, TracingConfig,
    TransferConfig,
};
#[allow(unused_imports)]
use crate::types::event_callback::EventType;
use crate::types::{
    AccessKeys, ClientConfigLocation, S3Credentials, SseCustomerKey, SseKmsKeyId, StoragePath,
};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ObjectCannedAcl, RequestPayer, ServerSideEncryption,
    StorageClass,
};
use aws_smithy_types::checksum_config::RequestChecksumCalculation;
use cfg_if::cfg_if;
use chrono::{DateTime, Utc};
use clap::Parser;
use clap::builder::{ArgPredicate, NonEmptyStringValueParser};
use clap_verbosity_flag::{Verbosity, WarnLevel};
use fancy_regex::Regex;
#[cfg(feature = "version")]
use shadow_rs::shadow;
use std::ffi::OsString;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;

mod tests;
mod value_parser;

const EXPRESS_ONEZONE_STORAGE_SUFFIX: &str = "--x-s3";

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
const DEFAULT_NO_SYNC_SYSTEM_METADATA: bool = false;
const DEFAULT_NO_SYNC_USER_DEFINED_METADATA: bool = false;
const DEFAULT_WARN_AS_ERROR: bool = false;
const DEFAULT_IGNORE_SYMLINKS: bool = false;
const DEFAULT_FORCE_PATH_STYLE: bool = false;
const DEFAULT_HEAD_EACH_TARGET: bool = false;
const DEFAULT_ENABLE_VERSIONING: bool = false;
const DEFAULT_REMOVE_MODIFIED_FILTER: bool = false;
const DEFAULT_CHECK_SIZE: bool = false;
const DEFAULT_CHECK_ETAG: bool = false;
const DEFAULT_CHECK_MTIME_AND_ETAG: bool = false;
const DEFAULT_SYNC_WITH_DELETE: bool = false;
const DEFAULT_DISABLE_TAGGING: bool = false;
const DEFAULT_SYNC_LATEST_TAGGING: bool = false;
const DEFAULT_NO_GUESS_MIME_TYPE: bool = false;
const DEFAULT_SERVER_SIDE_COPY: bool = false;
const DEFAULT_DISABLE_MULTIPART_VERIFY: bool = false;
const DEFAULT_DISABLE_ETAG_VERIFY: bool = false;
const DEFAULT_DISABLE_ADDITIONAL_CHECKSUM_VERIFY: bool = false;
const DEFAULT_ENABLE_ADDITIONAL_CHECKSUM: bool = false;
const DEFAULT_DRY_RUN: bool = false;
const DEFAULT_MAX_KEYS: i32 = 1000;
const DEFAULT_PUT_LAST_MODIFIED_METADATA: bool = false;
const DEFAULT_DISABLE_STALLED_STREAM_PROTECTION: bool = false;
const DEFAULT_DISABLE_PAYLOAD_SIGNING: bool = false;
const DEFAULT_DISABLE_CONTENT_MD5_HEADER: bool = false;
const DEFAULT_DELETE_EXCLUDED: bool = false;
const DEFAULT_FULL_OBJECT_CHECKSUM: bool = false;
const DEFAULT_DISABLE_EXPRESS_ONE_ZONE_ADDITIONAL_CHECKSUM: bool = false;
const DEFAULT_MAX_PARALLEL_MULTIPART_UPLOADS: u16 = 16;
const DEFAULT_MAX_PARALLEL_LISTINGS: u16 = 16;
const DEFAULT_OBJECT_LISTING_QUEUE_SIZE: u32 = 200000;
const DEFAULT_PARALLEL_LISTING_MAX_DEPTH: u16 = 2;
const DEFAULT_ALLOW_PARALLEL_LISTINGS_IN_EXPRESS_ONE_ZONE: bool = false;
const DEFAULT_ACCELERATE: bool = false;
const DEFAULT_REQUEST_PAYER: bool = false;
const DEFAULT_REPORT_SYNC_STATUS: bool = false;
const DEFAULT_REPORT_METADATA_SYNC_STATUS: bool = false;
const DEFAULT_REPORT_TAGGING_SYNC_STATUS: bool = false;
#[allow(dead_code)]
const DEFAULT_ALLOW_LUA_OS_LIBRARY: bool = false;
#[allow(dead_code)]
const DEFAULT_ALLOW_LUA_UNSAFE_VM: bool = false;
#[allow(dead_code)]
const DEFAULT_LUA_VM_MEMORY_LIMIT: &str = "64MiB";
const DEFAULT_SHOW_NO_PROGRESS: bool = false;
const DEFAULT_IF_MATCH: bool = false;
const DEFAULT_COPY_SOURCE_IF_MATCH: bool = false;

const NO_S3_STORAGE_SPECIFIED: &str = "either SOURCE or TARGET must be s3://\n";
const LOCAL_STORAGE_SPECIFIED: &str = "with --enable-versioning/--sync-latest-tagging/--copy-source-if-match, both storage must be s3://\n";
const VERSIONING_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE: &str =
    "--enable-versioning is not supported with express onezone storage class\n";
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

const CHECK_ETAG_CONFLICT: &str =
    "--head-each-target is required for --check-etag, or remove --remove-modified-filter\n";
const CHECK_ETAG_CONFLICT_SSE_KMS: &str =
    "--check-etag is not supported with --sse aws:kms | aws:kms:dsse \n";
const CHECK_ETAG_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE: &str =
    "--check-etag is not supported with express onezone storage class\n";

const CHECK_MTIME_AND_ETAG_CONFLICT_SSE_KMS: &str =
    "--check-mtime-and-etag is not supported with --sse aws:kms | aws:kms:dsse \n";
const CHECK_MTIME_AND_ETAG_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE: &str =
    "--check-mtime-and-etag is not supported with express onezone storage class\n";

const SOURCE_LOCAL_STORAGE_DIR_NOT_FOUND: &str = "directory must be specified as a source\n";
const TARGET_LOCAL_STORAGE_INVALID: &str = "invalid target path\n";
const SSE_KMS_KEY_ID_ARGUMENTS_CONFLICT: &str =
    "--sse-kms-key-id must be used with --sse aws:kms | aws:kms:dsse\n";
const LOCAL_STORAGE_SPECIFIED_WITH_SSE_C: &str =
    "with --source-sse-c/--target-sse-c, remote storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_DISABLE_PAYLOAD_SIGNING: &str =
    "with --disable-payload-signing, target storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_DISABLE_CONTENT_MD5_HEADER: &str =
    "with --disable-content-md5-header, target storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_FULL_OBJECT_CHECKSUM: &str =
    "with --full-object-checksum, target storage must be s3://\n";
const FULL_OBJECT_CHECKSUM_NOT_SUPPORTED: &str =
    "Only CRC32/CRC32C/CRC64NVME supports full object checksum\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ACCELERATE: &str =
    "with --source-accelerate, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ACCELERATE: &str =
    "with --target-accelerate, target storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_REQUEST_PAYER: &str =
    "with --source-request-payer, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_REQUEST_PAYER: &str =
    "with --target-request-payer, target storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_SERVER_SIDE_COPY: &str =
    "with --server-side-copy, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_SERVER_SIDE_COPY: &str =
    "with --server-side-copy, target storage must be s3://\n";

const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_FILTER_INCLUDE_METADATA_REGEX: &str =
    "with --filter-include-metadata-regex, source storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_FILTER_EXCLUDE_METADATA_REGEX: &str =
    "with --filter-exclude-metadata-regex, source storage must be s3://\n";

const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_FILTER_INCLUDE_TAG_REGEX: &str =
    "with --filter-include-tag-regex, source storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_FILTER_EXCLUDE_TAG_REGEX: &str =
    "with --filter-exclude-tag-regex, source storage must be s3://\n";

const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SYNC_SYSTEM_METADATA: &str =
    "with --no-sync-system-metadata, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_NO_SYNC_SYSTEM_METADATA: &str =
    "with --no-sync-system-metadata, target storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SYNC_USER_DEFINED_METADATA: &str =
    "with --no-sync-user-defined-metadata, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_NO_SYNC_USER_DEFINED_METADATA: &str =
    "with --no-sync-user-defined-metadata, target storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_POINT_IN_TIME: &str =
    "with --point-in-time, source storage must be s3://\n";
const POINT_IN_TIME_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE: &str =
    "--point-in-time is not supported with express onezone storage class\n";
const POINT_IN_TIME_VALUE_INVALID: &str = "--point-in-time value is later than current time\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_REPORT_METADATA_SYNC_STATUS: &str =
    "with --report-metadata-sync-status, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_REPORT_METADATA_SYNC_STATUS: &str =
    "with --report-metadata-sync-status, target storage must be s3://\n";
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_REPORT_TAGGING_SYNC_STATUS: &str =
    "with --report-tagging-sync-status, source storage must be s3://\n";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_REPORT_TAGGING_SYNC_STATUS: &str =
    "with --report-tagging-sync-status, target storage must be s3://\n";
const NO_SOURCE_CREDENTIAL_REQUIRED: &str = "no source credential required\n";
const NO_TARGET_CREDENTIAL_REQUIRED: &str = "no target credential required\n";
#[allow(dead_code)]
const LUA_SCRIPT_LOAD_ERROR: &str = "Failed to load and compile Lua script callback: ";
const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_IF_MATCH: &str =
    "with --if-match, target storage must be s3://\n";
const IF_MATCH_CONFLICT: &str =
    "--head-each-target is required for --if-match, or remove --remove-modified-filter\n";

#[cfg(feature = "version")]
shadow!(build);

#[derive(Parser, Clone, Debug)]
#[cfg_attr(feature = "version", command(version=format!("{} ({} {}), {}", build::PKG_VERSION, build::SHORT_COMMIT, build::BUILD_TARGET, build::RUST_VERSION)))]
pub struct CLIArgs {
    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix] or local path", value_parser = storage_path::check_storage_path, default_value_if("auto_complete_shell", ArgPredicate::IsPresent, "s3://ignored"), required = false)]
    source: String,

    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix] or local path", value_parser = storage_path::check_storage_path, default_value_if("auto_complete_shell", ArgPredicate::IsPresent, "s3://ignored"), required = false)]
    target: String,

    /// A simulation mode. No actions will be performed.
    #[arg(long, env, default_value_t = DEFAULT_DRY_RUN, help_heading = "General")]
    dry_run: bool,

    /// Don't show the progress bar.
    #[arg(long, env, default_value_t = DEFAULT_SHOW_NO_PROGRESS, help_heading = "General")]
    show_no_progress: bool,

    #[arg(long, env, default_value_t = DEFAULT_SERVER_SIDE_COPY, help_heading = "General",
    long_help = r#"Use server-side copy. This option is only available both source and target are S3 storage.
It cannot work with between different object storages or regions."#)]
    server_side_copy: bool,

    /// Location of the file that the AWS CLI uses to store configuration profiles
    #[arg(long, env, value_name = "FILE", help_heading = "AWS Configuration")]
    aws_config_file: Option<PathBuf>,

    /// Location of the file that the AWS CLI uses to store access keys
    #[arg(long, env, value_name = "FILE", help_heading = "AWS Configuration")]
    aws_shared_credentials_file: Option<PathBuf>,

    /// Source AWS CLI profile
    #[arg(long, env, conflicts_with_all = ["source_access_key", "source_secret_access_key", "source_session_token"], help_heading = "AWS Configuration")]
    source_profile: Option<String>,

    /// Source access key
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_secret_access_key", help_heading = "AWS Configuration")]
    source_access_key: Option<String>,

    /// Source secret access key
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_access_key", help_heading = "AWS Configuration")]
    source_secret_access_key: Option<String>,

    /// Source session token
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_access_key", help_heading = "AWS Configuration")]
    source_session_token: Option<String>,

    /// Source region
    #[arg(long, env, value_parser = NonEmptyStringValueParser::new(), help_heading = "Source Options")]
    source_region: Option<String>,

    /// Source endpoint url
    #[arg(long, env, value_parser = url::check_scheme, help_heading = "Source Options")]
    source_endpoint_url: Option<String>,

    /// Use Amazon S3 Transfer Acceleration for the source bucket.
    #[arg(long, env, default_value_t = DEFAULT_ACCELERATE, help_heading = "Source Options")]
    source_accelerate: bool,

    /// Use request payer for the source bucket.
    #[arg(long, env, default_value_t = DEFAULT_REQUEST_PAYER, help_heading = "Source Options")]
    source_request_payer: bool,

    /// Force path-style addressing for source endpoint.
    #[arg(long, env, default_value_t = DEFAULT_FORCE_PATH_STYLE, help_heading = "Source Options")]
    source_force_path_style: bool,

    /// Target AWS CLI profile
    #[arg(long, env, conflicts_with_all = ["target_access_key", "target_secret_access_key", "target_session_token"], help_heading = "AWS Configuration")]
    target_profile: Option<String>,

    /// Target access key
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_secret_access_key", help_heading = "AWS Configuration")]
    target_access_key: Option<String>,

    /// Target secret access key
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key", help_heading = "AWS Configuration")]
    target_secret_access_key: Option<String>,

    /// Target session token
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key", help_heading = "AWS Configuration")]
    target_session_token: Option<String>,

    /// Target region
    #[arg(long, env, value_parser = NonEmptyStringValueParser::new(), help_heading = "Target Options")]
    target_region: Option<String>,

    /// Target endpoint url
    #[arg(long, env, value_parser = url::check_scheme, help_heading = "Target Options")]
    target_endpoint_url: Option<String>,

    /// Use Amazon S3 Transfer Acceleration for the target bucket.
    #[arg(long, env, default_value_t = DEFAULT_ACCELERATE, help_heading = "Target Options")]
    target_accelerate: bool,

    /// Use request payer for the target bucket.
    #[arg(long, env, default_value_t = DEFAULT_REQUEST_PAYER,help_heading = "Target Options")]
    target_request_payer: bool,

    /// Force path-style addressing for target endpoint.
    #[arg(long, env, default_value_t = DEFAULT_FORCE_PATH_STYLE, help_heading = "Target Options")]
    target_force_path_style: bool,

    #[arg(long, env, value_parser = storage_class::parse_storage_class, help_heading = "Target Options",
    long_help = r#"Type of storage to use for the target object.
Valid choices: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONE-ZONE_IA | INTELLIGENT_TIERING | GLACIER |
               DEEP_ARCHIVE | GLACIER_IR | EXPRESS_ONEZONE"#)]
    storage_class: Option<String>,

    #[arg(
        long,
        env,
        help_heading = "Filtering",
        long_help = r#"Sync only objects older than given time (RFC3339 datetime).
Example: 2023-02-19T12:00:00Z"#
    )]
    filter_mtime_before: Option<DateTime<Utc>>,

    #[arg(
        long,
        env,
        help_heading = "Filtering",
        long_help = r#"Sync only objects newer than OR EQUAL TO given time (RFC3339 datetime).
Example: 2023-02-19T12:00:00Z"#
    )]
    filter_mtime_after: Option<DateTime<Utc>>,

    #[arg(long, env, value_parser = human_bytes::check_human_bytes_without_limit, help_heading = "Filtering", long_help=r#"Sync only objects smaller than given size.
Allow suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB"#)]
    filter_smaller_size: Option<String>,

    #[arg(long, env, value_parser = human_bytes::check_human_bytes_without_limit, help_heading = "Filtering", long_help=r#"Sync only objects larger than OR EQUAL TO given size.
Allow suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB"#)]
    filter_larger_size: Option<String>,

    /// Sync only objects that match a given regular expression.
    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex, help_heading = "Filtering")]
    filter_include_regex: Option<String>,

    /// Do not sync objects that match a given regular expression.
    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex, help_heading = "Filtering")]
    filter_exclude_regex: Option<String>,

    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex, help_heading = "Filtering",
    long_help = r#"Sync only objects that have Content-Type matching a given regular expression.
If the source is local storage, Content-Type is guessed by the file extension,
Unless --no-guess-mime-type is specified.
It may take an extra API call to get Content-Type of the object.
"#)]
    filter_include_content_type_regex: Option<String>,

    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex, help_heading = "Filtering",
    long_help=r#"Do not sync objects that have Content-Type matching a given regular expression.
If the source is local storage, Content-Type is guessed by the file extension,
Unless --no-guess-mime-type is specified.
It may take an extra API call to get Content-Type of the object.
"#)]
    filter_exclude_content_type_regex: Option<String>,

    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex, help_heading = "Filtering",
    long_help=r#"Sync only objects that have metadata matching a given regular expression.
Keys(lowercase) must be sorted in alphabetical order, and comma separated.
This filter is applied after all other filters(except tag filters).
It may take an extra API call to get metadata of the object.

Example: "key1=(value1|value2),key2=value2""#)]
    filter_include_metadata_regex: Option<String>,

    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex, help_heading = "Filtering",
    long_help=r#"Do not sync objects that have metadata matching a given regular expression.
Keys(lowercase) must be sorted in alphabetical order, and comma separated.
This filter is applied after all other filters(except tag filters).
It may take an extra API call to get metadata of the object.

Example: "key1=(value1|value2),key2=value2""#)]
    filter_exclude_metadata_regex: Option<String>,

    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex, help_heading = "Filtering",
    long_help=r#"Sync only objects that have tag matching a given regular expression.
Keys must be sorted in alphabetical order, and '&' separated.
This filter is applied after all other filters.
It takes an extra API call to get tags of the object.

Example: "key1=(value1|value2)&key2=value2""#)]
    filter_include_tag_regex: Option<String>,

    #[arg(long, env, value_parser = crate::config::args::value_parser::regex::parse_regex, help_heading = "Filtering",
    long_help=r#"Do not sync objects that have tag matching a given regular expression.
Keys must be sorted in alphabetical order, and '&' separated.
This filter is applied after all other filters.
It takes an extra API call to get tags of the object.

Example: "key1=(value1|value2)&key2=value2""#)]
    filter_exclude_tag_regex: Option<String>,

    /// Do not update checking(ListObjectsV2) for modification in the target storage.
    #[arg(long, env, conflicts_with_all = ["enable_versioning"], default_value_t = DEFAULT_REMOVE_MODIFIED_FILTER, help_heading = "Update Checking")]
    remove_modified_filter: bool,

    /// Use object size for update checking.
    #[arg(long, env, conflicts_with_all = ["enable_versioning", "check_etag", "check_mtime_and_etag", "check_mtime_and_additional_checksum"], default_value_t = DEFAULT_CHECK_SIZE, help_heading = "Update Checking")]
    check_size: bool,

    /// Use etag for update checking.
    #[arg(long, env, conflicts_with_all = ["enable_versioning", "check_size", "check_mtime_and_etag", "check_mtime_and_additional_checksum", "source_sse_c_key", "target_sse_c_key"], default_value_t = DEFAULT_CHECK_ETAG, help_heading = "Update Checking",)]
    check_etag: bool,

    #[arg(long, env, conflicts_with_all = ["enable_versioning", "remove_modified_filter", "check_size", "check_etag", "source_sse_c_key", "target_sse_c_key"], default_value_t = DEFAULT_CHECK_MTIME_AND_ETAG, help_heading = "Update Checking",
    long_help=r#"Use the modification time and ETag for update checking.
If the source modification date is newer, check the ETag.
"#)]
    check_mtime_and_etag: bool,

    /// Use additional checksum for update checking.
    #[arg(long, env, conflicts_with_all = ["enable_versioning", "check_size", "check_etag", "check_mtime_and_etag", "check_mtime_and_additional_checksum"], value_parser = checksum_algorithm::parse_checksum_algorithm, help_heading = "Update Checking")]
    check_additional_checksum: Option<String>,

    #[arg(long, env, conflicts_with_all = ["enable_versioning", "remove_modified_filter", "check_size", "check_etag", "check_mtime_and_etag", "check_additional_checksum"], value_parser = checksum_algorithm::parse_checksum_algorithm, help_heading = "Update Checking",
    long_help=r#"Use the modification time and additional checksum for update checking.
If the source modification date is newer, check the additional checksum.
"#)]
    check_mtime_and_additional_checksum: Option<String>,

    /// Additional checksum algorithm for upload
    #[arg(long, env, value_parser = checksum_algorithm::parse_checksum_algorithm, help_heading = "Verification")]
    additional_checksum_algorithm: Option<String>,

    #[arg(long, env, default_value_t = DEFAULT_FULL_OBJECT_CHECKSUM, help_heading = "Verification", long_help=r#"Use full object checksum for verification. CRC64NVME automatically use full object checksum.
This option cannot be used with SHA1/SHA256 additional checksum."#)]
    full_object_checksum: bool,

    /// Enable additional checksum for download
    #[arg(long, env, default_value_t = DEFAULT_ENABLE_ADDITIONAL_CHECKSUM, help_heading = "Verification")]
    enable_additional_checksum: bool,

    /// Disable multipart upload verification with ETag/additional checksum.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_MULTIPART_VERIFY, help_heading = "Verification")]
    disable_multipart_verify: bool,

    /// Disable etag verification.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_ETAG_VERIFY, help_heading = "Verification")]
    disable_etag_verify: bool,

    #[arg(long, env, requires = "additional_checksum_algorithm", default_value_t = DEFAULT_DISABLE_ADDITIONAL_CHECKSUM_VERIFY, help_heading = "Verification",
    long_help=r#"Disable additional checksum verification 
But use additional checksum for upload (The hash value is stored in the target object)."#)]
    disable_additional_checksum_verify: bool,

    /// Number of workers for synchronization
    #[arg(long, env, default_value_t = DEFAULT_WORKER_SIZE, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance")]
    worker_size: u16,

    /// Maximum number of parallel multipart uploads/downloads
    #[arg(long, env, default_value_t = DEFAULT_MAX_PARALLEL_MULTIPART_UPLOADS, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance")]
    max_parallel_uploads: u16,

    #[arg(long, env, default_value_t = DEFAULT_MAX_PARALLEL_LISTINGS, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance", long_help=r#"Maximum number of parallel listings of objects."#)]
    max_parallel_listings: u16,

    #[arg(long, env, default_value_t = DEFAULT_PARALLEL_LISTING_MAX_DEPTH, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance", long_help=r#"Maximum depth(sub directroy/prefix) of parallel listings."#)]
    max_parallel_listing_max_depth: u16,

    /// Queue size for object listings
    #[arg(long, env, default_value_t = DEFAULT_OBJECT_LISTING_QUEUE_SIZE, value_parser = clap::value_parser!(u32).range(1..), help_heading = "Performance")]
    object_listing_queue_size: u32,

    #[arg(long, env, default_value_t = DEFAULT_ALLOW_PARALLEL_LISTINGS_IN_EXPRESS_ONE_ZONE, help_heading = "Performance", long_help=r#"Allow parallel listings in express one zone storage class.
It may include multipart upload in progress objects in the listing result."#)]
    allow_parallel_listings_in_express_one_zone: bool,

    /// Rate limit objects per second
    #[arg(long, env,  value_parser = clap::value_parser!(u32).range(10..), help_heading = "Performance")]
    rate_limit_objects: Option<u32>,

    /// Rate limit bandwidth(bytes per sec). Allow suffixes: MB, MiB, GB, GiB
    #[arg(long, env, value_parser = human_bytes::check_human_bandwidth, help_heading = "Performance")]
    rate_limit_bandwidth: Option<String>,

    #[arg(long, env, conflicts_with_all = ["auto_chunksize"], default_value = DEFAULT_MULTIPART_THRESHOLD, value_parser = human_bytes::check_human_bytes, help_heading = "Multipart Settings",
    long_help=r#"Object size threshold that s3sync uses for multipart upload
Allow suffixes: MB, MiB, GB, GiB.
The larger the size, the larger the memory usage."#)]
    multipart_threshold: String,

    #[arg(long, env, conflicts_with_all = ["auto_chunksize"], default_value = DEFAULT_MULTIPART_CHUNKSIZE, value_parser = human_bytes::check_human_bytes, help_heading = "Multipart Settings",
    long_help=r#"Chunk size that s3sync uses for multipart upload of individual files
Allow suffixes: MB, MiB, GB, GiB.
The larger the size, the larger the memory usage."#)]
    multipart_chunksize: String,

    #[arg(long, env, conflicts_with_all = ["multipart_threshold", "multipart_chunksize"], default_value_t = DEFAULT_AUTO_CHUNKSIZE, help_heading = "Multipart Settings",
    long_help=r#"Automatically adjusts a chunk size to match the source or target.
It takes extra HEAD requests(1 API call per part)."#)]
    auto_chunksize: bool,

    /// Cache-Control HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    cache_control: Option<String>,

    /// Content-Disposition HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    content_disposition: Option<String>,

    /// Content-Encoding HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    content_encoding: Option<String>,

    /// Content-Language HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    content_language: Option<String>,

    /// Content-Type HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    content_type: Option<String>,

    #[arg(
        long,
        env,
        help_heading = "Metadata/Headers",
        long_help = r#"Expires HTTP header to set on the target object(RFC3339 datetime)
Example: 2023-02-19T12:00:00Z"#
    )]
    expires: Option<DateTime<Utc>>,

    #[arg(long, env, value_parser = metadata::check_metadata, help_heading = "Metadata/Headers", long_help=r#"Metadata to set on the target object
Example: key1=value1,key2=value2"#)]
    metadata: Option<String>,

    /// x-amz-website-redirect-location header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    website_redirect: Option<String>,

    #[arg(long, env, default_value_t =  DEFAULT_NO_SYNC_SYSTEM_METADATA, help_heading = "Metadata/Headers",
    long_help= r#"Do not sync system metadata
System metadata: content-disposition, content-encoding, content-language, content-type,
                 cache-control, expires, website-redirect"#)]
    no_sync_system_metadata: bool,

    /// Do not sync user-defined metadata.
    #[arg(long, env, default_value_t =  DEFAULT_NO_SYNC_USER_DEFINED_METADATA, help_heading = "Metadata/Headers")]
    no_sync_user_defined_metadata: bool,

    #[arg(long, env, conflicts_with_all = ["disable_tagging", "sync_latest_tagging"], value_parser = tagging::parse_tagging, help_heading = "Tagging",
    long_help=r#"Tagging to set on the target object.
Key/value must be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.

Example: key1=value1&key2=value2"#)]
    tagging: Option<String>,

    /// Do not copy tagging.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_TAGGING, help_heading = "Tagging")]
    disable_tagging: bool,

    #[arg(long, env, conflicts_with_all = ["enable_versioning", "disable_tagging"], default_value_t = DEFAULT_SYNC_LATEST_TAGGING, help_heading = "Tagging",
    long_help=r#"Copy the latest tagging from the source if necessary.
If this option is enabled, the --remove-modified-filter and
--head-each-target options are automatically enabled."#)]
    sync_latest_tagging: bool,

    #[arg(long, env, conflicts_with_all = ["delete", "head_each_target", "remove_modified_filter"], default_value_t = DEFAULT_ENABLE_VERSIONING, help_heading = "Versioning",
    long_help=r#"Sync all version objects in the source storage to the target versioning storage.
 "#)]
    enable_versioning: bool,

    #[arg(
        long,
        env,
        conflicts_with_all = ["delete", "head_each_target", "enable_versioning", "check_etag", "check_mtime_and_etag", "check_size", "check_mtime_and_additional_checksum"],
        help_heading = "Versioning",
        long_help = r#"Sync only objects at a specific point in time (RFC3339 datetime).
The source storage must be a versioning enabled S3 bucket.
By default, the target storage's objects will always be overwritten with the source's objects.

Example: 2025-07-16T12:00:00Z"#
    )]
    point_in_time: Option<DateTime<Utc>>,

    /// Server-side encryption. Valid choices: AES256 | aws:kms | aws:kms:dsse
    #[arg(long, env, value_parser = sse::parse_sse, help_heading = "Encryption")]
    sse: Option<String>,

    /// SSE KMS ID key
    #[arg(long, env, help_heading = "Encryption")]
    sse_kms_key_id: Option<String>,

    /// Source SSE-C algorithm. Valid choices: AES256
    #[arg(long, env, conflicts_with_all = ["sse", "sse_kms_key_id"], requires = "source_sse_c_key", value_parser = sse::parse_sse_c, help_heading = "Encryption")]
    source_sse_c: Option<String>,

    /// Source SSE-C customer-provided encryption key(256bit key. must be base64 encoded)
    #[arg(
        long,
        env,
        requires = "source_sse_c_key_md5",
        help_heading = "Encryption"
    )]
    source_sse_c_key: Option<String>,

    /// Source base64 encoded MD5 digest of source_sse_c_key
    #[arg(long, env, requires = "source_sse_c", help_heading = "Encryption")]
    source_sse_c_key_md5: Option<String>,

    /// Target SSE-C algorithm. Valid choices: AES256
    #[arg(long, env, conflicts_with_all = ["sse", "sse_kms_key_id"], requires = "target_sse_c_key", value_parser = sse::parse_sse_c, help_heading = "Encryption")]
    target_sse_c: Option<String>,

    /// Target SSE-C customer-provided encryption key(256bit key. must be base64 encoded)
    #[arg(
        long,
        env,
        requires = "target_sse_c_key_md5",
        help_heading = "Encryption"
    )]
    target_sse_c_key: Option<String>,

    /// Target base64 encoded MD5 digest of target-sse-c-key
    #[arg(long, env, requires = "target_sse_c", help_heading = "Encryption")]
    target_sse_c_key_md5: Option<String>,

    /// Trace verbosity(-v: show info, -vv: show debug, -vvv show trace)
    #[clap(flatten)]
    verbosity: Verbosity<WarnLevel>,

    #[arg(
        long,
        env,
        default_value_t = DEFAULT_REPORT_SYNC_STATUS,
        conflicts_with_all = ["dry_run", "delete", "enable_versioning", "head_each_target"],
        help_heading = "Reporting",
        long_help = r#"Report sync status to the target storage.
Default verification is for etag. For additional checksum, use --check-additional-checksum.
For more precise control, use with --auto-chunksize."#
    )]
    report_sync_status: bool,

    #[arg(
        long,
        env,
        default_value_t = DEFAULT_REPORT_METADATA_SYNC_STATUS,
        requires = "report_sync_status",
        help_heading = "Reporting",
        long_help = r#"Report metadata sync status to the target storage.
It must be used with --report-sync-status.
Note: s3sync generated user-defined metadata(s3sync_origin_version_id/s3sync_origin_last_modified) were ignored.
      Because they are usually different from the source storage."#
    )]
    report_metadata_sync_status: bool,

    #[arg(
        long,
        env,
        default_value_t = DEFAULT_REPORT_TAGGING_SYNC_STATUS,
        requires = "report_sync_status",
        help_heading = "Reporting",
        long_help = r#"Report tagging sync status to the target storage.
It must be used with --report-sync-status."#
    )]
    report_tagging_sync_status: bool,

    /// Show trace as json format.
    #[arg(long, env, default_value_t = DEFAULT_JSON_TRACING, help_heading = "Tracing/Logging")]
    json_tracing: bool,

    /// Enable aws sdk tracing.
    #[arg(long, env, default_value_t = DEFAULT_AWS_SDK_TRACING, help_heading = "Tracing/Logging")]
    aws_sdk_tracing: bool,

    /// Show span event tracing.
    #[arg(long, env, default_value_t = DEFAULT_SPAN_EVENTS_TRACING, help_heading = "Tracing/Logging")]
    span_events_tracing: bool,

    /// Disable ANSI terminal colors.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_COLOR_TRACING, help_heading = "Tracing/Logging")]
    disable_color_tracing: bool,

    /// Maximum retry attempts that s3sync retry handler use
    #[arg(long, env, default_value_t = DEFAULT_AWS_MAX_ATTEMPTS, value_name = "max_attempts", help_heading = "Retry Options")]
    aws_max_attempts: u32,

    #[arg(long, env, default_value_t = DEFAULT_INITIAL_BACKOFF_MILLISECONDS, value_name = "initial_backoff", help_heading = "Retry Options",
    long_help=r#"A multiplier value used when calculating backoff times as part of an exponential backoff with jitter strategy.
"#)]
    initial_backoff_milliseconds: u64,

    /// Maximum force retry attempts that s3sync retry handler use
    #[arg(long, env, default_value_t = DEFAULT_FORCE_RETRY_COUNT, help_heading = "Retry Options")]
    force_retry_count: u32,

    #[arg(long, env, default_value_t = DEFAULT_FORCE_RETRY_INTERVAL_MILLISECONDS, value_name = "force_retry_interval", help_heading = "Retry Options",
    long_help=r#"Sleep interval (milliseconds) between s3sync force retries on error.
"#)]
    force_retry_interval_milliseconds: u64,

    #[arg(
        long,
        env,
        value_name = "operation_timeout",
        help_heading = "Timeout Options",
        long_help = r#"Operation timeout (milliseconds). For details, see the AWS SDK for Rust TimeoutConfig documentation.
The default has no timeout."#
    )]
    operation_timeout_milliseconds: Option<u64>,

    #[arg(
        long,
        env,
        value_name = "operation_attempt_timeout",
        help_heading = "Timeout Options",
        long_help = r#"Operation attempt timeout (milliseconds). For details, see the AWS SDK for Rust TimeoutConfig documentation.
The default has no timeout."#
    )]
    operation_attempt_timeout_milliseconds: Option<u64>,

    #[arg(
        long,
        env,
        value_name = "connect_timeout",
        help_heading = "Timeout Options",
        long_help = r#"Connect timeout (milliseconds).The default has AWS SDK default timeout (Currently 3100 milliseconds).
"#
    )]
    connect_timeout_milliseconds: Option<u64>,

    #[arg(
        long,
        env,
        value_name = "read_timeout",
        help_heading = "Timeout Options",
        long_help = r#"Read timeout (milliseconds). The default has no timeout."#
    )]
    read_timeout_milliseconds: Option<u64>,

    /// Treat warnings as errors(except for the case of etag/checksum mismatch, etc.).
    #[arg(long, env, default_value_t = DEFAULT_WARN_AS_ERROR, help_heading = "Advanced")]
    warn_as_error: bool,

    /// Ignore symbolic links.
    #[arg(long, env, default_value_t = DEFAULT_IGNORE_SYMLINKS, help_heading = "Advanced")]
    ignore_symlinks: bool,

    #[arg(long, env, conflicts_with_all = ["enable_versioning"], default_value_t = DEFAULT_HEAD_EACH_TARGET, help_heading = "Advanced",
    long_help=r#"HeadObject is used to check whether an object has been modified in the target storage.
It reduces the possibility of race condition issue"#)]
    head_each_target: bool,

    #[arg(long, env, value_parser = canned_acl::parse_canned_acl, help_heading = "Advanced",
    long_help=r#"ACL for the objects
Valid choices: private | public-read | public-read-write | authenticated-read | aws-exec-read |
               bucket-owner-read | bucket-owner-full-control"#)]
    acl: Option<String>,

    /// Do not try to guess the mime type of local file.
    #[arg(long, env, default_value_t = DEFAULT_NO_GUESS_MIME_TYPE, help_heading = "Advanced")]
    no_guess_mime_type: bool,

    /// Maximum number of objects returned in a single list object request
    #[arg(long, env, default_value_t = DEFAULT_MAX_KEYS, value_parser = clap::value_parser!(i32).range(1..=32767), help_heading = "Advanced")]
    max_keys: i32,

    /// Put last modified of the source to metadata.
    #[arg(long, env, default_value_t = DEFAULT_PUT_LAST_MODIFIED_METADATA, help_heading = "Advanced")]
    put_last_modified_metadata: bool,

    #[arg(long, env, value_name = "SHELL", value_parser = clap_complete::shells::Shell::from_str, help_heading = "Advanced",
    long_help=r#"Generate a auto completions script.
Valid choices: bash, fish, zsh, powershell, elvish."#)]
    auto_complete_shell: Option<clap_complete::shells::Shell>,

    /// Disable stalled stream protection.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_STALLED_STREAM_PROTECTION, help_heading = "Advanced")]
    disable_stalled_stream_protection: bool,

    /// Disable payload signing for object uploads.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_PAYLOAD_SIGNING, help_heading = "Advanced")]
    disable_payload_signing: bool,

    #[arg(long, env, default_value_t = DEFAULT_DISABLE_CONTENT_MD5_HEADER, help_heading = "Advanced",
    long_help=r#"Disable Content-MD5 header for object uploads. It disables the ETag verification for the uploaded object.
"#)]
    disable_content_md5_header: bool,

    #[arg(long, env, default_value_t = DEFAULT_DISABLE_EXPRESS_ONE_ZONE_ADDITIONAL_CHECKSUM, help_heading = "Advanced",
    long_help=r#"Disable default additional checksum verification in Express One Zone storage class.
 "#)]
    disable_express_one_zone_additional_checksum: bool,

    #[arg(long, env, requires = "delete", default_value_t = DEFAULT_DELETE_EXCLUDED, help_heading = "Advanced",
    long_help=r#"When used in combination with --delete options, supplied --filter-exclude-regex patterns will not prevent an object from being deleted.
"#)]
    delete_excluded: bool,

    #[arg(long, env, conflicts_with_all = ["enable_versioning", "point_in_time"], default_value_t = DEFAULT_IF_MATCH, help_heading = "Advanced", long_help=r#"Add an If-Match header for PutObject/CompleteMultipartUpload/DeleteObject requests.
This is for like an optimistic lock."#)]
    if_match: bool,

    #[arg(long, env, conflicts_with_all = ["enable_versioning", "point_in_time"], requires = "server_side_copy", default_value_t = DEFAULT_COPY_SOURCE_IF_MATCH, help_heading = "Advanced", long_help=r#"Add an x-amz-copy-source-if-match header for CopyObject/UploadPartCopy requests.
This is for like an optimistic lock."#)]
    copy_source_if_match: bool,

    /// Don't delete more than a specified number of objects
    #[arg(long, env, value_parser = clap::value_parser!(u64).range(1..), help_heading = "Advanced")]
    max_delete: Option<u64>,

    #[cfg(feature = "lua_support")]
    #[arg(
        long,
        env,
        help_heading = "Lua scripting support",
        value_parser = file_exist::is_file_exist,
        long_help = r#"Path to the Lua script that is executed as preprocess callback"#
    )]
    preprocess_callback_lua_script: Option<String>,

    #[cfg(feature = "lua_support")]
    #[arg(
        long,
        env,
        help_heading = "Lua scripting support",
        value_parser = file_exist::is_file_exist,
        long_help = r#"Path to the Lua script that is executed as event callback"#
    )]
    event_callback_lua_script: Option<String>,

    #[cfg(feature = "lua_support")]
    #[arg(
        long,
        env,
        help_heading = "Lua scripting support",
        value_parser = file_exist::is_file_exist,
        long_help = r#"Path to the Lua script that is executed as filter callback"#
    )]
    filter_callback_lua_script: Option<String>,

    #[cfg(feature = "lua_support")]
    #[arg(long, env, conflicts_with_all = ["allow_lua_unsafe_vm"], default_value_t = DEFAULT_ALLOW_LUA_OS_LIBRARY, help_heading = "Lua scripting support", long_help="Allow Lua OS and I/O library functions in the Lua script.")]
    allow_lua_os_library: bool,

    #[cfg(feature = "lua_support")]
    #[arg(long, env, default_value = DEFAULT_LUA_VM_MEMORY_LIMIT, value_parser = human_bytes::check_human_bytes_without_limit, help_heading = "Lua scripting support",
    long_help=r#"Memory limit for the Lua VM. Allow suffixes: KB, KiB, MB, MiB, GB, GiB.
Zero means no limit.
If the memory limit is exceeded, the whole process will be terminated."#)]
    lua_vm_memory_limit: String,

    #[cfg(feature = "lua_support")]
    #[arg(long, env, conflicts_with_all = ["allow_lua_os_library"], default_value_t = DEFAULT_ALLOW_LUA_UNSAFE_VM, help_heading = "Dangerous",
    long_help=r#"Allow unsafe Lua VM functions in the Lua script.
It allows the Lua script to load unsafe standard libraries or C modules."#)]
    allow_lua_unsafe_vm: bool,

    #[arg(long, env, conflicts_with_all = ["enable_versioning"], default_value_t = DEFAULT_SYNC_WITH_DELETE, help_heading = "Dangerous",
    long_help=r#"Delete objects that exist in the target but not in the source.
Exclude filters other than --filter-exclude-regex will not prevent an object from being deleted.
[Warning] Since this can cause data loss, test first with the --dry-run option.
 "#)]
    delete: bool,

    /// Unit test purpose only
    #[arg(long, hide = true, default_value_t = false, help_heading = "Dangerous")]
    allow_both_local_storage: bool,

    /// test purpose only
    #[cfg(feature = "e2e_test_dangerous_simulations")]
    #[arg(long, hide = true, default_value_t = false, help_heading = "Dangerous")]
    test_user_defined_callback: bool,

    /// [dangerous] Test purpose only
    #[cfg(feature = "e2e_test_dangerous_simulations")]
    #[arg(long, hide = true, default_value_t = false, help_heading = "Dangerous")]
    allow_e2e_test_dangerous_simulation: bool,

    /// [dangerous] Test purpose only
    #[cfg(feature = "e2e_test_dangerous_simulations")]
    #[arg(long, hide = true, help_heading = "Dangerous")]
    cancellation_point: Option<String>,

    /// [dangerous] Test purpose only
    #[cfg(feature = "e2e_test_dangerous_simulations")]
    #[arg(long, hide = true, help_heading = "Dangerous")]
    panic_simulation_point: Option<String>,

    /// [dangerous] Test purpose only
    #[cfg(feature = "e2e_test_dangerous_simulations")]
    #[arg(long, hide = true, help_heading = "Dangerous")]
    error_simulation_point: Option<String>,
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
    // skipcq: RS-R1000
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
        self.check_check_e_tag_conflict()?;
        self.check_check_mtime_and_e_tag_conflict()?;
        self.check_ignore_symlinks_conflict()?;
        self.check_no_guess_mime_type_conflict()?;
        self.check_endpoint_url_conflict()?;
        self.check_disable_payload_signing_conflict()?;
        self.check_disable_content_md5_header_conflict()?;
        self.check_full_object_checksum_conflict()?;
        self.check_accelerate_conflict()?;
        self.check_request_payer_conflict()?;
        self.check_server_side_copy_conflict()?;
        self.check_filter_include_metadata_regex_conflict()?;
        self.check_filter_exclude_metadata_regex_conflict()?;
        self.check_filter_include_tag_regex_conflict()?;
        self.check_filter_exclude_tag_regex_conflict()?;
        self.check_server_no_sync_system_metadata_conflict()?;
        self.check_server_no_sync_user_defined_metadata_conflict()?;
        self.check_point_in_time_conflict()?;
        self.check_report_metadata_sync_status_conflict()?;
        self.check_report_tagging_sync_status_conflict()?;
        self.check_if_match_conflict()?;
        self.check_copy_source_if_match_conflict()?;

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
        if !self.enable_versioning {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        let target = storage_path::parse_storage_path(&self.target);

        if !storage_path::is_both_storage_s3(&source, &target) {
            return Err(LOCAL_STORAGE_SPECIFIED.to_string());
        }

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&self.source) {
            if is_express_onezone_storage(&bucket) {
                return Err(VERSIONING_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE.to_string());
            }
        }

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&self.target) {
            if is_express_onezone_storage(&bucket) {
                return Err(VERSIONING_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE.to_string());
            }
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
                || (ServerSideEncryption::from_str(self.sse.as_ref().unwrap()).unwrap()
                    != ServerSideEncryption::AwsKms
                    && ServerSideEncryption::from_str(self.sse.as_ref().unwrap()).unwrap()
                        != ServerSideEncryption::AwsKmsDsse))
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

        if !self.check_etag && !self.check_mtime_and_etag && matches!(source, StoragePath::Local(_))
        {
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
            && self.website_redirect.is_none()
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

    fn check_check_e_tag_conflict(&self) -> Result<(), String> {
        if !self.check_etag {
            return Ok(());
        }
        if self.remove_modified_filter && !self.head_each_target {
            return Err(CHECK_ETAG_CONFLICT.to_string());
        }

        if self.sse.is_some() {
            let sse = ServerSideEncryption::from_str(self.sse.as_ref().unwrap()).unwrap();
            if sse == ServerSideEncryption::AwsKms || sse == ServerSideEncryption::AwsKmsDsse {
                return Err(CHECK_ETAG_CONFLICT_SSE_KMS.to_string());
            }
        }

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&self.source) {
            if is_express_onezone_storage(&bucket) {
                return Err(CHECK_ETAG_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE.to_string());
            }
        }

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&self.target) {
            if is_express_onezone_storage(&bucket) {
                return Err(CHECK_ETAG_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE.to_string());
            }
        }

        Ok(())
    }

    fn check_check_mtime_and_e_tag_conflict(&self) -> Result<(), String> {
        if !self.check_mtime_and_etag {
            return Ok(());
        }

        if self.sse.is_some() {
            let sse = ServerSideEncryption::from_str(self.sse.as_ref().unwrap()).unwrap();
            if sse == ServerSideEncryption::AwsKms || sse == ServerSideEncryption::AwsKmsDsse {
                return Err(CHECK_MTIME_AND_ETAG_CONFLICT_SSE_KMS.to_string());
            }
        }

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&self.source) {
            if is_express_onezone_storage(&bucket) {
                return Err(CHECK_MTIME_AND_ETAG_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE.to_string());
            }
        }

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&self.target) {
            if is_express_onezone_storage(&bucket) {
                return Err(CHECK_MTIME_AND_ETAG_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE.to_string());
            }
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

    fn check_disable_payload_signing_conflict(&self) -> Result<(), String> {
        if !self.disable_payload_signing {
            return Ok(());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_DISABLE_PAYLOAD_SIGNING.to_string());
        }

        Ok(())
    }

    fn check_disable_content_md5_header_conflict(&self) -> Result<(), String> {
        if !self.disable_content_md5_header {
            return Ok(());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_DISABLE_CONTENT_MD5_HEADER.to_string());
        }

        Ok(())
    }

    fn check_full_object_checksum_conflict(&self) -> Result<(), String> {
        if !self.full_object_checksum {
            return Ok(());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_FULL_OBJECT_CHECKSUM.to_string());
        }

        if let Some(additional_checksum_algorithm) = &self.additional_checksum_algorithm {
            if additional_checksum_algorithm == "SHA1" || additional_checksum_algorithm == "SHA256"
            {
                return Err(FULL_OBJECT_CHECKSUM_NOT_SUPPORTED.to_string());
            }
        }

        Ok(())
    }

    fn check_accelerate_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.source_accelerate {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ACCELERATE.to_string());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) && self.target_accelerate {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ACCELERATE.to_string());
        }

        Ok(())
    }

    fn check_request_payer_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.source_request_payer {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_REQUEST_PAYER.to_string());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) && self.target_request_payer {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_REQUEST_PAYER.to_string());
        }

        Ok(())
    }

    fn check_server_side_copy_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.server_side_copy {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_SERVER_SIDE_COPY.to_string());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) && self.server_side_copy {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_SERVER_SIDE_COPY.to_string());
        }

        Ok(())
    }

    fn check_filter_include_metadata_regex_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.filter_include_metadata_regex.is_some() {
            return Err(
                SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_FILTER_INCLUDE_METADATA_REGEX.to_string(),
            );
        }

        Ok(())
    }

    fn check_filter_exclude_metadata_regex_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.filter_exclude_metadata_regex.is_some() {
            return Err(
                SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_FILTER_EXCLUDE_METADATA_REGEX.to_string(),
            );
        }

        Ok(())
    }

    fn check_filter_include_tag_regex_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.filter_include_tag_regex.is_some() {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_FILTER_INCLUDE_TAG_REGEX.to_string());
        }

        Ok(())
    }

    fn check_filter_exclude_tag_regex_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.filter_exclude_tag_regex.is_some() {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_FILTER_EXCLUDE_TAG_REGEX.to_string());
        }

        Ok(())
    }

    fn check_server_no_sync_system_metadata_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.no_sync_system_metadata {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SYNC_SYSTEM_METADATA.to_string());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) && self.no_sync_system_metadata {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_NO_SYNC_SYSTEM_METADATA.to_string());
        }

        Ok(())
    }

    fn check_server_no_sync_user_defined_metadata_conflict(&self) -> Result<(), String> {
        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) && self.no_sync_user_defined_metadata {
            return Err(
                SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SYNC_USER_DEFINED_METADATA.to_string(),
            );
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) && self.no_sync_user_defined_metadata {
            return Err(
                TARGET_LOCAL_STORAGE_SPECIFIED_WITH_NO_SYNC_USER_DEFINED_METADATA.to_string(),
            );
        }

        Ok(())
    }
    fn check_point_in_time_conflict(&self) -> Result<(), String> {
        if self.point_in_time.is_none() {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_POINT_IN_TIME.to_string());
        }

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&self.source) {
            if is_express_onezone_storage(&bucket) {
                return Err(POINT_IN_TIME_NOT_SUPPORTED_WITH_EXPRESS_ONEZONE.to_string());
            }
        }

        if Utc::now() < self.point_in_time.unwrap() {
            return Err(POINT_IN_TIME_VALUE_INVALID.to_string());
        }

        Ok(())
    }

    fn check_report_metadata_sync_status_conflict(&self) -> Result<(), String> {
        if !self.report_metadata_sync_status {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) {
            return Err(
                SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_REPORT_METADATA_SYNC_STATUS.to_string(),
            );
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(
                TARGET_LOCAL_STORAGE_SPECIFIED_WITH_REPORT_METADATA_SYNC_STATUS.to_string(),
            );
        }

        Ok(())
    }

    fn check_report_tagging_sync_status_conflict(&self) -> Result<(), String> {
        if !self.report_tagging_sync_status {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        if matches!(source, StoragePath::Local(_)) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_REPORT_TAGGING_SYNC_STATUS.to_string());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_REPORT_TAGGING_SYNC_STATUS.to_string());
        }

        Ok(())
    }

    fn check_if_match_conflict(&self) -> Result<(), String> {
        if !self.if_match {
            return Ok(());
        }

        let target = storage_path::parse_storage_path(&self.target);
        if matches!(target, StoragePath::Local(_)) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_IF_MATCH.to_string());
        }

        if self.remove_modified_filter && !self.head_each_target {
            return Err(IF_MATCH_CONFLICT.to_string());
        }

        Ok(())
    }

    fn check_copy_source_if_match_conflict(&self) -> Result<(), String> {
        if !self.copy_source_if_match {
            return Ok(());
        }

        let source = storage_path::parse_storage_path(&self.source);
        let target = storage_path::parse_storage_path(&self.target);

        if !storage_path::is_both_storage_s3(&source, &target) {
            return Err(LOCAL_STORAGE_SPECIFIED.to_string());
        }

        Ok(())
    }

    fn build_client_configs(
        &self,
        request_checksum_calculation: RequestChecksumCalculation,
    ) -> (Option<ClientConfig>, Option<ClientConfig>) {
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

        let parallel_upload_semaphore =
            Arc::new(Semaphore::new(self.max_parallel_uploads as usize));

        let source_request_payer = if self.source_request_payer {
            Some(RequestPayer::Requester)
        } else {
            None
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
            cli_timeout_config: CLITimeoutConfig {
                operation_timeout_milliseconds: self.operation_timeout_milliseconds,
                operation_attempt_timeout_milliseconds: self.operation_attempt_timeout_milliseconds,
                connect_timeout_milliseconds: self.connect_timeout_milliseconds,
                read_timeout_milliseconds: self.read_timeout_milliseconds,
            },
            disable_stalled_stream_protection: self.disable_stalled_stream_protection,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: parallel_upload_semaphore.clone(),
            accelerate: self.source_accelerate,
            request_payer: source_request_payer,
        });

        let target_request_payer = if self.target_request_payer {
            Some(RequestPayer::Requester)
        } else {
            None
        };

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
            cli_timeout_config: CLITimeoutConfig {
                operation_timeout_milliseconds: self.operation_timeout_milliseconds,
                operation_attempt_timeout_milliseconds: self.operation_attempt_timeout_milliseconds,
                connect_timeout_milliseconds: self.connect_timeout_milliseconds,
                read_timeout_milliseconds: self.read_timeout_milliseconds,
            },
            disable_stalled_stream_protection: self.disable_stalled_stream_protection,
            request_checksum_calculation,
            parallel_upload_semaphore,
            accelerate: self.target_accelerate,
            request_payer: target_request_payer,
        });

        (source_client_config, target_client_config)
    }
}

impl TryFrom<CLIArgs> for Config {
    type Error = String;

    fn try_from(value: CLIArgs) -> Result<Self, Self::Error> {
        value.validate_storage_config()?;

        let original_cloned_value = value.clone();

        let mut tracing_config = value.verbosity.log_level().map(|log_level| TracingConfig {
            tracing_level: log_level,
            json_tracing: value.json_tracing,
            aws_sdk_tracing: value.aws_sdk_tracing,
            span_events_tracing: value.span_events_tracing,
            disable_color_tracing: value.disable_color_tracing,
        });

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
        let include_content_type_regex = value
            .filter_include_content_type_regex
            .map(|regex| Regex::new(&regex).unwrap());
        let exclude_content_type_regex = value
            .filter_exclude_content_type_regex
            .map(|regex| Regex::new(&regex).unwrap());

        let include_metadata_regex = value
            .filter_include_metadata_regex
            .map(|regex| Regex::new(&regex).unwrap());
        let exclude_metadata_regex = value
            .filter_exclude_metadata_regex
            .map(|regex| Regex::new(&regex).unwrap());
        let include_tag_regex = value
            .filter_include_tag_regex
            .map(|regex| Regex::new(&regex).unwrap());
        let exclude_tag_regex = value
            .filter_exclude_tag_regex
            .map(|regex| Regex::new(&regex).unwrap());

        let rate_limit_bandwidth = value
            .rate_limit_bandwidth
            .map(|bandwidth| human_bytes::parse_human_bandwidth(&bandwidth).unwrap());

        let mut additional_checksum_algorithm = value
            .additional_checksum_algorithm
            .map(|algorithm| ChecksumAlgorithm::from(algorithm.as_str()));

        let check_additional_checksum_algorithm = value
            .check_additional_checksum
            .map(|algorithm| ChecksumAlgorithm::from(algorithm.as_str()));

        let check_mtime_and_additional_checksum = value
            .check_mtime_and_additional_checksum
            .map(|algorithm| ChecksumAlgorithm::from(algorithm.as_str()));

        let mut checksum_mode = if value.enable_additional_checksum {
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

        let mut full_object_checksum = if additional_checksum_algorithm
            .as_ref()
            .is_some_and(|algorithm| algorithm == &ChecksumAlgorithm::Crc64Nvme)
        {
            true
        } else {
            value.full_object_checksum
        };

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&value.source) {
            if is_express_onezone_storage(&bucket)
                && !value.disable_express_one_zone_additional_checksum
            {
                checksum_mode = Some(ChecksumMode::Enabled);
            }
        }

        let mut request_checksum_calculation = RequestChecksumCalculation::WhenRequired;
        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(&value.target) {
            if is_express_onezone_storage(&bucket)
                && additional_checksum_algorithm.is_none()
                && !value.disable_express_one_zone_additional_checksum
            {
                additional_checksum_algorithm = Some(ChecksumAlgorithm::Crc64Nvme);
                full_object_checksum = true;
                request_checksum_calculation = RequestChecksumCalculation::WhenSupported;
            } else if additional_checksum_algorithm.is_some() {
                request_checksum_calculation = RequestChecksumCalculation::WhenSupported;
            }
        }

        let (source_client_config, target_client_config) =
            original_cloned_value.build_client_configs(request_checksum_calculation);

        #[allow(unused_assignments)]
        #[allow(unused_mut)]
        let mut allow_e2e_test_dangerous_simulation = false;

        #[allow(unused_assignments)]
        #[allow(unused_mut)]
        let mut cancellation_point = None;
        #[allow(unused_assignments)]
        #[allow(unused_mut)]
        let mut panic_simulation_point = None;
        #[allow(unused_assignments)]
        #[allow(unused_mut)]
        let mut error_simulation_point = None;
        #[cfg(feature = "e2e_test_dangerous_simulations")]
        {
            allow_e2e_test_dangerous_simulation = value.allow_e2e_test_dangerous_simulation;
            cancellation_point = value.cancellation_point;
            panic_simulation_point = value.panic_simulation_point;
            error_simulation_point = value.error_simulation_point;
        }

        #[allow(unused_assignments)]
        #[allow(unused_mut)]
        let mut test_user_defined_callback = false;
        #[cfg(feature = "e2e_test_dangerous_simulations")]
        {
            test_user_defined_callback = value.test_user_defined_callback;
        }

        // If point-in-time is specified, we need to remove the modified filter.
        // Because target objects may need to be modified.
        let mut remove_modified_filter = if value.point_in_time.is_some() {
            true
        } else {
            value.remove_modified_filter
        };

        // Reporting need to be done in dry run mode.
        let dry_run = if value.report_sync_status {
            true
        } else {
            value.dry_run
        };

        // If report_sync_status is true, we need to remove the modified filter.
        // And must warn as error.
        let warn_as_error;
        let check_etag;
        let head_each_target;
        if value.report_sync_status {
            remove_modified_filter = true;
            warn_as_error = true;
            head_each_target = true;
            check_etag = check_additional_checksum_algorithm.is_none();
        } else {
            warn_as_error = value.warn_as_error;
            check_etag = value.check_etag;
            head_each_target = value.head_each_target;
        }

        if dry_run {
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

        cfg_if! {
            if #[cfg(feature = "lua_support")] {
                let preprocess_callback_lua_script =  value.preprocess_callback_lua_script.clone();
                let event_callback_lua_script = value.event_callback_lua_script.clone();
                let filter_callback_lua_script = value.filter_callback_lua_script.clone();
                let allow_lua_os_library = value.allow_lua_os_library;
                let allow_lua_unsafe_vm = value.allow_lua_unsafe_vm;
                let lua_vm_memory_limit = human_bytes::parse_human_bytes_without_low_limit(&value.lua_vm_memory_limit)? as usize;
            } else {
                let preprocess_callback_lua_script =  None;
                let event_callback_lua_script = None;
                let filter_callback_lua_script = None;
                let allow_lua_os_library = false;
                let allow_lua_unsafe_vm = false;
                let lua_vm_memory_limit = 0;
            }
        }

        #[allow(unused_mut)]
        let mut preprocess_manager = PreprocessManager::new();
        cfg_if! {
            if #[cfg(feature = "lua_support")] {
                if let Some(preprocess_callback_lua_script) = preprocess_callback_lua_script.as_ref() {
                    let mut lua_preprocess_callback = crate::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        lua_vm_memory_limit,
                        allow_lua_os_library,
                        allow_lua_unsafe_vm,
                    );
                    if let Err(e) =
                        lua_preprocess_callback.load_and_compile(preprocess_callback_lua_script.as_str())
                    {
                        return Err(LUA_SCRIPT_LOAD_ERROR.to_string() + &e.to_string());
                    }
                    preprocess_manager.register_callback(lua_preprocess_callback);
                }
            }
        }

        #[allow(unused_mut)]
        let mut event_manager = EventManager::new();
        cfg_if! {
            if #[cfg(feature = "lua_support")] {
                if let Some(event_callback_lua_script) = event_callback_lua_script.as_ref() {
                    let mut lua_event_callback = crate::callback::lua_event_callback::LuaEventCallback::new(
                        lua_vm_memory_limit,
                        allow_lua_os_library,
                        allow_lua_unsafe_vm,
                    );
                    if let Err(e) = lua_event_callback.load_and_compile(event_callback_lua_script.as_str())
                    {
                        return Err(LUA_SCRIPT_LOAD_ERROR.to_string() + &e.to_string());
                    }
                    // Lua event callback is registered for all events.
                    event_manager.register_callback(EventType::ALL_EVENTS, lua_event_callback, dry_run);
                }
            }
        }

        #[allow(unused_mut)]
        let mut filter_manager = FilterManager::new();
        cfg_if! {
            if #[cfg(feature = "lua_support")] {
                if let Some(filter_callback_lua_script) = filter_callback_lua_script.as_ref() {
                    let mut lua_filter_callback = crate::callback::lua_filter_callback::LuaFilterCallback::new(
                        lua_vm_memory_limit,
                        allow_lua_os_library,
                        allow_lua_unsafe_vm,
                    );
                    if let Err(e) =
                        lua_filter_callback.load_and_compile(filter_callback_lua_script.as_str())
                    {
                        return Err(LUA_SCRIPT_LOAD_ERROR.to_string() + &e.to_string());
                    }
                    filter_manager.register_callback(lua_filter_callback);
                }
            }
        }

        Ok(Config {
            source: storage_path::parse_storage_path(&value.source),
            target: storage_path::parse_storage_path(&value.target),

            show_no_progress: value.show_no_progress,

            source_client_config,
            target_client_config,

            tracing_config,

            force_retry_config: ForceRetryConfig {
                force_retry_count: value.force_retry_count,
                force_retry_interval_milliseconds: value.force_retry_interval_milliseconds,
            },

            transfer_config: TransferConfig {
                multipart_threshold: human_bytes::parse_human_bytes(&value.multipart_threshold)?,
                multipart_chunksize: human_bytes::parse_human_bytes(&value.multipart_chunksize)?,
                auto_chunksize: value.auto_chunksize,
            },

            worker_size: value.worker_size,

            warn_as_error,
            follow_symlinks: !value.ignore_symlinks,
            head_each_target,
            sync_with_delete: value.delete,
            disable_tagging: value.disable_tagging,
            sync_latest_tagging: value.sync_latest_tagging,
            server_side_copy: value.server_side_copy,
            no_guess_mime_type: value.no_guess_mime_type,
            disable_multipart_verify: value.disable_multipart_verify,
            disable_etag_verify: value.disable_etag_verify,
            disable_additional_checksum_verify: value.disable_additional_checksum_verify,
            enable_versioning: value.enable_versioning,
            point_in_time: value.point_in_time,
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
            dry_run,
            rate_limit_objects: value.rate_limit_objects,
            rate_limit_bandwidth,
            max_parallel_listings: value.max_parallel_listings,
            object_listing_queue_size: value.object_listing_queue_size,
            max_parallel_listing_max_depth: value.max_parallel_listing_max_depth,
            allow_parallel_listings_in_express_one_zone: value
                .allow_parallel_listings_in_express_one_zone,
            cache_control: value.cache_control,
            content_disposition: value.content_disposition,
            content_encoding: value.content_encoding,
            content_language: value.content_language,
            content_type: value.content_type,
            expires: value.expires,
            metadata,
            website_redirect: value.website_redirect,
            no_sync_system_metadata: value.no_sync_system_metadata,
            no_sync_user_defined_metadata: value.no_sync_user_defined_metadata,
            tagging,
            filter_config: FilterConfig {
                before_time: value.filter_mtime_before,
                after_time: value.filter_mtime_after,
                remove_modified_filter,
                check_size: value.check_size,
                check_etag,
                check_mtime_and_etag: value.check_mtime_and_etag,
                check_checksum_algorithm: check_additional_checksum_algorithm,
                check_mtime_and_additional_checksum,
                include_regex,
                exclude_regex,
                include_content_type_regex,
                exclude_content_type_regex,
                include_metadata_regex,
                exclude_metadata_regex,
                include_tag_regex,
                exclude_tag_regex,
                larger_size: filter_larger_size,
                smaller_size: filter_smaller_size,
                filter_manager,
            },
            max_keys: value.max_keys,
            put_last_modified_metadata: value.put_last_modified_metadata,
            auto_complete_shell: value.auto_complete_shell,
            disable_payload_signing: value.disable_payload_signing,
            disable_content_md5_header: value.disable_content_md5_header,
            delete_excluded: value.delete_excluded,
            full_object_checksum,
            allow_e2e_test_dangerous_simulation,
            test_user_defined_callback,
            cancellation_point,
            panic_simulation_point,
            error_simulation_point,
            source_accelerate: value.source_accelerate,
            target_accelerate: value.target_accelerate,
            source_request_payer: value.source_request_payer,
            target_request_payer: value.target_request_payer,
            report_sync_status: value.report_sync_status,
            report_metadata_sync_status: value.report_metadata_sync_status,
            report_tagging_sync_status: value.report_tagging_sync_status,
            event_manager,
            preprocess_manager,
            preprocess_callback_lua_script,
            event_callback_lua_script,
            filter_callback_lua_script,
            allow_lua_os_library,
            allow_lua_unsafe_vm,
            lua_vm_memory_limit,
            if_match: value.if_match,
            copy_source_if_match: value.copy_source_if_match,
            max_delete: value.max_delete,
        })
    }
}

fn is_express_onezone_storage(bucket: &str) -> bool {
    bucket.ends_with(EXPRESS_ONEZONE_STORAGE_SUFFIX)
}
