use std::collections::HashMap;

use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ObjectCannedAcl, ServerSideEncryption, StorageClass,
};
use chrono::{DateTime, Utc};
use regex::Regex;

use crate::types::{ClientConfigLocation, S3Credentials, SseCustomerKey, SseKmsKeyId, StoragePath};

pub mod args;

#[derive(Debug, Clone)]
pub struct Config {
    pub source: StoragePath,
    pub target: StoragePath,
    pub source_client_config: Option<ClientConfig>,
    pub target_client_config: Option<ClientConfig>,
    pub force_retry_config: ForceRetryConfig,
    pub tracing_config: Option<TracingConfig>,
    pub transfer_config: TransferConfig,
    pub worker_size: u16,
    pub warn_as_error: bool,
    pub follow_symlinks: bool,
    pub head_each_target: bool,
    pub sync_with_delete: bool,
    pub disable_tagging: bool,
    pub sync_latest_tagging: bool,
    pub no_guess_mime_type: bool,
    pub disable_multipart_verify: bool,
    pub disable_etag_verify: bool,
    pub enable_versioning: bool,
    pub storage_class: Option<StorageClass>,
    pub sse: Option<ServerSideEncryption>,
    pub sse_kms_key_id: SseKmsKeyId,
    pub source_sse_c: Option<String>,
    pub source_sse_c_key: SseCustomerKey,
    pub source_sse_c_key_md5: Option<String>,
    pub target_sse_c: Option<String>,
    pub target_sse_c_key: SseCustomerKey,
    pub target_sse_c_key_md5: Option<String>,
    pub canned_acl: Option<ObjectCannedAcl>,
    pub additional_checksum_mode: Option<ChecksumMode>,
    pub additional_checksum_algorithm: Option<ChecksumAlgorithm>,
    pub dry_run: bool,
    pub rate_limit_objects: Option<u32>,
    pub rate_limit_bandwidth: Option<u64>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<DateTime<Utc>>,
    pub metadata: Option<HashMap<String, String>>,
    pub tagging: Option<String>,
    pub filter_config: FilterConfig,
    pub put_last_modified_metadata: bool,
    pub max_keys: i32,
    pub auto_complete_shell: Option<clap_complete::shells::Shell>,
}

impl Config {
    pub fn is_sha1_digest_listing_required(&self) -> bool {
        is_sha1_digest_listing_required(self.sync_with_delete)
    }
}

fn is_sha1_digest_listing_required(sync_with_delete: bool) -> bool {
    !sync_with_delete
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub client_config_location: ClientConfigLocation,
    pub credential: S3Credentials,
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub retry_config: RetryConfig,
    pub https_proxy: Option<String>,
    pub http_proxy: Option<String>,
    pub no_verify_ssl: bool,
    pub disable_stalled_stream_protection: bool,
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub aws_max_attempts: u32,
    pub initial_backoff_milliseconds: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct TracingConfig {
    pub tracing_level: log::Level,
    pub json_tracing: bool,
    pub aws_sdk_tracing: bool,
    pub span_events_tracing: bool,
    pub disable_color_tracing: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ForceRetryConfig {
    pub force_retry_count: u32,
    pub force_retry_interval_milliseconds: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct TransferConfig {
    pub multipart_threshold: u64,
    pub multipart_chunksize: u64,
    pub auto_chunksize: bool,
}

impl TransferConfig {
    pub fn is_multipart_upload_required(&self, content_length: u64) -> bool {
        self.multipart_threshold <= content_length
    }
}

#[derive(Debug, Clone, Default)]
pub struct FilterConfig {
    pub before_time: Option<DateTime<Utc>>,
    pub after_time: Option<DateTime<Utc>>,
    pub remove_modified_filter: bool,
    pub check_size: bool,
    pub include_regex: Option<Regex>,
    pub exclude_regex: Option<Regex>,
    pub larger_size: Option<u64>,
    pub smaller_size: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_multipart_upload_necessary() {
        init_dummy_tracing_subscriber();

        let transfer_config = TransferConfig {
            multipart_threshold: 8 * 1024 * 1024,
            multipart_chunksize: 8 * 1024 * 1024,
            auto_chunksize: false,
        };

        assert!(transfer_config.is_multipart_upload_required(8 * 1024 * 1024));
        assert!(transfer_config.is_multipart_upload_required((8 * 1024 * 1024) + 1));
        assert!(!transfer_config.is_multipart_upload_required((8 * 1024 * 1024) - 1));
    }

    #[test]
    fn is_sha1_digest_listing_required_test() {
        init_dummy_tracing_subscriber();

        assert!(is_sha1_digest_listing_required(false));
        assert!(!is_sha1_digest_listing_required(true));
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
