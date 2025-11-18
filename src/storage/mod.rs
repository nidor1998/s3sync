use anyhow::{Result, anyhow};
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::copy_object::CopyObjectOutput;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumMode, ObjectPart, ObjectVersion, RequestPayer, Tagging};
use aws_smithy_types::body::SdkBody;
use dyn_clone::DynClone;
use futures_util::stream::TryStreamExt;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use leaky_bucket::RateLimiter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::ReaderStream;

use crate::Config;
use crate::config::ClientConfig;
use crate::storage::checksum::AdditionalChecksum;
use crate::types::async_callback::AsyncReadWithCallback;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, S3syncObject, SseCustomerKey, StoragePath, SyncStatistics};

pub mod additional_checksum_verify;
pub mod checksum;
pub mod e_tag_verify;
pub mod local;
pub mod s3;

pub type Storage = Box<dyn StorageTrait + Send + Sync>;

pub struct StoragePair {
    pub source: Storage,
    pub target: Storage,
}

#[async_trait]
pub trait StorageFactory {
    #[allow(clippy::too_many_arguments)]
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        client_config: Option<ClientConfig>,
        request_payer: Option<RequestPayer>,
        rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
        has_warning: Arc<AtomicBool>,
        object_to_list: Option<String>,
    ) -> Storage;
}

#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait StorageTrait: DynClone {
    fn is_local_storage(&self) -> bool;
    fn is_express_onezone_storage(&self) -> bool;
    async fn list_objects(
        &self,
        sender: &Sender<S3syncObject>,
        max_keys: i32,
        warn_as_error: bool,
    ) -> Result<()>;
    async fn list_object_versions(
        &self,
        sender: &Sender<S3syncObject>,
        max_keys: i32,
        warn_as_error: bool,
    ) -> Result<()>;
    #[allow(clippy::too_many_arguments)]
    async fn get_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<GetObjectOutput>;
    async fn get_object_versions(&self, key: &str, max_keys: i32) -> Result<Vec<ObjectVersion>>;
    async fn get_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
    ) -> Result<GetObjectTaggingOutput>;
    async fn head_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput>;
    async fn head_object_first_part(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput>;

    async fn get_object_parts(
        &self,
        key: &str,
        version_id: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>>;
    async fn get_object_parts_attributes(
        &self,
        key: &str,
        version_id: Option<String>,
        max_parts: i32,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>>;
    #[allow(clippy::too_many_arguments)]
    async fn put_object(
        &self,
        key: &str,
        source: Storage,
        source_size: u64,
        source_additional_checksum: Option<String>,
        get_object_output_first_chunk: GetObjectOutput,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        copy_source_if_match: Option<String>,
    ) -> Result<PutObjectOutput>;
    async fn put_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
        tagging: Tagging,
    ) -> Result<PutObjectTaggingOutput>;
    async fn delete_object(
        &self,
        key: &str,
        version_id: Option<String>,
        if_match: Option<String>,
    ) -> Result<DeleteObjectOutput>;
    async fn delete_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
    ) -> Result<DeleteObjectTaggingOutput>;
    async fn is_versioning_enabled(&self) -> Result<bool>;
    fn get_client(&self) -> Option<Arc<Client>>;
    fn get_stats_sender(&self) -> Sender<SyncStatistics>;
    async fn send_stats(&self, stats: SyncStatistics);
    fn get_local_path(&self) -> PathBuf;
    fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>>;
    fn generate_copy_source_key(&self, key: &str, version_id: Option<String>) -> String;
    fn set_warning(&self);
}

#[rustfmt::skip] // For coverage tool incorrectness
pub fn convert_to_buf_byte_stream_with_callback<R>(
    byte_stream: R,
    stats_sender: Sender<SyncStatistics>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    additional_checksum: Option<Arc<AdditionalChecksum>>,
    object_checksum: Option<ObjectChecksum>,
) -> ByteStream
where
    R: AsyncRead + Send + 'static + Sync,
{
    let async_read = AsyncReadWithCallback::new(byte_stream, stats_sender, rate_limit_bandwidth, additional_checksum, object_checksum);

    let buf_reader = BufReader::new(async_read);

    let reader_stream = ReaderStream::new(buf_reader).map_ok(Frame::data);

    let stream_body = StreamBody::new(reader_stream);

    let boxed_body = BodyExt::boxed(stream_body);

    let sdk_body = SdkBody::from_body_1_x(boxed_body);

    ByteStream::new(sdk_body)
}

pub fn get_range_from_content_range(get_object_output: &GetObjectOutput) -> Option<(u64, u64)> {
    let content_range = get_object_output.content_range()?;
    let parts: Vec<&str> = content_range.split_whitespace().collect();
    if parts.len() == 2 {
        let range_parts: Vec<&str> = parts[1].split('/').collect();
        if range_parts.len() == 2 {
            let byte_range: Vec<&str> = range_parts[0].split('-').collect();
            if byte_range.len() == 2 {
                let start = byte_range[0].parse::<u64>().ok()?;
                let end = byte_range[1].parse::<u64>().ok()?;
                return Some((start, end));
            }
        }
    }

    None
}

pub fn convert_head_to_get_object_output(head_object_output: HeadObjectOutput) -> GetObjectOutput {
    GetObjectOutput::builder()
        .set_accept_ranges(head_object_output.accept_ranges().map(|s| s.to_string()))
        .set_body(Some(ByteStream::from(vec![])))
        .set_bucket_key_enabled(head_object_output.bucket_key_enabled())
        .set_cache_control(head_object_output.cache_control().map(|s| s.to_string()))
        .set_checksum_crc32(head_object_output.checksum_crc32().map(|s| s.to_string()))
        .set_checksum_crc32_c(head_object_output.checksum_crc32_c().map(|s| s.to_string()))
        .set_checksum_crc64_nvme(
            head_object_output
                .checksum_crc64_nvme()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha1(head_object_output.checksum_sha1().map(|s| s.to_string()))
        .set_checksum_sha256(head_object_output.checksum_sha256().map(|s| s.to_string()))
        .set_checksum_type(head_object_output.checksum_type().cloned())
        .set_content_disposition(
            head_object_output
                .content_disposition()
                .map(|s| s.to_string()),
        )
        .set_content_encoding(head_object_output.content_encoding().map(|s| s.to_string()))
        .set_content_language(head_object_output.content_language().map(|s| s.to_string()))
        .set_content_length(head_object_output.content_length())
        .set_content_range(head_object_output.content_range().map(|s| s.to_string()))
        .set_content_type(head_object_output.content_type().map(|s| s.to_string()))
        .set_delete_marker(head_object_output.delete_marker())
        .set_e_tag(head_object_output.e_tag().map(|s| s.to_string()))
        .set_expiration(head_object_output.expiration().map(|s| s.to_string()))
        .set_expires_string(head_object_output.expires_string().map(|s| s.to_string()))
        .set_last_modified(head_object_output.last_modified().cloned())
        .set_metadata(head_object_output.metadata().cloned())
        .set_missing_meta(head_object_output.missing_meta())
        .set_object_lock_legal_hold_status(
            head_object_output.object_lock_legal_hold_status().cloned(),
        )
        .set_object_lock_mode(head_object_output.object_lock_mode().cloned())
        .set_object_lock_retain_until_date(
            head_object_output.object_lock_retain_until_date().cloned(),
        )
        .set_parts_count(head_object_output.parts_count())
        .set_replication_status(head_object_output.replication_status().cloned())
        .set_request_charged(head_object_output.request_charged().cloned())
        .set_restore(head_object_output.restore().map(|s| s.to_string()))
        .set_server_side_encryption(head_object_output.server_side_encryption().cloned())
        .set_sse_customer_algorithm(
            head_object_output
                .sse_customer_algorithm()
                .map(|s| s.to_string()),
        )
        .set_sse_customer_key_md5(
            head_object_output
                .sse_customer_key_md5()
                .map(|s| s.to_string()),
        )
        .set_ssekms_key_id(head_object_output.ssekms_key_id().map(|s| s.to_string()))
        .set_storage_class(head_object_output.storage_class().cloned())
        .set_tag_count(head_object_output.tag_count())
        .set_version_id(head_object_output.version_id().map(|s| s.to_string()))
        .set_website_redirect_location(
            head_object_output
                .website_redirect_location()
                .map(|s| s.to_string()),
        )
        .build()
}

pub fn convert_copy_to_put_object_output(
    copy_object_output: CopyObjectOutput,
    size: i64,
) -> PutObjectOutput {
    PutObjectOutput::builder()
        .set_bucket_key_enabled(copy_object_output.bucket_key_enabled())
        .set_checksum_crc32(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_crc32()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc32_c(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_crc32_c()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc64_nvme(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_crc64_nvme()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha1(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_sha1()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha256(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_sha256()
                .map(|s| s.to_string()),
        )
        .set_checksum_type(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_type()
                .cloned(),
        )
        .set_e_tag(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .e_tag()
                .map(|s| s.to_string()),
        )
        .set_expiration(
            copy_object_output
                .clone()
                .expiration()
                .map(|s| s.to_string()),
        )
        .set_request_charged(copy_object_output.request_charged().cloned())
        .set_server_side_encryption(copy_object_output.server_side_encryption().cloned())
        .set_size(Some(size))
        .set_sse_customer_algorithm(
            copy_object_output
                .sse_customer_algorithm()
                .map(|s| s.to_string()),
        )
        .set_sse_customer_key_md5(
            copy_object_output
                .sse_customer_key_md5()
                .map(|s| s.to_string()),
        )
        .set_ssekms_encryption_context(
            copy_object_output
                .ssekms_encryption_context()
                .map(|s| s.to_string()),
        )
        .set_ssekms_key_id(copy_object_output.ssekms_key_id().map(|s| s.to_string()))
        .set_version_id(copy_object_output.version_id().map(|s| s.to_string()))
        .build()
}

pub fn convert_copy_to_upload_part_output(
    upload_part_copy_output: UploadPartCopyOutput,
) -> UploadPartOutput {
    UploadPartOutput::builder()
        .set_server_side_encryption(upload_part_copy_output.server_side_encryption().cloned())
        .set_e_tag(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .e_tag()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc32(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_crc32()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc32_c(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_crc32_c()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc64_nvme(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_crc64_nvme()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha1(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_sha1()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha256(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_sha256()
                .map(|s| s.to_string()),
        )
        .set_sse_customer_algorithm(
            upload_part_copy_output
                .sse_customer_algorithm()
                .map(|s| s.to_string()),
        )
        .set_sse_customer_key_md5(
            upload_part_copy_output
                .sse_customer_key_md5()
                .map(|s| s.to_string()),
        )
        .set_ssekms_key_id(
            upload_part_copy_output
                .ssekms_key_id()
                .map(|s| s.to_string()),
        )
        .set_bucket_key_enabled(upload_part_copy_output.bucket_key_enabled())
        .set_request_charged(upload_part_copy_output.request_charged().cloned())
        .build()
}

#[derive(Clone)]
pub struct FileRange {
    pub offset: u64,
    pub size: u64,
}

pub fn parse_range_header(range_header: &str) -> Result<FileRange> {
    if !range_header.starts_with("bytes=") {
        return Err(anyhow!(
            "Range header must start with 'bytes=': {}",
            range_header
        ));
    }

    let range = range_header.trim_start_matches("bytes=");
    let parts: Vec<_> = range.split('-').collect();
    if parts.len() != 2 {
        return Err(anyhow!("Invalid range format: {}", range));
    }

    let offset = parts[0].parse::<u64>()?;
    let size = if parts[1].is_empty() {
        return Err(anyhow!("Invalid range format: {}", range));
    } else {
        let end = parts[1].parse::<u64>()?;
        if end < offset {
            return Err(anyhow!("End of range cannot be less than start: {}", range));
        }
        end - offset + 1
    };

    Ok(FileRange { offset, size })
}

pub fn parse_range_header_string(range: &str) -> Option<(u64, u64)> {
    let parts: Vec<&str> = range.trim_start_matches("bytes=").split('-').collect();
    if parts.len() == 2 {
        let start = parts[0].parse::<u64>().ok()?;
        let end = parts[1].parse::<u64>().ok()?;
        return Some((start, end));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::EnvFilter;

    #[test]
    fn get_range_from_content_range_test() {
        init_dummy_tracing_subscriber();

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-1000/67589")
            .build();
        let (start, end) = get_range_from_content_range(&get_object_output).unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 1000);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-0/67589")
            .build();
        let (start, end) = get_range_from_content_range(&get_object_output).unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 0);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 500-999/67589")
            .build();
        let (start, end) = get_range_from_content_range(&get_object_output).unwrap();
        assert_eq!(start, 500);
        assert_eq!(end, 999);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 500-999/*")
            .build();
        let (start, end) = get_range_from_content_range(&get_object_output).unwrap();
        assert_eq!(start, 500);
        assert_eq!(end, 999);
    }

    #[test]
    fn get_range_from_content_range_error_test() {
        init_dummy_tracing_subscriber();

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("0-1000/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-1000")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes -1000/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes a-1000/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-a/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);
    }

    #[test]
    fn test_parse_range_header() {
        let range = parse_range_header("bytes=55-120").unwrap();
        assert_eq!(range.offset, 55);
        assert_eq!(range.size, 66);

        assert!(parse_range_header("bytes=65-65").is_ok());
    }

    #[test]
    fn test_parse_range_header_error() {
        assert!(parse_range_header("0-55").is_err());
        assert!(parse_range_header("bytes=0-").is_err());
        assert!(parse_range_header("bytes=-55").is_err());
        assert!(parse_range_header("bytes=60-55").is_err());
        assert!(parse_range_header("bytes=65-64").is_err());
    }

    #[test]
    fn test_parse_range_header_string() {
        let (start, end) = parse_range_header_string("bytes=55-120").unwrap();
        assert_eq!(start, 55);
        assert_eq!(end, 120);

        assert!(parse_range_header("bytes=65-65").is_ok());
    }

    #[test]
    fn test_parse_range_header_string_error() {
        assert!(parse_range_header_string("bytes=0-").is_none());
        assert!(parse_range_header_string("bytes=-55").is_none());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
