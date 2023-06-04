use std::sync::Arc;

use anyhow::Result;
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::types::{ChecksumMode, ObjectPart, ObjectVersion, Tagging};
use aws_sdk_s3::Client;
use aws_smithy_http::body::SdkBody;
use aws_smithy_http::byte_stream::ByteStream;
use dyn_clone::DynClone;
use hyper::Body;
use leaky_bucket::RateLimiter;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::ReaderStream;

use crate::config::ClientConfig;
use crate::storage::checksum::AdditionalChecksum;
use crate::types::async_callback::AsyncReadWithCallback;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, S3syncObject, SseCustomerKey, StoragePath, SyncStatistics};
use crate::Config;

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
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        client_config: Option<ClientConfig>,
        rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    ) -> Storage;
}

#[async_trait]
pub trait StorageTrait: DynClone {
    fn is_local_storage(&self) -> bool;
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
    async fn get_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
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
    async fn put_object(
        &self,
        key: &str,
        get_object_output: GetObjectOutput,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
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
}

pub fn convert_to_buf_byte_stream_with_callback<R>(
    byte_stream: R,
    stats_sender: Sender<SyncStatistics>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    additional_checksum: Option<Arc<AdditionalChecksum>>,
    object_checksum: Option<ObjectChecksum>,
) -> ByteStream
where
    R: AsyncRead + Send + 'static,
{
    ByteStream::new(SdkBody::from(Body::wrap_stream(ReaderStream::new(
        BufReader::new(AsyncReadWithCallback::new(
            byte_stream,
            stats_sender,
            rate_limit_bandwidth,
            additional_checksum,
            object_checksum,
        )),
    ))))
}

pub fn get_size_string_from_content_range(get_object_output: &GetObjectOutput) -> String {
    let content_length_str = get_object_output.content_length().to_string();
    let size = if get_object_output.content_range().is_some() {
        // example: bytes 200-1000/67589　67589 will be returned
        get_object_output
            .content_range()
            .unwrap()
            .split('/')
            .collect::<Vec<&str>>()[1]
    } else {
        &content_length_str
    };

    size.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_size_string_from_content_range_test() {
        init_dummy_tracing_subscriber();

        let get_object_output = GetObjectOutput::builder()
            .content_range("bytes 200-1000/67589")
            .build();
        assert_eq!(
            get_size_string_from_content_range(&get_object_output),
            "67589".to_string()
        );

        let get_object_output = GetObjectOutput::builder()
            .content_range("bytes 200-1000/*")
            .build();
        assert_eq!(
            get_size_string_from_content_range(&get_object_output),
            "*".to_string()
        );
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
