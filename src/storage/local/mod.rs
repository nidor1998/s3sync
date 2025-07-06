use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput;
use aws_sdk_s3::operation::get_object::builders::GetObjectOutputBuilder;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::builders::HeadObjectOutputBuilder;
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, Object, ObjectPart, ObjectVersion, RequestPayer,
    ServerSideEncryption, StorageClass, Tagging,
};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::Length;
use aws_smithy_types_convert::date_time::DateTimeExt;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use leaky_bucket::RateLimiter;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{debug, info, trace, warn};
use walkdir::{DirEntry, WalkDir};

use crate::config::ClientConfig;
use crate::storage::additional_checksum_verify::{
    generate_checksum_from_path, generate_checksum_from_path_with_chunksize,
    is_multipart_upload_checksum,
};
use crate::storage::e_tag_verify::{
    generate_e_tag_hash_from_path, generate_e_tag_hash_from_path_with_auto_chunksize,
    is_multipart_upload_e_tag, verify_e_tag,
};
use crate::storage::{
    convert_to_buf_byte_stream_with_callback, get_size_string_from_content_range, Storage,
    StorageFactory, StorageTrait,
};
use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use crate::types::SyncStatistics::{ChecksumVerified, ETagVerified, SyncBytes, SyncWarning};
use crate::types::{
    is_full_object_checksum, ObjectChecksum, S3syncObject, SseCustomerKey, StoragePath,
    SyncStatistics,
};
use crate::{storage, Config};

pub mod fs_util;

const MISMATCH_WARNING_WITH_HELP: &str = "mismatch. object in the local storage may be corrupted. \
 or the current multipart_threshold or multipart_chunksize may be different when uploading to the source. \
 To suppress this warning, please add --disable-multipart-verify command line option. \
 To resolve this issue, please add --auto-chunksize command line option(but extra API overheads).";

pub struct LocalStorageFactory {}

#[async_trait]
impl StorageFactory for LocalStorageFactory {
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        _client_config: Option<ClientConfig>,
        _request_payer: Option<RequestPayer>,
        rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    ) -> Storage {
        LocalStorage::create(
            config,
            path,
            cancellation_token,
            stats_sender,
            rate_limit_objects_per_sec,
            rate_limit_bandwidth,
        )
        .await
    }
}

#[derive(Clone)]
struct LocalStorage {
    config: Config,
    path: PathBuf,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
    rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
}

impl LocalStorage {
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    ) -> Storage {
        let local_path = if let StoragePath::Local(local_path) = path {
            local_path
        } else {
            panic!("local path not found")
        };

        let storage = LocalStorage {
            config,
            path: local_path,
            cancellation_token,
            stats_sender,
            rate_limit_objects_per_sec,
            rate_limit_bandwidth,
        };

        Box::new(storage)
    }

    async fn check_dir_entry(&self, entry: &DirEntry, warn_as_error: bool) -> Result<bool> {
        if entry.file_type().is_dir() || entry.file_type().is_symlink() {
            return Ok(false);
        }

        let regular_file_check_result = fs_util::is_regular_file(&entry.path().to_path_buf()).await;
        if let Err(e) = regular_file_check_result {
            let path = entry.path().to_str().unwrap();

            self.send_stats(SyncWarning {
                key: path.to_string(),
            })
            .await;

            let path = entry.path().to_str().unwrap();
            let error = e.to_string();
            let source = e.source();

            warn!(
                path = path,
                error = error,
                source = source,
                "failed to access file."
            );

            if warn_as_error {
                return Err(anyhow!("failed to is_regular_file(): {:?}.", e));
            }

            return Ok(false);
        }
        if !regular_file_check_result? {
            let path = entry.path().to_str().unwrap();

            debug!(path = path, "skip non regular file.");

            return Ok(false);
        }

        Ok(true)
    }

    async fn exec_rate_limit_objects_per_sec(&self) {
        if self.rate_limit_objects_per_sec.is_some() {
            self.rate_limit_objects_per_sec
                .as_ref()
                .unwrap()
                .acquire(1)
                .await;
        }
    }

    // I can't find a way to simplify this function.
    #[allow(clippy::too_many_arguments)]
    async fn verify_local_file(
        &self,
        key: &str,
        object_checksum: Option<ObjectChecksum>,
        source_sse: &Option<ServerSideEncryption>,
        source_e_tag: &Option<String>,
        source_content_length: u64,
        source_final_checksum: Option<String>,
        source_checksum_algorithm: Option<ChecksumAlgorithm>,
        real_path: &PathBuf,
        target_object_parts: Option<Vec<ObjectPart>>,
        target_content_length: u64,
        source_express_onezone_storage: bool,
    ) -> Result<()> {
        let key = key.to_string();
        if !self.config.disable_etag_verify && !source_express_onezone_storage {
            trace!(
                key = key,
                size = source_content_length,
                "download completed. start to etag verify. depends on the size, this may take a while.",
            );

            let target_sse = None;
            let target_e_tag = if let Some(parts) = target_object_parts.as_ref() {
                // If the source object is a multipart upload, we need to calculate the ETag from the parts.
                if is_multipart_upload_e_tag(source_e_tag) {
                    Some(
                        generate_e_tag_hash_from_path_with_auto_chunksize(
                            real_path,
                            parts.iter().map(|part| part.size().unwrap()).collect(),
                        )
                        .await?,
                    )
                } else {
                    // If the source object is not a multipart upload, we need to calculate the ETag from the whole file.
                    Some(
                        generate_e_tag_hash_from_path(
                            real_path,
                            source_content_length as usize + 1,
                            source_content_length as usize + 1,
                        )
                        .await?,
                    )
                }
            } else {
                Some(
                    generate_e_tag_hash_from_path(
                        real_path,
                        self.config.transfer_config.multipart_chunksize as usize,
                        self.config.transfer_config.multipart_threshold as usize,
                    )
                    .await?,
                )
            };

            let verify_result = verify_e_tag(
                !self.config.disable_multipart_verify,
                &self.config.source_sse_c,
                &self.config.target_sse_c,
                source_sse,
                source_e_tag,
                &target_sse,
                &target_e_tag,
            );

            if let Some(e_tag_match) = verify_result {
                if !e_tag_match {
                    if (source_content_length == target_content_length)
                        && (is_multipart_upload_e_tag(source_e_tag)
                            && self.config.disable_multipart_verify)
                    {
                        trace!(
                            key = &key,
                            source_e_tag = source_e_tag,
                            target_e_tag = target_e_tag,
                            "skip e_tag verification."
                        );
                    } else {
                        let message = if source_content_length
                            == fs_util::get_file_size(real_path).await
                            && is_multipart_upload_e_tag(source_e_tag)
                            && object_checksum
                                .clone()
                                .unwrap_or_default()
                                .object_parts
                                .is_none()
                        {
                            format!("{} {}", "e_tag", MISMATCH_WARNING_WITH_HELP)
                        } else {
                            "e_tag mismatch. file in the local storage may be corrupted."
                                .to_string()
                        };

                        let source_e_tag = source_e_tag.clone().unwrap();
                        let target_e_tag = target_e_tag.clone().unwrap();
                        warn!(
                            key = key,
                            source_e_tag = source_e_tag,
                            target_e_tag = target_e_tag,
                            message
                        );

                        self.send_stats(SyncWarning { key: key.clone() }).await;
                    }
                } else {
                    let source_e_tag = source_e_tag.clone().unwrap();
                    let target_e_tag = target_e_tag.clone().unwrap();
                    trace!(
                        key = &key,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        "e_tag verified."
                    );

                    self.send_stats(ETagVerified { key: key.clone() }).await;
                }
            }
        } else if source_content_length != target_content_length {
            warn!(
                key = &key,
                source_content_length = source_content_length,
                target_content_length = target_content_length,
                "content length mismatch. file in the local storage may be corrupted."
            );

            self.send_stats(SyncWarning { key: key.clone() }).await;
        }

        // Since aws-sdk-s3 1.69.0, the checksum mode is always enabled,
        // and cannot be disabled(maybe).
        // So, s3sync check the checksum mode is enabled by the user.
        if self.config.additional_checksum_mode.is_none() {
            return Ok(());
        }

        if let Some(source_final_checksum) = source_final_checksum {
            trace!(
                key = &key,
                size = source_content_length,
                "start to additional checksum verify. depends on the size, this may take a while.",
            );

            let parts = if let Some(parts) = target_object_parts.as_ref() {
                parts
                    .iter()
                    .map(|part| part.size().unwrap())
                    .collect::<Vec<i64>>()
            } else {
                vec![source_content_length as i64]
            };

            // If the source object is not a multipart upload, we need to calculate the checksum whole the file.
            let multipart_threshold =
                if !is_multipart_upload_checksum(&Some(source_final_checksum.clone())) {
                    source_content_length as usize + 1
                } else {
                    // If the source object is a multipart upload, and first chunk size is equal to the first part size,
                    // We adjust the multipart threshold to the first part size.
                    if source_content_length == (*parts.clone().first().unwrap() as u64) {
                        source_content_length as usize
                    } else {
                        self.config.transfer_config.multipart_threshold as usize
                    }
                };

            let target_final_checksum = generate_checksum_from_path(
                real_path,
                source_checksum_algorithm.as_ref().unwrap().clone(),
                parts,
                multipart_threshold,
                is_full_object_checksum(&Some(source_final_checksum.clone())),
            )
            .await?;

            let additional_checksum_algorithm =
                source_checksum_algorithm.as_ref().unwrap().as_str();

            if source_final_checksum != target_final_checksum {
                warn!(
                    key = key,
                    additional_checksum_algorithm = additional_checksum_algorithm,
                    source_final_checksum = source_final_checksum,
                    target_final_checksum = target_final_checksum,
                    "additional checksum mismatch. file in the local storage may be corrupted."
                );

                self.send_stats(SyncWarning { key }).await;
            } else {
                trace!(
                    key = &key,
                    additional_checksum_algorithm = additional_checksum_algorithm,
                    source_final_checksum = source_final_checksum,
                    target_final_checksum = target_final_checksum,
                    "additional checksum verified."
                );

                self.send_stats(ChecksumVerified { key: key.clone() }).await;
            }
        }
        Ok(())
    }

    async fn put_object_single_part(
        &self,
        key: &str,
        source: Storage,
        get_object_output: GetObjectOutput,
        _tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Result<PutObjectOutput> {
        let source_sse = get_object_output.server_side_encryption().cloned();
        let source_e_tag = get_object_output.e_tag().map(|e_tag| e_tag.to_string());
        let source_content_length = get_object_output.content_length().unwrap() as u64;
        let source_final_checksum = if let Some(object_checksum) = object_checksum.as_ref() {
            object_checksum.final_checksum.clone()
        } else {
            None
        };
        let source_checksum_algorithm = if let Some(object_checksum) = object_checksum.as_ref() {
            object_checksum.checksum_algorithm.clone()
        } else {
            None
        };
        let source_storage_class = get_object_output.storage_class().cloned();

        let source_last_modified =
            DateTime::from_millis(get_object_output.last_modified.unwrap().to_millis()?)
                .to_chrono_utc()?
                .to_rfc3339();

        if fs_util::check_directory_traversal(key) {
            return Err(anyhow!(S3syncError::DirectoryTraversalError));
        }

        if self.config.dry_run {
            // In a dry run, content-range is set.
            let content_length_string = get_size_string_from_content_range(&get_object_output);

            self.send_stats(SyncBytes(
                u64::from_str(&content_length_string).unwrap_or_default(),
            ))
            .await;

            let real_path = fs_util::key_to_file_path(self.path.to_path_buf(), key)
                .to_string_lossy()
                .to_string();
            info!(
                key = key,
                real_path = real_path,
                source_last_modified = source_last_modified,
                size = content_length_string,
                "[dry-run] sync completed.",
            );

            return Ok(PutObjectOutput::builder().build());
        }

        if fs_util::is_key_a_directory(key) {
            fs_util::create_directory_hierarchy_from_key(self.path.clone(), key).await?;

            return Ok(PutObjectOutput::builder().build());
        }

        let mut temp_file = fs_util::create_temp_file_from_key(&self.path, key).await?;
        let mut file = tokio::fs::File::from_std(temp_file.as_file_mut().try_clone()?);

        let seconds = get_object_output.last_modified().as_ref().unwrap().secs();
        let nanos = get_object_output
            .last_modified()
            .as_ref()
            .unwrap()
            .subsec_nanos();

        self.exec_rate_limit_objects_per_sec().await;

        let byte_stream = convert_to_buf_byte_stream_with_callback(
            get_object_output.body.into_async_read(),
            self.get_stats_sender(),
            source.get_rate_limit_bandwidth(),
            None,
            None,
        );

        let mut buf_reader = BufReader::new(byte_stream.into_async_read());

        let mut chunked_remaining: u64 = 0;
        loop {
            let buffer = buf_reader.fill_buf().await?;
            if buffer.is_empty() {
                break;
            }

            let buffer_len = buffer.len();
            file.write_all(buffer).await?;
            buf_reader.consume(buffer_len);

            // make it easy to cancel
            chunked_remaining += buffer_len as u64;
            if chunked_remaining > self.config.transfer_config.multipart_chunksize {
                chunked_remaining = 0;

                if self.cancellation_token.is_cancelled() {
                    warn!(key = key, "sync cancelled.",);
                    return Err(anyhow!(S3syncError::Cancelled));
                }
            }
        }

        file.flush().await?;
        drop(file);

        let real_path = fs_util::key_to_file_path(self.path.to_path_buf(), key);
        temp_file.persist(&real_path)?;

        fs_util::set_last_modified(self.path.to_path_buf(), key, seconds, nanos)?;

        let target_object_parts = if let Some(object_checksum) = &object_checksum {
            object_checksum.object_parts.clone()
        } else {
            None
        };

        let target_content_length = fs_util::get_file_size(&real_path).await;

        self.verify_local_file(
            key,
            object_checksum,
            &source_sse,
            &source_e_tag,
            source_content_length,
            source_final_checksum,
            source_checksum_algorithm,
            &real_path,
            target_object_parts,
            target_content_length,
            source_storage_class == Some(StorageClass::ExpressOnezone),
        )
        .await?;

        let lossy_path = real_path.to_string_lossy().to_string();
        info!(
            key = key,
            real_path = lossy_path,
            size = source_content_length,
            "sync completed.",
        );

        Ok(PutObjectOutput::builder().build())
    }

    #[allow(clippy::too_many_arguments)]
    // skipcq: RS-R1000
    async fn put_object_multipart(
        &self,
        key: &str,
        source: Storage,
        source_size: u64,
        source_additional_checksum: Option<String>,
        get_object_output_first_chunk: GetObjectOutput,
        _tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Result<PutObjectOutput> {
        let source_version_id = get_object_output_first_chunk
            .version_id()
            .map(|v| v.to_string());
        let source_sse = get_object_output_first_chunk
            .server_side_encryption()
            .cloned();
        let source_e_tag = get_object_output_first_chunk
            .e_tag()
            .map(|e_tag| e_tag.to_string());
        let source_checksum_algorithm = if let Some(object_checksum) = object_checksum.as_ref() {
            object_checksum.checksum_algorithm.clone()
        } else {
            None
        };
        let source_storage_class = get_object_output_first_chunk.storage_class().cloned();
        let source_last_modified = DateTime::from_millis(
            get_object_output_first_chunk
                .last_modified
                .unwrap()
                .to_millis()?,
        )
        .to_chrono_utc()?
        .to_rfc3339();
        let source_last_modified_seconds = get_object_output_first_chunk
            .last_modified()
            .as_ref()
            .unwrap()
            .secs();
        let source_last_modified_nanos = get_object_output_first_chunk
            .last_modified()
            .as_ref()
            .unwrap()
            .subsec_nanos();

        if fs_util::check_directory_traversal(key) {
            return Err(anyhow!(S3syncError::DirectoryTraversalError));
        }

        if self.config.dry_run {
            self.send_stats(SyncBytes(
                u64::from_str(&source_size.to_string()).unwrap_or_default(),
            ))
            .await;

            let real_path = fs_util::key_to_file_path(self.path.to_path_buf(), key)
                .to_string_lossy()
                .to_string();
            info!(
                key = key,
                real_path = real_path,
                source_last_modified = source_last_modified,
                size = source_size,
                "[dry-run] sync completed.",
            );

            return Ok(PutObjectOutput::builder().build());
        }

        if fs_util::is_key_a_directory(key) {
            fs_util::create_directory_hierarchy_from_key(self.path.clone(), key).await?;

            return Ok(PutObjectOutput::builder().build());
        }

        let shared_total_upload_size = Arc::new(Mutex::new(Vec::new()));

        let mut temp_file = fs_util::create_temp_file_from_key(&self.path, key).await?;
        let mut file = tokio::fs::File::from_std(temp_file.as_file_mut().try_clone()?);

        let config_chunksize = self.config.transfer_config.multipart_chunksize as usize;

        let byte_stream = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            self.get_stats_sender(),
            source.get_rate_limit_bandwidth(),
            None,
            None,
        );

        self.exec_rate_limit_objects_per_sec().await;

        let first_chunk_content_length =
            get_object_output_first_chunk.content_length.unwrap() as usize;
        let mut chunked_remaining: u64 = 0;
        let mut first_chunk_data = Vec::<u8>::with_capacity(first_chunk_content_length);
        let mut buf_reader = BufReader::new(byte_stream.into_async_read());
        let mut read_data_size = 0;
        loop {
            let tmp_buffer = buf_reader.fill_buf().await?;
            if tmp_buffer.is_empty() {
                if read_data_size != first_chunk_content_length {
                    return Err(anyhow!("Invalid first chunk data size. Expected: {first_chunk_content_length}, Actual: {read_data_size}"));
                }
                break;
            }
            let buffer_len = tmp_buffer.len();
            first_chunk_data.append(tmp_buffer.to_vec().as_mut());
            buf_reader.consume(buffer_len);

            read_data_size += buffer_len;

            // make it easy to cancel
            chunked_remaining += buffer_len as u64;
            if chunked_remaining > config_chunksize as u64 {
                chunked_remaining = 0;

                if self.cancellation_token.is_cancelled() {
                    warn!(key = key, "sync cancelled.",);
                    return Err(anyhow!(S3syncError::Cancelled));
                }
            }
        }

        let mut offset = 0;
        let mut part_number = 1;
        let mut upload_parts_join_handles = FuturesUnordered::new();
        loop {
            let chunksize = if part_number == 1 {
                first_chunk_content_length
            } else if offset + config_chunksize as u64 > source_size {
                (source_size - offset) as usize
            } else {
                config_chunksize
            };

            let mut cloned_file = tokio::fs::File::from_std(temp_file.reopen()?);

            let cloned_source = dyn_clone::clone_box(&*(source));
            let source_key = key.to_string();
            let source_version_id = source_version_id.clone();
            let source_sse_c = self.config.source_sse_c.clone();
            let source_sse_c_key = self.config.source_sse_c_key.clone();
            let source_sse_c_key_md5 = self.config.source_sse_c_key_md5.clone();

            let additional_checksum_mode = self.config.additional_checksum_mode.clone();

            let total_upload_size = Arc::clone(&shared_total_upload_size);

            let cancellation_token = self.cancellation_token.clone();
            let mut chunk_whole_data = Vec::<u8>::with_capacity(chunksize);
            chunk_whole_data.resize_with(chunksize, Default::default);

            if part_number == 1 {
                chunk_whole_data.copy_from_slice(&first_chunk_data);
            }

            let permit = self
                .config
                .clone()
                .target_client_config
                .unwrap()
                .parallel_upload_semaphore
                .acquire_owned()
                .await?;

            let task: JoinHandle<Result<()>> = task::spawn(async move {
                let _permit = permit; // Keep the semaphore permit in scope

                debug!(
                    key = &source_key,
                    part_number = part_number,
                    offset = offset,
                    chunksize = chunksize,
                    "LocalStorage: write to local file",
                );

                // If the part number is greater than 1, we need to get the object from the source storage.
                if part_number > 1 {
                    let range = Some(format!(
                        "bytes={}-{}",
                        offset,
                        offset + chunksize as u64 - 1
                    ));
                    debug!(
                        key = &source_key,
                        part_number = part_number,
                        "LocalStorage: source get_object() start. range = {range:?}",
                    );

                    let get_object_output = cloned_source
                        .get_object(
                            &source_key,
                            source_version_id,
                            additional_checksum_mode,
                            range,
                            source_sse_c,
                            source_sse_c_key,
                            source_sse_c_key_md5,
                        )
                        .await;
                    let chunk_content_length =
                        get_object_output.as_ref().unwrap().content_length.unwrap() as usize;
                    let body = convert_to_buf_byte_stream_with_callback(
                        get_object_output
                            .context("get_object() failed.")?
                            .body
                            .into_async_read(),
                        cloned_source.get_stats_sender().clone(),
                        cloned_source.get_rate_limit_bandwidth(),
                        None,
                        None,
                    )
                    .into_async_read();

                    let mut chunked_remaining: u64 = 0;
                    let mut chunk_data = Vec::<u8>::with_capacity(chunk_content_length);
                    let mut buf_reader = BufReader::new(body);
                    let mut read_data_size = 0;
                    loop {
                        let tmp_buffer = buf_reader.fill_buf().await?;
                        if tmp_buffer.is_empty() {
                            if read_data_size != chunk_content_length {
                                return Err(anyhow!("Invalid chunk data size. Expected: {chunk_content_length}, Actual: {read_data_size}"));
                            }
                            break;
                        }

                        let buffer_len = tmp_buffer.len();
                        chunk_data.append(tmp_buffer.to_vec().as_mut());
                        buf_reader.consume(buffer_len);

                        read_data_size += buffer_len;

                        chunked_remaining += buffer_len as u64;
                        if chunked_remaining > config_chunksize as u64 {
                            chunked_remaining = 0;

                            if cancellation_token.is_cancelled() {
                                warn!(key = &source_key, "sync cancelled.",);
                                return Err(anyhow!(S3syncError::Cancelled));
                            }
                        }
                    }

                    chunk_whole_data.copy_from_slice(&chunk_data);
                } else {
                    debug!(
                        key = &source_key,
                        part_number = part_number,
                        first_chunk_content_length = first_chunk_content_length,
                        "LocalStorage: source get_object() skipped for first part.",
                    );
                }

                let chunk_whole_data_size = chunk_whole_data.len();
                cloned_file.seek(io::SeekFrom::Start(offset)).await?;
                cloned_file.write_all(&chunk_whole_data).await?;
                cloned_file.flush().await?;

                let mut upload_size_vec = total_upload_size.lock().unwrap();
                upload_size_vec.push(chunk_whole_data_size as u64);

                debug!(
                    key = &source_key,
                    part_number = part_number,
                    "LocalStorage: write_all() completed",
                );

                Ok(())
            });

            upload_parts_join_handles.push(task);

            part_number += 1;
            offset += chunksize as u64;

            if offset >= source_size {
                break;
            }
        }

        while let Some(result) = upload_parts_join_handles.next().await {
            result??;
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }
        }

        file.flush().await?;
        drop(file);

        let real_path = fs_util::key_to_file_path(self.path.to_path_buf(), key);
        temp_file.persist(&real_path)?;

        fs_util::set_last_modified(
            self.path.to_path_buf(),
            key,
            source_last_modified_seconds,
            source_last_modified_nanos,
        )?;

        let target_object_parts = if let Some(object_checksum) = &object_checksum {
            object_checksum.object_parts.clone()
        } else {
            None
        };

        let total_upload_size: u64 = shared_total_upload_size.lock().unwrap().iter().sum();
        if total_upload_size == source_size {
            debug!(
                key,
                total_upload_size, "multipart upload(local) completed successfully."
            );
        } else {
            return Err(anyhow!(format!(
                "multipart upload(local) size mismatch: key={key}, expected = {0}, actual {total_upload_size}",
                source_size
            )));
        }

        let target_content_length = fs_util::get_file_size(&real_path).await;

        self.verify_local_file(
            key,
            object_checksum,
            &source_sse,
            &source_e_tag,
            source_size,
            source_additional_checksum,
            source_checksum_algorithm,
            &real_path,
            target_object_parts,
            target_content_length,
            source_storage_class == Some(StorageClass::ExpressOnezone),
        )
        .await?;

        let lossy_path = real_path.to_string_lossy().to_string();
        info!(
            key = key,
            real_path = lossy_path,
            size = source_size,
            "sync completed.",
        );

        Ok(PutObjectOutput::builder().build())
    }
}

#[async_trait]
impl StorageTrait for LocalStorage {
    fn is_local_storage(&self) -> bool {
        true
    }

    fn is_express_onezone_storage(&self) -> bool {
        false
    }

    async fn list_objects(
        &self,
        sender: &Sender<S3syncObject>,
        _max_keys: i32,
        warn_as_error: bool,
    ) -> Result<()> {
        for entry in WalkDir::new(&self.path).follow_links(self.config.follow_symlinks) {
            if let Err(e) = entry {
                if let Some(inner) = e.io_error() {
                    if inner.kind() == io::ErrorKind::NotFound {
                        continue;
                    }
                }

                let path = e
                    .path()
                    .unwrap_or_else(|| Path::new(""))
                    .to_string_lossy()
                    .to_string();
                self.send_stats(SyncWarning {
                    key: path.to_string(),
                })
                .await;

                let error = e.to_string();
                warn!(path = path, error = error, "failed to list local files.");

                if warn_as_error {
                    return Err(anyhow!("failed to list(): {:?}.", e));
                }
                continue;
            }

            if !self
                .check_dir_entry(entry.as_ref().unwrap(), warn_as_error)
                .await?
            {
                continue;
            }

            if self.cancellation_token.is_cancelled() {
                trace!("list() canceled.");
                break;
            }

            let mut path = remove_local_path_prefix(
                entry.as_ref().unwrap().path().to_str().unwrap(),
                self.path.to_str().unwrap(),
            );

            if cfg!(windows) {
                path = convert_windows_directory_char_to_slash(&path);
            }

            let e_tag = if self.config.filter_config.check_etag
                && !self.config.transfer_config.auto_chunksize
                && !self.config.filter_config.remove_modified_filter
                && self.config.filter_config.check_checksum_algorithm.is_none()
            {
                Some(
                    generate_e_tag_hash_from_path(
                        &PathBuf::from(entry.as_ref().unwrap().path()),
                        self.config.transfer_config.multipart_chunksize as usize,
                        self.config.transfer_config.multipart_threshold as usize,
                    )
                    .await?,
                )
            } else {
                None
            };

            let object = S3syncObject::NotVersioning(build_object_from_dir_entry(
                entry.as_ref().unwrap(),
                &path,
                e_tag,
                self.config.additional_checksum_algorithm.clone(),
            ));

            // This is special for test emulation.
            #[allow(clippy::collapsible_if)]
            if cfg!(feature = "e2e_test_dangerous_simulations") {
                if self.config.allow_e2e_test_dangerous_simulation {
                    simulate_not_found_test_case().await;
                }
            }

            if let Err(e) = sender
                .send(object)
                .await
                .context("async_channel::Sender::send() failed.")
            {
                return if !sender.is_closed() { Err(e) } else { Ok(()) };
            }
        }

        Ok(())
    }

    #[cfg(not(tarpaulin_include))]
    async fn list_object_versions(
        &self,
        _sender: &Sender<S3syncObject>,
        _max_keys: i32,
        _warn_as_error: bool,
    ) -> Result<()> {
        // local storage does not support versioning.
        unimplemented!();
    }

    // skipcq: RS-R1000
    async fn get_object(
        &self,
        key: &str,
        _version_id: Option<String>,
        _checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<GetObjectOutput> {
        let mut path = self.path.clone();
        path.push(key);

        if !path.clone().try_exists()? {
            let (get_object_error, response) = build_no_such_key_response();

            return Err(anyhow!(SdkError::service_error(get_object_error, response)));
        }

        let content_type = if self.config.no_guess_mime_type {
            None
        } else {
            Some(
                mime_guess::from_path(&path)
                    .first_or_octet_stream()
                    .to_string(),
            )
        };

        let mut need_checksum = true;
        let body;
        let content_length;
        let content_range;
        if range.is_some() {
            let file_range = storage::parse_range_header(&range.unwrap())?;
            body = Some(
                ByteStream::read_from()
                    .path(path.clone())
                    .offset(file_range.offset)
                    .length(Length::Exact(file_range.size))
                    .buffer_size(self.config.transfer_config.multipart_chunksize as usize)
                    .build()
                    .await?,
            );
            // For performance, if the range is specified, we need to calculate the checksum only if the offset is 0.
            need_checksum = file_range.offset == 0;
            content_length = file_range.size as i64;
            content_range = Some(format!(
                "bytes {}-{}/{}",
                file_range.offset,
                file_range.offset + file_range.size - 1,
                fs_util::get_file_size(&path).await
            ));
        } else {
            body = Some(ByteStream::from_path(path.clone()).await?);
            content_length = fs_util::get_file_size(&path).await as i64;
            content_range = None;
        }

        if self.config.dry_run {
            need_checksum = false;
        }

        let checksum = if self.config.additional_checksum_algorithm.is_some() && need_checksum {
            Some(
                generate_checksum_from_path_with_chunksize(
                    &path,
                    self.config
                        .additional_checksum_algorithm
                        .as_ref()
                        .unwrap()
                        .clone(),
                    self.config.transfer_config.multipart_chunksize as usize,
                    self.config.transfer_config.multipart_threshold as usize,
                    self.config.full_object_checksum,
                )
                .await?,
            )
        } else {
            None
        };

        let checksum_sha256 = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Sha256
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_sha1 = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Sha1
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc32 = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc32
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc32_c = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc32C
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc64_nvme = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc64Nvme
            ) {
            checksum.clone()
        } else {
            None
        };

        Ok(GetObjectOutputBuilder::default()
            .set_content_length(Some(content_length))
            .set_content_type(content_type)
            .set_content_range(content_range)
            .last_modified(fs_util::get_last_modified(&path).await)
            .set_body(body)
            .set_checksum_sha256(checksum_sha256)
            .set_checksum_sha1(checksum_sha1)
            .set_checksum_crc32(checksum_crc32)
            .set_checksum_crc32_c(checksum_crc32_c)
            .set_checksum_crc64_nvme(checksum_crc64_nvme)
            .build())
    }

    #[cfg(not(tarpaulin_include))]
    async fn get_object_versions(&self, _key: &str, _max_keys: i32) -> Result<Vec<ObjectVersion>> {
        // local storage does not support versioning.
        unimplemented!();
    }

    #[cfg(not(tarpaulin_include))]
    async fn get_object_tagging(
        &self,
        _key: &str,
        _version_id: Option<String>,
    ) -> Result<GetObjectTaggingOutput> {
        // local storage does not support tagging.
        unimplemented!();
    }

    async fn head_object(
        &self,
        key: &str,
        _version_id: Option<String>,
        _checksum_mode: Option<ChecksumMode>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput> {
        let path = fs_util::key_to_file_path(self.path.to_path_buf(), key);

        let result = path.try_exists();
        if let Err(e) = result {
            self.send_stats(SyncWarning {
                key: key.to_string(),
            })
            .await;

            let error = e.to_string();
            let source = e.source();

            warn!(error = error, source = source, "failed to access object.");
            return Err(anyhow!("failed to path.try_exists()."));
        }

        if !result? {
            let (head_object_error, response) = build_not_found_response();

            return Err(anyhow!(SdkError::service_error(
                head_object_error,
                response
            )));
        }

        if path.is_dir() {
            return Ok(HeadObjectOutputBuilder::default()
                .set_content_length(Some(0))
                .build());
        }

        Ok(HeadObjectOutputBuilder::default()
            .set_content_length(Some(fs_util::get_file_size(&path).await as i64))
            .last_modified(fs_util::get_last_modified(&path).await)
            .build())
    }

    async fn head_object_first_part(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _checksum_mode: Option<ChecksumMode>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput> {
        unimplemented!();
    }

    #[cfg(not(tarpaulin_include))]
    async fn get_object_parts(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        // local storage does not support multipart upload.
        unimplemented!();
    }

    #[cfg(not(tarpaulin_include))]
    async fn get_object_parts_attributes(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _max_parts: i32,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        // local storage does not support multipart upload.
        unimplemented!();
    }

    async fn put_object(
        &self,
        key: &str,
        source: Storage,
        source_size: u64,
        source_additional_checksum: Option<String>,
        get_object_output_first_chunk: GetObjectOutput,
        _tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Result<PutObjectOutput> {
        if get_object_output_first_chunk.content_range.is_none() {
            self.put_object_single_part(
                key,
                source,
                get_object_output_first_chunk,
                _tagging,
                object_checksum,
            )
            .await
        } else {
            self.put_object_multipart(
                key,
                source,
                source_size,
                source_additional_checksum,
                get_object_output_first_chunk,
                _tagging,
                object_checksum,
            )
            .await
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn put_object_tagging(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _tagging: Tagging,
    ) -> Result<PutObjectTaggingOutput> {
        // local storage does not support tagging.
        unimplemented!();
    }

    async fn delete_object(
        &self,
        key: &str,
        _version_id: Option<String>,
    ) -> Result<DeleteObjectOutput> {
        let file_to_delete = fs_util::key_to_file_path(self.path.to_path_buf(), key);
        let lossy_path = file_to_delete.to_string_lossy().to_string();

        if self.config.dry_run {
            info!(
                key = key,
                real_path = lossy_path,
                "[dry-run] delete completed.",
            );

            return Ok(DeleteObjectOutput::builder().build());
        }

        tokio::fs::remove_file(&file_to_delete).await?;

        info!(key = key, real_path = lossy_path, "delete completed.",);

        Ok(DeleteObjectOutput::builder().build())
    }

    #[cfg(not(tarpaulin_include))]
    async fn delete_object_tagging(
        &self,
        _key: &str,
        _version_id: Option<String>,
    ) -> Result<DeleteObjectTaggingOutput> {
        // local storage does not support tagging.
        unimplemented!();
    }

    #[cfg(not(tarpaulin_include))]
    async fn is_versioning_enabled(&self) -> Result<bool> {
        unimplemented!();
    }

    fn get_client(&self) -> Option<Arc<Client>> {
        None
    }

    fn get_stats_sender(&self) -> Sender<SyncStatistics> {
        self.stats_sender.clone()
    }

    async fn send_stats(&self, stats: SyncStatistics) {
        let _ = self.stats_sender.send(stats).await;
    }
    fn get_local_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
        self.rate_limit_bandwidth.clone()
    }
}

async fn simulate_not_found_test_case() {
    #[cfg(feature = "e2e_test_dangerous_simulations")]
    {
        const NOT_FOUND_TEST_FILE: &str =
            "./playground/not_found_test/s3sync_not_found_test_66143ea2-53cb-4ee9-98d6-7067bf5f325d";
        const NOT_FOUND_DANGEROUS_SIMULATION_ENV: &str = "S3SYNC_NOT_FOUND_DANGEROUS_SIMULATION";
        const NOT_FOUND_DANGEROUS_SIMULATION_ENV_ALLOW: &str = "ALLOW";

        if std::env::var(NOT_FOUND_DANGEROUS_SIMULATION_ENV)
            .is_ok_and(|v| v == NOT_FOUND_DANGEROUS_SIMULATION_ENV_ALLOW)
        {
            tracing::error!(
                "remove not found test file. This message should not be shown in the production."
            );
            let _ = tokio::fs::remove_file(PathBuf::from(NOT_FOUND_TEST_FILE)).await;
        }
    }
}

fn remove_local_path_prefix(path: &str, prefix: &str) -> String {
    if path == prefix {
        let path = PathBuf::from(path);
        return path.file_name().unwrap().to_str().unwrap().to_string();
    }

    let mut without_prefix_path = path.replacen(prefix, "", 1);

    if without_prefix_path.starts_with(std::path::MAIN_SEPARATOR) {
        without_prefix_path = without_prefix_path.replacen(std::path::MAIN_SEPARATOR, "", 1);
    }
    without_prefix_path
}

fn build_object_from_dir_entry(
    entry: &DirEntry,
    key: &str,
    e_tag: Option<String>,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Object {
    Object::builder()
        .set_key(Some(key.to_string()))
        .set_size(Some(
            i64::try_from(entry.metadata().unwrap().len()).unwrap(),
        ))
        .set_last_modified(Some(DateTime::from(
            entry.metadata().unwrap().modified().unwrap(),
        )))
        .set_e_tag(e_tag)
        .set_checksum_algorithm(checksum_algorithm.map(|algorithm| vec![algorithm]))
        .build()
}

fn build_not_found_response() -> (HeadObjectError, Response<SdkBody>) {
    let head_object_error =
        HeadObjectError::NotFound(aws_sdk_s3::types::error::NotFound::builder().build());
    let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));
    (head_object_error, response)
}

fn build_no_such_key_response() -> (GetObjectError, Response<SdkBody>) {
    let get_object_error =
        GetObjectError::NoSuchKey(aws_sdk_s3::types::error::NoSuchKey::builder().build());
    let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));
    (get_object_error, response)
}

fn convert_windows_directory_char_to_slash(path: &str) -> String {
    path.replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::parse_from_args;
    use crate::storage::local::remove_local_path_prefix;
    use crate::types::token::create_pipeline_cancellation_token;
    use tokio::io::AsyncReadExt;
    use tracing_subscriber::EnvFilter;

    const TEST_DATA_SIZE: u64 = 5;
    const TEST_SOURCE_OBJECTS_COUNT: usize = 6;

    #[test]
    #[cfg(target_family = "unix")]
    fn remove_path_prefix_test_unix() {
        init_dummy_tracing_subscriber();

        assert_eq!(remove_local_path_prefix("./dir1/data1", "./dir1"), "data1");
        assert_eq!(remove_local_path_prefix("./dir1/data1", "./dir1/"), "data1");
        assert_eq!(remove_local_path_prefix("dir1/data1", "dir1"), "data1");
        assert_eq!(remove_local_path_prefix("dir1/data1", "dir1/"), "data1");
        assert_eq!(remove_local_path_prefix("/dir1/data1", "/dir1"), "data1");
        assert_eq!(remove_local_path_prefix("/dir1/data1", "/dir1/"), "data1");
        assert_eq!(
            remove_local_path_prefix("/dir1/data1", "/dir1/data1"),
            "data1"
        );
        assert_eq!(
            remove_local_path_prefix("/dir1/data1/dir1/", "dir1/"),
            "data1/dir1/"
        );
        assert_eq!(
            remove_local_path_prefix("/dir1/data1/dir1/data1", "dir1/"),
            "data1/dir1/data1"
        );
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn remove_path_prefix_test_windows() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            remove_local_path_prefix(".\\dir1\\data1", ".\\dir1"),
            "data1"
        );
        assert_eq!(
            remove_local_path_prefix(".\\dir1\\data1", ".\\dir1\\"),
            "data1"
        );
        assert_eq!(remove_local_path_prefix("dir1\\data1", "dir1"), "data1");
        assert_eq!(remove_local_path_prefix("dir1\\data1", "dir1\\"), "data1");
        assert_eq!(remove_local_path_prefix("\\dir1\\data1", "\\dir1"), "data1");
        assert_eq!(
            remove_local_path_prefix("\\dir1\\data1", "\\dir1\\"),
            "data1"
        );
        assert_eq!(
            remove_local_path_prefix("\\dir1\\data1", "\\dir1\\data1"),
            "data1"
        );

        assert_eq!(
            remove_local_path_prefix(
                "C:\\Users\\Administrator\\Documents\\s3sync",
                "C:\\Users\\Administrator\\Documents"
            ),
            "s3sync"
        );

        assert_eq!(
            remove_local_path_prefix("\\dir1\\data1\\dir1\\", "dir1\\"),
            "data1\\dir1\\"
        );
        assert_eq!(
            remove_local_path_prefix("\\dir1\\data1\\dir1\\data1", "\\dir1\\"),
            "data1\\dir1\\data1"
        );
    }

    #[test]
    fn build_object_from_dir_entry_test() {
        init_dummy_tracing_subscriber();

        let mut entry_iter = WalkDir::new("./test_data/5byte.dat").into_iter();
        let entry = entry_iter.next().unwrap().unwrap();
        let object = build_object_from_dir_entry(&entry, "test_data/5byte.dat", None, None);

        assert_eq!(object.key().unwrap(), "test_data/5byte.dat");
        assert_eq!(object.size().unwrap(), TEST_DATA_SIZE as i64);
        assert!(object.last_modified().is_some());
        assert!(object.checksum_algorithm().is_empty());
    }

    #[test]
    fn build_object_from_dir_entry_checksum() {
        init_dummy_tracing_subscriber();

        let mut entry_iter = WalkDir::new("./test_data/5byte.dat").into_iter();
        let entry = entry_iter.next().unwrap().unwrap();
        let object = build_object_from_dir_entry(
            &entry,
            "test_data/5byte.dat",
            None,
            Some(ChecksumAlgorithm::Sha256),
        );
        assert_eq!(object.key().unwrap(), "test_data/5byte.dat");
        assert_eq!(object.size().unwrap(), TEST_DATA_SIZE as i64);
        assert!(object.last_modified().is_some());
        assert_eq!(object.checksum_algorithm().as_ref().len(), 1);
        assert_eq!(object.checksum_algorithm()[0], ChecksumAlgorithm::Sha256);
    }

    #[tokio::test]
    async fn create_storage() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn is_local_storage() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        assert!(storage.is_local_storage());
    }

    #[tokio::test]
    #[should_panic]
    async fn create_storage_panic_not_local_storage() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "s3://source-bucket",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn stats_channel_test() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "s3://source-bucket",
            "test_data/source/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, stats_receiver) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let stats_sender = storage.get_stats_sender();

        stats_sender.send(SyncBytes(0)).await.unwrap();
        assert_eq!(stats_receiver.recv().await.unwrap(), SyncBytes(0));
    }

    #[tokio::test]
    async fn list_storage() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/source/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        storage.list_objects(&sender, 1000, false).await.unwrap();

        assert_eq!(receiver.len(), TEST_SOURCE_OBJECTS_COUNT);

        if let S3syncObject::NotVersioning(object) = receiver.recv().await.unwrap() {
            assert!(object.key().is_some());
            assert!(object.last_modified().is_some());
        } else {
            panic!("NotVersioning object not found")
        }
    }

    #[tokio::test]
    async fn list_storage_not_found() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/notfound/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, _) = async_channel::bounded::<S3syncObject>(1000);
        storage.list_objects(&sender, 1000, false).await.unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn list_storage_special_files() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "/dev/fd",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, _) = async_channel::bounded::<S3syncObject>(1000);
        storage.list_objects(&sender, 1000, false).await.unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn list_storage_special_files_warn_as_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "/dev/fd",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, _) = async_channel::bounded::<S3syncObject>(1000);
        assert!(storage.list_objects(&sender, 1000, true).await.is_err());
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn list_storage_special_files_linux() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "/proc/self/fd",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, _) = async_channel::bounded::<S3syncObject>(1000);
        storage.list_objects(&sender, 1000, false).await.unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "windows")]
    async fn list_storage_for_windows() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/for_windows/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        storage.list_objects(&sender, 1000, false).await.unwrap();

        if let S3syncObject::NotVersioning(object) = receiver.recv().await.unwrap() {
            assert_eq!(object.key().unwrap(), "dir1/data1");
            assert!(object.last_modified().is_some());
        } else {
            panic!("NotVersioning object not found")
        }
    }

    #[tokio::test]
    async fn list_storage_canceled() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/source",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            cancellation_token.clone(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        cancellation_token.cancel();
        storage.list_objects(&sender, 1000, false).await.unwrap();

        assert_eq!(receiver.len(), 0);
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn list_storage_permission_warn_as_error() {
        init_dummy_tracing_subscriber();

        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        assert!(
            !nix::unistd::geteuid().is_root(),
            "tests must not run as root"
        );

        let mut permissions = fs::metadata("./test_data/denied_dir6")
            .unwrap()
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions("./test_data/denied_dir6", permissions).unwrap();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/denied_dir6",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, _) = async_channel::bounded::<S3syncObject>(1000);

        assert!(storage.list_objects(&sender, 1000, true).await.is_err());

        permissions = fs::metadata("./test_data/denied_dir6")
            .unwrap()
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions("./test_data/denied_dir6", permissions).unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn list_storage_permission_dir_warning() {
        init_dummy_tracing_subscriber();

        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata("./test_data/denied_dir4")
            .unwrap()
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions("./test_data/denied_dir4", permissions).unwrap();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/denied_dir4",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, _) = async_channel::bounded::<S3syncObject>(1000);

        permissions = fs::metadata("./test_data/denied_dir4")
            .unwrap()
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions("./test_data/denied_dir4", permissions).unwrap();

        assert!(storage.list_objects(&sender, 1000, false).await.is_ok());
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn list_storage_permission_file_warning() {
        init_dummy_tracing_subscriber();

        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata("./test_data/denied/denied")
            .unwrap()
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions("./test_data/denied/denied", permissions.clone()).unwrap();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/denied",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let (sender, _) = async_channel::bounded::<S3syncObject>(1000);

        permissions.set_mode(0o644);
        fs::set_permissions("./test_data/denied/denied", permissions).unwrap();

        assert!(storage.list_objects(&sender, 1000, false).await.is_ok());
    }

    #[tokio::test]
    async fn get_object() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                None,
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();
        if cfg!(target_family = "windows") {
            assert_eq!(get_object_result.content_length, Some(7));
        } else {
            assert_eq!(get_object_result.content_length, Some(6));
        }

        assert!(get_object_result.content_range.is_none());
    }

    #[tokio::test]
    async fn get_object_range() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=0-3".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        let mut data = "".to_string();
        get_object_result
            .body
            .into_async_read()
            .read_to_string(&mut data)
            .await
            .unwrap();
        assert_eq!(data, "data");
        assert_eq!(get_object_result.content_length, Some(4));

        if cfg!(target_family = "windows") {
            assert_eq!(
                get_object_result.content_range,
                Some("bytes 0-3/7".to_string())
            );
        } else {
            assert_eq!(
                get_object_result.content_range,
                Some("bytes 0-3/6".to_string())
            );
        }

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=1-2".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        let mut data = "".to_string();
        get_object_result
            .body
            .into_async_read()
            .read_to_string(&mut data)
            .await
            .unwrap();
        assert_eq!(data, "at");
        assert_eq!(get_object_result.content_length, Some(2));
        if cfg!(target_family = "windows") {
            assert_eq!(
                get_object_result.content_range,
                Some("bytes 1-2/7".to_string())
            );
        } else {
            assert_eq!(
                get_object_result.content_range,
                Some("bytes 1-2/6".to_string())
            );
        }

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=0-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        let mut data = "".to_string();
        get_object_result
            .body
            .into_async_read()
            .read_to_string(&mut data)
            .await
            .unwrap();
        assert_eq!(data, "data1");
        assert_eq!(get_object_result.content_length, Some(5));
        if cfg!(target_family = "windows") {
            assert_eq!(
                get_object_result.content_range,
                Some("bytes 0-4/7".to_string())
            );
        } else {
            assert_eq!(
                get_object_result.content_range,
                Some("bytes 0-4/6".to_string())
            );
        }

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=0-5".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        let mut data = "".to_string();
        get_object_result
            .body
            .into_async_read()
            .read_to_string(&mut data)
            .await
            .unwrap();

        assert_eq!(get_object_result.content_length, Some(6));

        if cfg!(target_family = "windows") {
            assert_eq!(data, "data1\r");
            assert_eq!(
                get_object_result.content_range,
                Some("bytes 0-5/7".to_string())
            );
        } else {
            assert_eq!(data, "data1\n");
            assert_eq!(
                get_object_result.content_range,
                Some("bytes 0-5/6".to_string())
            );
        }
    }

    #[tokio::test]
    async fn get_object_range_with_checksum_sha256() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "dummy_access_key",
            "--target-secret-access-key",
            "dummy_secret_access_key",
            "--additional-checksum-algorithm",
            "SHA256",
            "./test_data/",
            "s3://dummy-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=0-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_some());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_none());

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=1-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_none());
    }

    #[tokio::test]
    async fn get_object_range_with_checksum_sha1() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "dummy_access_key",
            "--target-secret-access-key",
            "dummy_secret_access_key",
            "--additional-checksum-algorithm",
            "SHA1",
            "./test_data/",
            "s3://dummy-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=0-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_some());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_none());

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=1-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_none());
    }

    #[tokio::test]
    async fn get_object_range_with_checksum_crc32() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "dummy_access_key",
            "--target-secret-access-key",
            "dummy_secret_access_key",
            "--additional-checksum-algorithm",
            "CRC32",
            "./test_data/",
            "s3://dummy-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=0-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_some());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_none());

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=1-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_none());
    }

    #[tokio::test]
    async fn get_object_range_with_checksum_crc32c() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "dummy_access_key",
            "--target-secret-access-key",
            "dummy_secret_access_key",
            "--additional-checksum-algorithm",
            "CRC32C",
            "./test_data/",
            "s3://dummy-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=0-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_some());
        assert!(get_object_result.checksum_crc64_nvme.is_none());

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=1-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_none());
    }

    #[tokio::test]
    async fn get_object_range_with_checksum_crc64nvme() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "dummy_access_key",
            "--target-secret-access-key",
            "dummy_secret_access_key",
            "--additional-checksum-algorithm",
            "CRC64NVME",
            "./test_data/",
            "s3://dummy-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=0-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_some());

        let get_object_result = storage
            .get_object(
                "source/data1",
                None,
                None,
                Some("bytes=1-4".to_string()),
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        assert!(get_object_result.checksum_sha256.is_none());
        assert!(get_object_result.checksum_sha1.is_none());
        assert!(get_object_result.checksum_crc32.is_none());
        assert!(get_object_result.checksum_crc32_c.is_none());
        assert!(get_object_result.checksum_crc64_nvme.is_none());
    }
    #[tokio::test]
    async fn head_object() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let head_object_output = storage
            .head_object(
                "source/dir1/6byte.dat",
                None,
                None,
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();
        assert_eq!(head_object_output.content_length().unwrap(), 6);
        assert!(head_object_output.last_modified().is_some());
    }

    #[tokio::test]
    async fn head_object_not_found() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let head_object_output = storage
            .head_object(
                "source/dir1/no_data",
                None,
                None,
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await;
        assert!(head_object_output.is_err());

        if let Some(SdkError::ServiceError(e)) = head_object_output
            .err()
            .unwrap()
            .downcast_ref::<SdkError<HeadObjectError, Response<SdkBody>>>()
        {
            assert!(e.err().is_not_found());
            return;
        }

        // skipcq: RS-W1021
        assert!(false, "no 404 error occurred")
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn head_object_error() {
        init_dummy_tracing_subscriber();

        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        assert!(
            !nix::unistd::geteuid().is_root(),
            "tests must not run as root"
        );

        let mut permissions = fs::metadata("./test_data/denied_dir5")
            .unwrap()
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions("./test_data/denied_dir5", permissions).unwrap();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let head_object_output = storage
            .head_object(
                "denied_dir5/data",
                None,
                None,
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await;

        permissions = fs::metadata("./test_data/denied_dir5")
            .unwrap()
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions("./test_data/denied_dir5", permissions).unwrap();

        assert!(head_object_output.is_err());

        if matches!(
            head_object_output
                .err()
                .unwrap()
                .downcast_ref::<SdkError<HeadObjectError, Response<SdkBody>>>(),
            Some(SdkError::ServiceError(_))
        ) {
            // skipcq: RS-W1021
            assert!(false, "ServiceError occurred");
        }
    }

    #[tokio::test]
    async fn put_object() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender.clone(),
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let get_object_output = storage
            .get_object(
                "source/data1",
                None,
                None,
                None,
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        let source_storage = LocalStorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        storage
            .put_object(
                "target/data1",
                source_storage,
                6,
                None,
                get_object_output,
                None,
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_object_directory() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender.clone(),
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let source_storage = LocalStorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        storage
            .put_object(
                "target/",
                source_storage,
                6,
                None,
                GetObjectOutputBuilder::default()
                    .set_content_length(Some(0))
                    .last_modified(DateTime::from_secs(1))
                    .build(),
                None,
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_object_with_directory_traversal() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender.clone(),
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let source_storage = LocalStorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        let e = storage
            .put_object(
                "target/../../etc/passwd",
                source_storage,
                6,
                None,
                GetObjectOutputBuilder::default()
                    .set_content_length(Some(1))
                    .last_modified(DateTime::from_secs(1))
                    .build(),
                None,
                None,
            )
            .await
            .err()
            .unwrap();

        assert_eq!(
            *e.downcast_ref::<S3syncError>().unwrap(),
            S3syncError::DirectoryTraversalError
        );
    }

    #[test]
    fn convert_windows_directory_char_to_slash_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            convert_windows_directory_char_to_slash("test1\\test2"),
            "test1/test2"
        );
        assert_eq!(
            convert_windows_directory_char_to_slash("test1\\test2\\test3"),
            "test1/test2/test3"
        );
        assert_eq!(
            convert_windows_directory_char_to_slash("\\test1\\test2\\test3"),
            "/test1/test2/test3"
        );
        assert_eq!(
            convert_windows_directory_char_to_slash("test1test2test3"),
            "test1test2test3"
        );
    }

    #[tokio::test]
    async fn is_express_onezone_storage_false() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        assert!(!storage.is_express_onezone_storage());
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn get_local_path() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "dummy_access_key",
            "--source-secret-access-key",
            "dummy_secret_access_key",
            "s3://dummy-bucket",
            "./test_data/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(storage.get_local_path().to_str().unwrap(), "./test_data/");
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
