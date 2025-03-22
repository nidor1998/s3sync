use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput;
use aws_sdk_s3::operation::get_object::builders::GetObjectOutputBuilder;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::builders::HeadObjectOutputBuilder;
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, Object, ObjectPart, ObjectVersion, ServerSideEncryption,
    StorageClass, Tagging,
};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types_convert::date_time::DateTimeExt;
use leaky_bucket::RateLimiter;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tracing::{debug, info, trace, warn};
use walkdir::{DirEntry, WalkDir};

use crate::config::ClientConfig;
use crate::storage::additional_checksum_verify::{
    generate_checksum_from_path, generate_checksum_from_path_with_chunksize,
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
use crate::Config;

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
        if !regular_file_check_result.unwrap() {
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
        if !self.config.disable_etag_verify && !source_express_onezone_storage {
            trace!(
                key = key,
                size = source_content_length,
                "download completed. start to etag verify. depends on the size, this may take a while.",
            );

            let target_sse = None;
            let target_e_tag = if let Some(parts) = target_object_parts.as_ref() {
                Some(
                    generate_e_tag_hash_from_path_with_auto_chunksize(
                        real_path,
                        parts.iter().map(|part| part.size().unwrap()).collect(),
                    )
                    .await?,
                )
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
                        self.send_stats(SyncWarning {
                            key: key.to_string(),
                        })
                        .await;

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
                    }
                } else {
                    self.send_stats(ETagVerified {
                        key: key.to_string(),
                    })
                    .await;

                    let source_e_tag = source_e_tag.clone().unwrap();
                    let target_e_tag = target_e_tag.clone().unwrap();
                    trace!(
                        key = key,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        "e_tag verified."
                    );
                }
            }
        } else if source_content_length != target_content_length {
            self.send_stats(SyncWarning {
                key: key.to_string(),
            })
            .await;

            warn!(
                key = key,
                source_content_length = source_content_length,
                target_content_length = target_content_length,
                "content length mismatch. file in the local storage may be corrupted."
            );
        }

        // Since aws-sdk-s3 1.69.0, the checksum mode is always enabled,
        // and cannot be disabled(maybe).
        // So, s3sync check the checksum mode is enabled by the user.
        if self.config.additional_checksum_mode.is_none() {
            return Ok(());
        }

        if let Some(source_final_checksum) = source_final_checksum {
            trace!(
                key = key,
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

            let target_final_checksum = generate_checksum_from_path(
                real_path,
                source_checksum_algorithm.as_ref().unwrap().clone(),
                parts,
                self.config.transfer_config.multipart_threshold as usize,
                is_full_object_checksum(&Some(source_final_checksum.clone())),
            )
            .await?;

            let additional_checksum_algorithm =
                source_checksum_algorithm.as_ref().unwrap().as_str();

            if source_final_checksum != target_final_checksum {
                self.send_stats(SyncWarning {
                    key: key.to_string(),
                })
                .await;

                warn!(
                    key = key,
                    additional_checksum_algorithm = additional_checksum_algorithm,
                    source_final_checksum = source_final_checksum,
                    target_final_checksum = target_final_checksum,
                    "additional checksum mismatch. file in the local storage may be corrupted."
                );
            } else {
                self.send_stats(ChecksumVerified {
                    key: key.to_string(),
                })
                .await;

                trace!(
                    key = key,
                    additional_checksum_algorithm = additional_checksum_algorithm,
                    source_final_checksum = source_final_checksum,
                    target_final_checksum = target_final_checksum,
                    "additional checksum verified."
                );
            }
        }
        Ok(())
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
        panic!("not implemented");
    }

    async fn get_object(
        &self,
        key: &str,
        _version_id: Option<String>,
        _checksum_mode: Option<ChecksumMode>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<GetObjectOutput> {
        let mut path = self.path.clone();
        path.push(key);

        let content_type = if self.config.no_guess_mime_type {
            None
        } else {
            Some(
                mime_guess::from_path(&path)
                    .first_or_octet_stream()
                    .to_string(),
            )
        };

        let checksum = if self.config.additional_checksum_algorithm.is_some() {
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
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Sha256
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_sha1 = if self.config.additional_checksum_algorithm.is_some()
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Sha1
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc32 = if self.config.additional_checksum_algorithm.is_some()
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc32
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc32_c = if self.config.additional_checksum_algorithm.is_some()
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc32C
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc64_nvme = if self.config.additional_checksum_algorithm.is_some()
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc64Nvme
            ) {
            checksum.clone()
        } else {
            None
        };

        Ok(GetObjectOutputBuilder::default()
            .set_content_length(Some(fs_util::get_file_size(&path).await as i64))
            .set_content_type(content_type)
            .last_modified(fs_util::get_last_modified(&path).await)
            .set_body(Some(ByteStream::from_path(path).await?))
            .set_checksum_sha256(checksum_sha256)
            .set_checksum_sha1(checksum_sha1)
            .set_checksum_crc32(checksum_crc32)
            .set_checksum_crc32_c(checksum_crc32_c)
            .set_checksum_crc64_nvme(checksum_crc64_nvme)
            .build())
    }

    #[cfg(not(tarpaulin_include))]
    async fn get_object_versions(&self, _key: &str, _max_keys: i32) -> Result<Vec<ObjectVersion>> {
        panic!("not implemented");
    }

    #[cfg(not(tarpaulin_include))]
    async fn get_object_tagging(
        &self,
        _key: &str,
        _version_id: Option<String>,
    ) -> Result<GetObjectTaggingOutput> {
        panic!("not implemented");
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

        if !result.unwrap() {
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

    #[cfg(not(tarpaulin_include))]
    async fn get_object_parts(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        panic!("not implemented");
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
        panic!("not implemented");
    }

    async fn put_object(
        &self,
        key: &str,
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

        let source_last_modified = DateTime::from_millis(
            get_object_output
                .last_modified
                .unwrap()
                .to_millis()
                .unwrap(),
        )
        .to_chrono_utc()
        .unwrap()
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
        let mut file = tokio::fs::File::from_std(temp_file.as_file_mut().try_clone().unwrap());

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
            self.rate_limit_bandwidth.clone(),
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
        temp_file.persist(&real_path).unwrap();

        fs_util::set_last_modified(self.path.to_path_buf(), key, seconds, nanos).unwrap();

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

    #[cfg(not(tarpaulin_include))]
    async fn put_object_tagging(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _tagging: Tagging,
    ) -> Result<PutObjectTaggingOutput> {
        panic!("not implemented");
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
        panic!("not implemented");
    }

    #[cfg(not(tarpaulin_include))]
    async fn is_versioning_enabled(&self) -> Result<bool> {
        panic!("not implemented");
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

fn convert_windows_directory_char_to_slash(path: &str) -> String {
    path.replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use crate::config::args::parse_from_args;
    use crate::storage::local::remove_local_path_prefix;
    use crate::types::token::create_pipeline_cancellation_token;
    use tracing_subscriber::EnvFilter;

    use super::*;

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
        )
        .await;

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        storage.list_objects(&sender, 1000, false).await.unwrap();

        assert_eq!(receiver.len(), TEST_SOURCE_OBJECTS_COUNT);

        if let S3syncObject::NotVersioning(object) = receiver.recv().await.unwrap() {
            assert!(object.key().is_some());
            assert!(object.last_modified().is_some());
        } else {
            panic!("NotVersing object not found")
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
        )
        .await;

        let (sender, _) = async_channel::bounded::<S3syncObject>(1000);
        storage.list_objects(&sender, 1000, false).await.unwrap();
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
        )
        .await;

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        storage.list_objects(&sender, 1000, false).await.unwrap();

        if let S3syncObject::NotVersioning(object) = receiver.recv().await.unwrap() {
            assert_eq!(object.key().unwrap(), "dir1/data1");
            assert!(object.last_modified().is_some());
        } else {
            panic!("NotVersing object not found")
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

        if nix::unistd::geteuid().is_root() {
            panic!("run tests from root");
        }

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
        )
        .await;

        storage
            .get_object(
                "source/data1",
                None,
                None,
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();
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

        panic!("no 404 error occurred")
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn head_object_error() {
        init_dummy_tracing_subscriber();

        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        if nix::unistd::geteuid().is_root() {
            panic!("run tests from root");
        }

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
            panic!("ServiceError occurred");
        }
    }

    #[tokio::test]
    async fn put_object() {
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
        )
        .await;

        let get_object_output = storage
            .get_object(
                "source/data1",
                None,
                None,
                None,
                SseCustomerKey { key: None },
                None,
            )
            .await
            .unwrap();

        storage
            .put_object("target/data1", get_object_output, None, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_object_directory() {
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
        )
        .await;

        storage
            .put_object(
                "target/",
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
        )
        .await;

        let e = storage
            .put_object(
                "target/../../etc/passwd",
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
