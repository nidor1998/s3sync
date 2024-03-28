use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_channel::Sender;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, ObjectPart, ServerSideEncryption,
    StorageClass,
};
use aws_sdk_s3::Client;
use aws_smithy_types_convert::date_time::DateTimeExt;
use base64::{engine::general_purpose, Engine as _};
use chrono::SecondsFormat;
use tokio::io::AsyncReadExt;
use tracing::{trace, warn};

use crate::config::Config;
use crate::storage;
use crate::storage::e_tag_verify::{generate_e_tag_hash, is_multipart_upload_e_tag};
use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use crate::types::SyncStatistics::{ChecksumVerified, EtagVerified, SyncWarning};
use crate::types::{
    SyncStatistics, S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY, S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY,
};

const MISMATCH_WARNING_WITH_HELP: &str = "mismatch. object in the target storage may be corrupted. \
 or the current multipart_threshold or multipart_chunksize may be different when uploading to the source. \
 To suppress this warning, please add --disable-multipart-verify command line option. \
 To resolve this issue, please add --auto-chunksize command line option(but extra API overheads).";

pub struct UploadManager {
    client: Arc<Client>,
    config: Config,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
    tagging: Option<String>,
    object_parts: Option<Vec<ObjectPart>>,
    concatnated_md5_hash: Vec<u8>,
    express_onezone_storage: bool,
}

impl UploadManager {
    pub fn new(
        client: Arc<Client>,
        config: Config,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        tagging: Option<String>,
        object_parts: Option<Vec<ObjectPart>>,
        express_onezone_storage: bool,
    ) -> Self {
        UploadManager {
            client,
            config,
            cancellation_token,
            stats_sender,
            tagging,
            object_parts,
            concatnated_md5_hash: vec![],
            express_onezone_storage,
        }
    }

    pub async fn upload(
        &mut self,
        bucket: &str,
        key: &str,
        mut get_object_output: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        get_object_output = self.modify_metadata(get_object_output);

        if self.is_auto_chunksize_enabled() {
            return self
                .upload_with_auto_chunksize(bucket, key, get_object_output)
                .await;
        }

        let put_object_output = if self
            .config
            .transfer_config
            .is_multipart_upload_required(get_object_output.content_length().unwrap() as u64)
        {
            self.multipart_upload(bucket, key, get_object_output)
                .await?
        } else {
            self.singlepart_upload(bucket, key, get_object_output)
                .await?
        };

        trace!(key = key, "{put_object_output:?}");
        Ok(put_object_output)
    }

    fn modify_metadata(&self, mut get_object_output: GetObjectOutput) -> GetObjectOutput {
        if self.config.metadata.is_some() {
            get_object_output.metadata = Some(self.config.metadata.as_ref().unwrap().clone());
        }

        if self.config.put_last_modified_metadata {
            get_object_output = Self::modify_last_modified_metadata(get_object_output);
        }

        if self.config.enable_versioning {
            get_object_output = Self::update_versioning_metadata(get_object_output);
        }

        get_object_output
    }

    fn modify_last_modified_metadata(mut get_object_output: GetObjectOutput) -> GetObjectOutput {
        // skipcq: RS-W1031
        let mut metadata = get_object_output
            .metadata()
            .unwrap_or(&HashMap::new())
            .clone();
        let last_modified = DateTime::from_millis(
            get_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        )
        .to_chrono_utc()
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, false);

        metadata.insert(
            S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY.to_string(),
            last_modified,
        );
        get_object_output.metadata = Some(metadata);

        get_object_output
    }

    fn update_versioning_metadata(mut get_object_output: GetObjectOutput) -> GetObjectOutput {
        if get_object_output.version_id().is_none() {
            return get_object_output;
        }

        let source_version_id = get_object_output.version_id().unwrap();

        // skipcq: RS-W1031
        let mut metadata = get_object_output
            .metadata()
            .unwrap_or(&HashMap::new())
            .clone();

        let last_modified = DateTime::from_millis(
            get_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        )
        .to_chrono_utc()
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, false);

        metadata.insert(
            S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY.to_string(),
            source_version_id.to_string(),
        );
        metadata.insert(
            S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY.to_string(),
            last_modified,
        );

        get_object_output.metadata = Some(metadata);

        get_object_output
    }

    pub async fn upload_with_auto_chunksize(
        &mut self,
        bucket: &str,
        key: &str,
        get_object_output: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        if self.object_parts.as_ref().unwrap().is_empty() {
            panic!("Illegal object_parts state");
        }

        let put_object_output = self
            .multipart_upload(bucket, key, get_object_output)
            .await?;

        trace!(key = key, "{put_object_output:?}");

        Ok(put_object_output)
    }

    async fn multipart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        get_object_output: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        let storage_class = if self.config.storage_class.is_none() {
            get_object_output.storage_class().cloned()
        } else {
            Some(self.config.storage_class.as_ref().unwrap().clone())
        };

        let create_multipart_upload_output = self
            .client
            .create_multipart_upload()
            .set_storage_class(storage_class)
            .bucket(bucket)
            .key(key)
            .set_metadata(get_object_output.metadata().cloned())
            .set_tagging(self.tagging.clone())
            .set_content_type(if self.config.content_type.is_none() {
                get_object_output
                    .content_type()
                    .map(|value| value.to_string())
            } else {
                self.config.content_type.clone()
            })
            .set_content_encoding(if self.config.content_encoding.is_none() {
                get_object_output
                    .content_encoding()
                    .map(|value| value.to_string())
            } else {
                self.config.content_encoding.clone()
            })
            .set_cache_control(if self.config.cache_control.is_none() {
                get_object_output
                    .cache_control()
                    .map(|value| value.to_string())
            } else {
                self.config.cache_control.clone()
            })
            .set_content_disposition(if self.config.content_disposition.is_none() {
                get_object_output
                    .content_disposition()
                    .map(|value| value.to_string())
            } else {
                self.config.content_disposition.clone()
            })
            .set_content_language(if self.config.content_language.is_none() {
                get_object_output
                    .content_language()
                    .map(|value| value.to_string())
            } else {
                self.config.content_language.clone()
            })
            .set_expires(if self.config.expires.is_none() {
                get_object_output.expires().cloned()
            } else {
                Some(
                    DateTime::from_str(
                        &self.config.expires.unwrap().to_rfc3339(),
                        DateTimeFormat::DateTimeWithOffset,
                    )
                    .unwrap(),
                )
            })
            .set_server_side_encryption(self.config.sse.clone())
            .set_ssekms_key_id(self.config.sse_kms_key_id.clone().id.clone())
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .set_acl(self.config.canned_acl.clone())
            .set_checksum_algorithm(self.config.additional_checksum_algorithm.as_ref().cloned())
            .send()
            .await
            .context("aws_sdk_s3::client::Client create_multipart_upload() failed.")?;
        let upload_id = create_multipart_upload_output.upload_id().unwrap();

        let upload_result = self
            .upload_parts_and_complete(bucket, key, upload_id, get_object_output)
            .await
            .context("upload_parts() failed.");
        if upload_result.is_err() {
            self.client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .context("aws_sdk_s3::client::Client abort_multipart_upload() failed.")?;
            return Err(upload_result.err().unwrap());
        }

        upload_result
    }

    async fn upload_parts_and_complete(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        get_object_output: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        let source_sse = get_object_output.server_side_encryption().cloned();
        let source_remote_storage = get_object_output.e_tag().is_some();
        let source_content_length = get_object_output.content_length().unwrap();
        let source_e_tag = get_object_output.e_tag().map(|e_tag| e_tag.to_string());
        let source_local_storage = source_e_tag.is_none();
        let source_checksum = get_additional_checksum_from_get_object_result(
            &get_object_output,
            self.config.additional_checksum_algorithm.as_ref().cloned(),
        );
        let source_storage_class = get_object_output.storage_class().cloned();

        let upload_parts = if self.is_auto_chunksize_enabled() {
            self.upload_parts_with_auto_chunksize(bucket, key, upload_id, get_object_output)
                .await
                .context("upload_parts_with_auto_chunksize() failed.")?
        } else {
            self.upload_parts(bucket, key, upload_id, get_object_output)
                .await
                .context("upload_parts() failed.")?
        };

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let complete_multipart_upload_output = self
            .client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed_multipart_upload)
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::Client complete_multipart_upload() failed.")?;

        trace!(
            key = key,
            upload_id = upload_id,
            "{complete_multipart_upload_output:?}"
        );

        let source_e_tag = if source_local_storage {
            Some(self.generate_e_tag_hash(self.calculate_parts_count(source_content_length)))
        } else {
            source_e_tag
        };

        if !self.config.disable_etag_verify
            && !self.express_onezone_storage
            && source_storage_class != Some(StorageClass::ExpressOnezone)
        {
            let target_sse = complete_multipart_upload_output
                .server_side_encryption()
                .cloned();
            let target_e_tag = complete_multipart_upload_output
                .e_tag()
                .map(|e| e.to_string());

            self.verify_e_tag(
                key,
                &source_sse,
                source_remote_storage,
                &source_e_tag,
                &target_sse,
                &target_e_tag,
            )
            .await;
        }

        let target_checksum = get_additional_checksum_from_multipart_upload_result(
            &complete_multipart_upload_output,
            self.config.additional_checksum_algorithm.clone(),
        );

        self.validate_checksum(
            key,
            source_checksum,
            target_checksum,
            &source_e_tag,
            source_remote_storage,
        )
        .await;

        Ok(PutObjectOutput::builder()
            .e_tag(complete_multipart_upload_output.e_tag().unwrap())
            .build())
    }

    async fn verify_e_tag(
        &mut self,
        key: &str,
        source_sse: &Option<ServerSideEncryption>,
        source_remote_storage: bool,
        source_e_tag: &Option<String>,
        target_sse: &Option<ServerSideEncryption>,
        target_e_tag: &Option<String>,
    ) {
        let verify_result = storage::e_tag_verify::verify_e_tag(
            !self.config.disable_multipart_verify,
            &self.config.source_sse_c,
            &self.config.target_sse_c,
            source_sse,
            source_e_tag,
            target_sse,
            target_e_tag,
        );

        if let Some(e_tag_match) = verify_result {
            if !e_tag_match {
                if source_remote_storage
                    && is_multipart_upload_e_tag(source_e_tag)
                    && self.config.disable_multipart_verify
                {
                    trace!(
                        key = &key,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        "skip e_tag verification"
                    );
                } else {
                    let message = if source_remote_storage
                        && is_multipart_upload_e_tag(source_e_tag)
                        && !self.is_auto_chunksize_enabled()
                    {
                        format!("{} {}", "e_tag", MISMATCH_WARNING_WITH_HELP)
                    } else {
                        "e_tag mismatch. file in the target storage may be corrupted.".to_string()
                    };

                    self.send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;

                    let source_e_tag = source_e_tag.clone().unwrap();
                    let target_e_tag = target_e_tag.clone().unwrap();

                    warn!(
                        key = &key,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        message
                    );
                }
            } else {
                self.send_stats(EtagVerified {
                    key: key.to_string(),
                })
                .await;

                trace!(
                    key = &key,
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    "e_tag verified."
                );
            }
        }
    }

    async fn upload_parts(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        get_object_output: GetObjectOutput,
    ) -> Result<Vec<CompletedPart>> {
        let mut upload_parts: Vec<CompletedPart> = Vec::new();

        let mut part_number = 1;
        let mut remaining_bytes = get_object_output.content_length().unwrap() as u64;

        let mut body = get_object_output.body.into_async_read();
        while 0 < remaining_bytes {
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }

            let chunksize = if remaining_bytes < self.config.transfer_config.multipart_chunksize {
                remaining_bytes
            } else {
                self.config.transfer_config.multipart_chunksize
            };

            let mut buffer =
                Vec::<u8>::with_capacity(self.config.transfer_config.multipart_chunksize as usize);
            buffer.resize_with(chunksize as usize, Default::default);
            body.read_exact(buffer.as_mut_slice())
                .await
                .context("async_read_ext::AsyncReadExt read_exact() failed.")?;

            let md5_digest_base64 = if !self.express_onezone_storage {
                let md5_digest = md5::compute(&buffer);
                self.concatnated_md5_hash
                    .append(&mut md5_digest.as_slice().to_vec());

                Some(general_purpose::STANDARD.encode(md5_digest.as_slice()))
            } else {
                None
            };

            let upload_part_output = self
                .client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .set_content_md5(md5_digest_base64)
                .content_length(chunksize as i64)
                .set_checksum_algorithm(self.config.additional_checksum_algorithm.clone())
                .set_sse_customer_algorithm(self.config.target_sse_c.clone())
                .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
                .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
                .body(ByteStream::from(buffer))
                .send()
                .await
                .context("aws_sdk_s3::client::Client upload_part() failed.")?;

            trace!(key = key, "{upload_part_output:?}");

            upload_parts.push(
                CompletedPart::builder()
                    .e_tag(upload_part_output.e_tag().unwrap())
                    .set_checksum_sha256(
                        upload_part_output
                            .checksum_sha256()
                            .map(|digest| digest.to_string()),
                    )
                    .set_checksum_sha1(
                        upload_part_output
                            .checksum_sha1()
                            .map(|digest| digest.to_string()),
                    )
                    .set_checksum_crc32(
                        upload_part_output
                            .checksum_crc32()
                            .map(|digest| digest.to_string()),
                    )
                    .set_checksum_crc32_c(
                        upload_part_output
                            .checksum_crc32_c()
                            .map(|digest| digest.to_string()),
                    )
                    .part_number(part_number)
                    .build(),
            );

            remaining_bytes -= chunksize;
            part_number += 1;
        }
        trace!(key = key, upload_id = upload_id, "{upload_parts:?}");

        Ok(upload_parts)
    }

    async fn upload_parts_with_auto_chunksize(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        get_object_output: GetObjectOutput,
    ) -> Result<Vec<CompletedPart>> {
        let mut upload_parts: Vec<CompletedPart> = Vec::new();

        let mut part_number = 1;

        let mut body = get_object_output.body.into_async_read();
        while part_number <= self.object_parts.as_ref().unwrap().len() {
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }
            let chunksize = self
                .object_parts
                .as_ref()
                .unwrap()
                .get(part_number - 1)
                .unwrap()
                .size()
                .unwrap();

            let mut buffer = Vec::<u8>::with_capacity(chunksize as usize);
            buffer.resize_with(chunksize as usize, Default::default);
            body.read_exact(buffer.as_mut_slice())
                .await
                .context("async_read_ext::AsyncReadExt read_exact() failed.")?;

            let md5_digest_base64 = if !self.express_onezone_storage {
                let md5_digest = md5::compute(&buffer);
                self.concatnated_md5_hash
                    .append(&mut md5_digest.as_slice().to_vec());

                Some(general_purpose::STANDARD.encode(md5_digest.as_slice()))
            } else {
                None
            };

            let upload_part_output = self
                .client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number as i32)
                .set_content_md5(md5_digest_base64)
                .content_length(chunksize)
                .set_checksum_algorithm(self.config.additional_checksum_algorithm.clone())
                .set_sse_customer_algorithm(self.config.target_sse_c.clone())
                .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
                .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
                .body(ByteStream::from(buffer))
                .send()
                .await
                .context("aws_sdk_s3::client::Client upload_part() failed.")?;

            trace!(key = key, "{upload_part_output:?}");

            upload_parts.push(
                CompletedPart::builder()
                    .e_tag(upload_part_output.e_tag().unwrap())
                    .set_checksum_sha256(
                        upload_part_output
                            .checksum_sha256()
                            .map(|digest| digest.to_string()),
                    )
                    .set_checksum_sha1(
                        upload_part_output
                            .checksum_sha1()
                            .map(|digest| digest.to_string()),
                    )
                    .set_checksum_crc32(
                        upload_part_output
                            .checksum_crc32()
                            .map(|digest| digest.to_string()),
                    )
                    .set_checksum_crc32_c(
                        upload_part_output
                            .checksum_crc32_c()
                            .map(|digest| digest.to_string()),
                    )
                    .part_number(part_number as i32)
                    .build(),
            );

            part_number += 1;
        }
        trace!(key = key, upload_id = upload_id, "{upload_parts:?}");

        Ok(upload_parts)
    }

    async fn singlepart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        mut get_object_output: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        let source_sse = get_object_output.server_side_encryption().cloned();
        let source_remote_storage = get_object_output.e_tag().is_some();
        let source_e_tag = get_object_output.e_tag().map(|e_tag| e_tag.to_string());
        let source_local_storage = source_e_tag.is_none();
        let source_checksum = get_additional_checksum_from_get_object_result(
            &get_object_output,
            self.config.additional_checksum_algorithm.as_ref().cloned(),
        );
        let source_storage_class = get_object_output.storage_class().cloned();

        let mut body = get_object_output.body.into_async_read();
        get_object_output.body = ByteStream::from_static(b"");

        // When created from an SdkBody, care must be taken to ensure retriability.
        // An SdkBody is retryable when constructed from in-memory data or when using SdkBody::retryable.
        let mut buffer =
            Vec::<u8>::with_capacity(get_object_output.content_length().unwrap() as usize);
        buffer.resize_with(
            get_object_output.content_length().unwrap() as usize,
            Default::default,
        );
        body.read_exact(buffer.as_mut_slice())
            .await
            .context("async_read_ext::AsyncReadExt read_exact() failed.")?;

        let md5_digest_base64 = if !self.express_onezone_storage {
            let md5_digest = md5::compute(&buffer);
            self.concatnated_md5_hash
                .append(&mut md5_digest.as_slice().to_vec());

            Some(general_purpose::STANDARD.encode(md5_digest.as_slice()))
        } else {
            None
        };

        let buffer_stream = ByteStream::from(buffer);

        let storage_class = if self.config.storage_class.is_none() {
            get_object_output.storage_class().cloned()
        } else {
            Some(self.config.storage_class.as_ref().unwrap().clone())
        };

        let put_object_output = self
            .client
            .put_object()
            .set_storage_class(storage_class)
            .bucket(bucket)
            .key(key)
            .content_length(get_object_output.content_length().unwrap())
            .body(buffer_stream)
            .set_metadata(get_object_output.metadata().cloned())
            .set_tagging(self.tagging.clone())
            .set_content_md5(md5_digest_base64)
            .set_content_type(if self.config.content_type.is_none() {
                get_object_output
                    .content_type()
                    .map(|value| value.to_string())
            } else {
                self.config.content_type.clone()
            })
            .set_content_encoding(if self.config.content_encoding.is_none() {
                get_object_output
                    .content_encoding()
                    .map(|value| value.to_string())
            } else {
                self.config.content_encoding.clone()
            })
            .set_cache_control(if self.config.cache_control.is_none() {
                get_object_output
                    .cache_control()
                    .map(|value| value.to_string())
            } else {
                self.config.cache_control.clone()
            })
            .set_content_disposition(if self.config.content_disposition.is_none() {
                get_object_output
                    .content_disposition()
                    .map(|value| value.to_string())
            } else {
                self.config.content_disposition.clone()
            })
            .set_content_language(if self.config.content_language.is_none() {
                get_object_output
                    .content_language()
                    .map(|value| value.to_string())
            } else {
                self.config.content_language.clone()
            })
            .set_expires(if self.config.expires.is_none() {
                get_object_output.expires().cloned()
            } else {
                Some(
                    DateTime::from_str(
                        &self.config.expires.unwrap().to_rfc3339(),
                        DateTimeFormat::DateTimeWithOffset,
                    )
                    .unwrap(),
                )
            })
            .set_server_side_encryption(self.config.sse.clone())
            .set_ssekms_key_id(self.config.sse_kms_key_id.clone().id.clone())
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .set_acl(self.config.canned_acl.clone())
            .set_checksum_algorithm(self.config.additional_checksum_algorithm.as_ref().cloned())
            .send()
            .await
            .context("aws_sdk_s3::client::Client put_object() failed.")?;

        let source_e_tag = if source_local_storage {
            Some(self.generate_e_tag_hash(0))
        } else {
            source_e_tag
        };

        if !self.config.disable_etag_verify
            && !self.express_onezone_storage
            && source_storage_class != Some(StorageClass::ExpressOnezone)
        {
            let target_sse = put_object_output.server_side_encryption().cloned();
            let target_e_tag = put_object_output.e_tag().map(|e| e.to_string());

            self.verify_e_tag(
                key,
                &source_sse,
                source_remote_storage,
                &source_e_tag,
                &target_sse,
                &target_e_tag,
            )
            .await;
        }

        let target_checksum = get_additional_checksum_from_put_object_result(
            &put_object_output,
            self.config.additional_checksum_algorithm.as_ref().cloned(),
        );

        self.validate_checksum(
            key,
            source_checksum,
            target_checksum,
            &source_e_tag,
            source_remote_storage,
        )
        .await;

        Ok(put_object_output)
    }

    async fn validate_checksum(
        &mut self,
        key: &str,
        source_checksum: Option<String>,
        target_checksum: Option<String>,
        source_e_tag: &Option<String>,
        source_remote_storage: bool,
    ) {
        if self.config.additional_checksum_mode.is_some() && source_checksum.is_none() {
            self.send_stats(SyncWarning {
                key: key.to_string(),
            })
            .await;

            warn!(
                key = &key,
                "additional checksum algorithm is different from the target storage. skip additional checksum verification."
            );
        }

        if target_checksum.is_some() && source_checksum.is_some() {
            let target_checksum = target_checksum.unwrap();
            let source_checksum = source_checksum.unwrap();

            let additional_checksum_algorithm = self
                .config
                .additional_checksum_algorithm
                .as_ref()
                .unwrap()
                .as_str();

            if target_checksum != source_checksum {
                if source_remote_storage
                    && is_multipart_upload_e_tag(source_e_tag)
                    && self.config.disable_multipart_verify
                {
                    trace!(
                        key = &key,
                        additional_checksum_algorithm = additional_checksum_algorithm,
                        target_checksum = target_checksum,
                        source_checksum = source_checksum,
                        "skip additional checksum verification."
                    );
                } else {
                    self.send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;

                    let message = if source_remote_storage
                        && is_multipart_upload_e_tag(source_e_tag)
                        && !self.is_auto_chunksize_enabled()
                    {
                        format!("{} {}", "additional checksum", MISMATCH_WARNING_WITH_HELP)
                    } else {
                        "additional checksum mismatch. file in the target storage may be corrupted."
                            .to_string()
                    };

                    warn!(
                        key = &key,
                        additional_checksum_algorithm = additional_checksum_algorithm,
                        target_checksum = target_checksum,
                        source_checksum = source_checksum,
                        message
                    );
                }
            } else {
                self.send_stats(ChecksumVerified {
                    key: key.to_string(),
                })
                .await;

                trace!(
                    key = &key,
                    additional_checksum_algorithm = additional_checksum_algorithm,
                    target_checksum = target_checksum,
                    source_checksum = source_checksum,
                    "additional checksum verified.",
                );
            }
        }
    }

    fn generate_e_tag_hash(&self, parts_count: i64) -> String {
        generate_e_tag_hash(&self.concatnated_md5_hash, parts_count)
    }

    fn calculate_parts_count(&self, content_length: i64) -> i64 {
        calculate_parts_count(
            self.config.transfer_config.multipart_threshold as i64,
            self.config.transfer_config.multipart_chunksize as i64,
            content_length,
        )
    }

    async fn send_stats(&self, stats: SyncStatistics) {
        let _ = self.stats_sender.send(stats).await;
    }

    fn is_auto_chunksize_enabled(&self) -> bool {
        self.config.transfer_config.auto_chunksize && self.object_parts.is_some()
    }
}

fn calculate_parts_count(
    multipart_threshold: i64,
    multipart_chunksize: i64,
    content_length: i64,
) -> i64 {
    if content_length < multipart_threshold {
        return 0;
    }

    if content_length % multipart_chunksize == 0 {
        return content_length / multipart_chunksize;
    }

    (content_length / multipart_chunksize) + 1
}

pub fn get_additional_checksum_from_put_object_result(
    put_object_output: &PutObjectOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => put_object_output
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => put_object_output
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => put_object_output
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => put_object_output
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

pub fn get_additional_checksum_from_get_object_result(
    get_object_output: &GetObjectOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => get_object_output
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => get_object_output
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => get_object_output
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => get_object_output
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

pub fn get_additional_checksum_from_multipart_upload_result(
    complete_multipart_upload_result: &CompleteMultipartUploadOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => complete_multipart_upload_result
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => complete_multipart_upload_result
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => complete_multipart_upload_result
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => complete_multipart_upload_result
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::primitives::DateTime;

    use super::*;

    #[test]
    fn update_versioning_metadata_with_new() {
        init_dummy_tracing_subscriber();

        let mut get_object_output = GetObjectOutput::builder()
            .last_modified(DateTime::from_secs(0))
            .version_id("version1")
            .build();

        get_object_output = UploadManager::update_versioning_metadata(get_object_output);
        assert_eq!(
            get_object_output
                .metadata()
                .as_ref()
                .unwrap()
                .get(S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY)
                .unwrap(),
            "version1"
        );
        assert_eq!(
            get_object_output
                .metadata()
                .as_ref()
                .unwrap()
                .get(S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY)
                .unwrap(),
            "1970-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn update_versioning_metadata_with_update() {
        init_dummy_tracing_subscriber();

        let mut get_object_output = GetObjectOutput::builder()
            .last_modified(DateTime::from_secs(0))
            .version_id("version1")
            .metadata("mykey", "myvalue")
            .build();

        get_object_output = UploadManager::update_versioning_metadata(get_object_output);
        assert_eq!(
            get_object_output
                .metadata()
                .as_ref()
                .unwrap()
                .get("mykey")
                .unwrap(),
            "myvalue"
        );
        assert_eq!(
            get_object_output
                .metadata()
                .as_ref()
                .unwrap()
                .get(S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY)
                .unwrap(),
            "version1"
        );
        assert_eq!(
            get_object_output
                .metadata()
                .as_ref()
                .unwrap()
                .get(S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY)
                .unwrap(),
            "1970-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn update_versioning_metadata_without_version_id() {
        init_dummy_tracing_subscriber();

        let mut get_object_output = GetObjectOutput::builder()
            .last_modified(DateTime::from_secs(0))
            .metadata("mykey", "myvalue")
            .build();

        get_object_output = UploadManager::update_versioning_metadata(get_object_output);
        assert_eq!(get_object_output.metadata().as_ref().unwrap().len(), 1);
        assert_eq!(
            get_object_output.metadata().unwrap().get("mykey").unwrap(),
            "myvalue"
        );
    }

    #[test]
    fn modify_last_modified_metadata_with_new() {
        init_dummy_tracing_subscriber();

        let mut get_object_output = GetObjectOutput::builder()
            .last_modified(DateTime::from_secs(0))
            .build();
        get_object_output = UploadManager::modify_last_modified_metadata(get_object_output);

        assert_eq!(
            get_object_output
                .metadata()
                .unwrap()
                .get(S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY)
                .unwrap(),
            "1970-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn modify_last_modified_metadata_with_update() {
        init_dummy_tracing_subscriber();

        let mut get_object_output = GetObjectOutput::builder()
            .last_modified(DateTime::from_secs(0))
            .metadata("key1", "value1")
            .build();
        get_object_output = UploadManager::modify_last_modified_metadata(get_object_output);

        assert_eq!(
            get_object_output
                .metadata()
                .unwrap()
                .get(S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY)
                .unwrap(),
            "1970-01-01T00:00:00+00:00"
        );
        assert_eq!(
            get_object_output.metadata().unwrap().get("key1").unwrap(),
            "value1"
        );
    }

    #[test]
    fn calculate_parts_count_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
            1
        );

        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, (8 * 1024 * 1024) - 1),
            0
        );

        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024),
            2
        );

        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, (16 * 1024 * 1024) + 1),
            3
        );
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
