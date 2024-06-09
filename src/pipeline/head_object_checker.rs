use anyhow::{anyhow, Result};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::types::builders::ObjectPartBuilder;
use aws_sdk_s3::types::{ChecksumMode, ServerSideEncryption};
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::{debug, warn};

use crate::storage::additional_checksum_verify::generate_checksum_from_path_for_check;
use crate::storage::e_tag_verify::{
    generate_e_tag_hash_from_path, generate_e_tag_hash_from_path_with_auto_chunksize,
    normalize_e_tag,
};
use crate::storage::local::fs_util;
use crate::storage::Storage;
use crate::types::S3syncObject;
use crate::types::SyncStatistics::SyncWarning;
use crate::{types, Config};

const FILTER_NAME: &str = "HeadObjectChecker";

pub struct HeadObjectChecker {
    worker_index: u16,
    config: Config,
    source: Storage,
    target: Storage,
}

impl HeadObjectChecker {
    pub fn new(config: Config, source: Storage, target: Storage, worker_index: u16) -> Self {
        Self {
            config,
            source,
            target,
            worker_index,
        }
    }

    pub(crate) async fn is_sync_required(&self, source_object: &S3syncObject) -> Result<bool> {
        if !self.is_head_object_check_required() {
            return Ok(true);
        }

        if self.check_target_local_storage_allow_overwrite() {
            return Ok(true);
        }

        match &source_object {
            S3syncObject::NotVersioning(_) => self.is_old_object(source_object).await,
            _ => {
                panic!("versioning object has been detected.")
            }
        }
    }

    async fn is_old_object(&self, source_object: &S3syncObject) -> Result<bool> {
        let checksum_mode = if self.config.filter_config.check_checksum_algorithm.is_some() {
            Some(ChecksumMode::Enabled)
        } else {
            None
        };

        let key = source_object.key();
        let head_target_object_output = self
            .target
            .head_object(
                key,
                None,
                checksum_mode,
                self.config.target_sse_c.clone(),
                self.config.target_sse_c_key.clone(),
                self.config.target_sse_c_key_md5.clone(),
            )
            .await;

        if let Ok(target_object) = head_target_object_output {
            return if self.config.filter_config.check_size {
                let different_size =
                    source_object.size() != target_object.content_length().unwrap();
                if !different_size {
                    let content_length = source_object.size();
                    let key = source_object.key();

                    debug!(
                        name = FILTER_NAME,
                        content_length = content_length,
                        key = key,
                        "object filtered."
                    );
                }

                Ok(different_size)
            } else if self.config.filter_config.check_etag
                && (self.config.head_each_target || self.config.transfer_config.auto_chunksize)
            {
                if !self.source.is_local_storage() && !self.target.is_local_storage() {
                    return self
                        .are_different_e_tags(key, source_object, &target_object)
                        .await;
                } else if self.source.is_local_storage() && !self.target.is_local_storage() {
                    Ok(self
                        .is_source_local_e_tag_different_from_target_s3(key, &target_object)
                        .await?)
                } else if !self.source.is_local_storage() && self.target.is_local_storage() {
                    Ok(self
                        .is_target_local_e_tag_different_from_source_s3(key, source_object)
                        .await?)
                } else {
                    panic!("source and target are both local storage.")
                }
            } else if self.config.filter_config.check_etag {
                // ETag has been checked by modified filter
                Ok(true)
            } else if self.config.filter_config.check_checksum_algorithm.is_some() {
                if !self.source.is_local_storage() && !self.target.is_local_storage() {
                    return self.are_different_checksums(key, &target_object).await;
                } else if self.source.is_local_storage() && !self.target.is_local_storage() {
                    return self
                        .is_source_local_checksum_different_from_target_s3(key, &target_object)
                        .await;
                } else if !self.source.is_local_storage() && self.target.is_local_storage() {
                    return self
                        .is_target_local_checksum_different_from_source_s3(key, &target_object)
                        .await;
                } else {
                    panic!("source and target are both local storage.")
                }
            } else {
                Ok(is_object_modified(source_object, &target_object))
            };
        }

        if is_head_object_not_found_error(head_target_object_output.as_ref().err().unwrap()) {
            return Ok(true);
        }

        self.target
            .send_stats(SyncWarning {
                key: key.to_string(),
            })
            .await;

        let error = head_target_object_output
            .as_ref()
            .err()
            .unwrap()
            .to_string();
        let source = head_target_object_output.as_ref().err().unwrap().source();
        warn!(
            worker_index = self.worker_index,
            error = error,
            source = source,
            "head_object() failed."
        );

        Err(anyhow!("head_object() failed. key={}.", key,))
    }

    async fn are_different_e_tags(
        &self,
        key: &str,
        source_object: &S3syncObject,
        head_target_object_output: &HeadObjectOutput,
    ) -> Result<bool> {
        let source_e_tag = normalize_e_tag(&Some(source_object.e_tag().unwrap().to_string()));
        let target_e_tag = normalize_e_tag(&Some(
            head_target_object_output.e_tag.clone().unwrap().to_string(),
        ));

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            source_object.last_modified().to_millis().unwrap(),
        ))
        .unwrap()
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();

        if let Some(sse) = head_target_object_output.server_side_encryption() {
            if sse != &ServerSideEncryption::Aes256 {
                self.target
                    .send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;

                warn!(
                    name = FILTER_NAME,
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = source_object.size(),
                    target_size = head_target_object_output.content_length().unwrap(),
                    key = key,
                    "object filtered. Only ServerSideEncryption::Aes256 is supported."
                );

                return Ok(false);
            }
        }

        if source_e_tag == target_e_tag {
            if source_object.size() != head_target_object_output.content_length().unwrap() {
                panic!("ETags are same but sizes are different.");
            }

            debug!(
                name = FILTER_NAME,
                source_e_tag = source_e_tag,
                target_e_tag = target_e_tag,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = source_object.size(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. ETags are same."
            );

            return Ok(false);
        } else {
            let head_source_object_output = self
                .source
                .head_object(
                    key,
                    None,
                    None,
                    self.config.source_sse_c.clone(),
                    self.config.source_sse_c_key.clone(),
                    self.config.source_sse_c_key_md5.clone(),
                )
                .await?;

            if let Some(sse) = head_source_object_output.server_side_encryption() {
                if sse != &ServerSideEncryption::Aes256 {
                    self.target
                        .send_stats(SyncWarning {
                            key: key.to_string(),
                        })
                        .await;

                    warn!(
                        name = FILTER_NAME,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        source_last_modified = source_last_modified,
                        target_last_modified = target_last_modified,
                        source_size = head_source_object_output.content_length().unwrap(),
                        target_size = head_target_object_output.content_length().unwrap(),
                        key = key,
                        "object filtered. Only ServerSideEncryption::Aes256 is supported."
                    );

                    return Ok(false);
                }
            }

            debug!(
                name = FILTER_NAME,
                source_e_tag = source_e_tag,
                target_e_tag = target_e_tag,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = source_object.size(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "ETags are different."
            );
        }

        Ok(true)
    }

    async fn is_source_local_e_tag_different_from_target_s3(
        &self,
        key: &str,
        head_target_object_output: &HeadObjectOutput,
    ) -> Result<bool> {
        let source_e_tag = if self.source.is_local_storage() {
            let mut local_path = self.source.get_local_path();
            local_path.push(key);

            if self.config.transfer_config.auto_chunksize {
                if let Ok(object_parts) = self
                    .target
                    .get_object_parts(
                        key,
                        None,
                        self.config.target_sse_c.clone(),
                        self.config.target_sse_c_key.clone(),
                        self.config.target_sse_c_key_md5.clone(),
                    )
                    .await
                {
                    if object_parts.is_empty() {
                        generate_e_tag_hash_from_path(
                            &local_path,
                            self.config.transfer_config.multipart_chunksize as usize,
                            self.config.transfer_config.multipart_threshold as usize,
                        )
                        .await?
                    } else {
                        generate_e_tag_hash_from_path_with_auto_chunksize(
                            &local_path,
                            object_parts
                                .iter()
                                .map(|part| part.size().unwrap())
                                .collect(),
                        )
                        .await
                        .unwrap()
                    }
                } else {
                    return Err(anyhow!("get_object_parts() failed. key={}.", key,));
                }
            } else {
                generate_e_tag_hash_from_path(
                    &local_path,
                    self.config.transfer_config.multipart_chunksize as usize,
                    self.config.transfer_config.multipart_threshold as usize,
                )
                .await?
            }
        } else {
            panic!("source is not local storage.")
        };

        let source_e_tag = normalize_e_tag(&Some(source_e_tag));
        let target_e_tag = normalize_e_tag(&Some(
            head_target_object_output.e_tag().unwrap().to_string(),
        ));

        let mut local_path = self.source.get_local_path();
        local_path.push(key);

        let head_source_object_output = self
            .source
            .head_object(
                key,
                None,
                Some(ChecksumMode::Enabled),
                self.config.source_sse_c.clone(),
                self.config.source_sse_c_key.clone(),
                self.config.source_sse_c_key_md5.clone(),
            )
            .await?;

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_source_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();

        if let Some(sse) = head_target_object_output.server_side_encryption() {
            if sse != &ServerSideEncryption::Aes256 {
                self.target
                    .send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;

                warn!(
                    name = FILTER_NAME,
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = head_source_object_output.content_length().unwrap(),
                    target_size = head_target_object_output.content_length().unwrap(),
                    key = key,
                    "object filtered. Only ServerSideEncryption::Aes256 is supported."
                );

                return Ok(false);
            }
        }

        if source_e_tag == target_e_tag {
            if head_source_object_output.content_length().unwrap()
                != head_target_object_output.content_length().unwrap()
            {
                panic!("ETags are same but sizes are different.");
            }

            debug!(
                name = FILTER_NAME,
                source_e_tag = source_e_tag,
                target_e_tag = target_e_tag,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. ETags are same."
            );
            return Ok(false);
        } else {
            debug!(
                name = FILTER_NAME,
                source_e_tag = source_e_tag,
                target_e_tag = target_e_tag,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "ETags are different."
            );
        }

        Ok(true)
    }

    async fn is_target_local_e_tag_different_from_source_s3(
        &self,
        key: &str,
        source_object: &S3syncObject,
    ) -> Result<bool> {
        let source_e_tag = source_object.e_tag();
        let target_e_tag = if self.target.is_local_storage() {
            let local_path = fs_util::key_to_file_path(self.target.get_local_path(), key);

            if self.config.transfer_config.auto_chunksize {
                if let Ok(object_parts) = self
                    .source
                    .get_object_parts(
                        key,
                        None,
                        self.config.target_sse_c.clone(),
                        self.config.target_sse_c_key.clone(),
                        self.config.target_sse_c_key_md5.clone(),
                    )
                    .await
                {
                    if object_parts.is_empty() {
                        generate_e_tag_hash_from_path(
                            &local_path,
                            self.config.transfer_config.multipart_chunksize as usize,
                            self.config.transfer_config.multipart_threshold as usize,
                        )
                        .await?
                    } else {
                        generate_e_tag_hash_from_path_with_auto_chunksize(
                            &local_path,
                            object_parts
                                .iter()
                                .map(|part| part.size().unwrap())
                                .collect(),
                        )
                        .await
                        .unwrap()
                    }
                } else {
                    return Err(anyhow!("get_object_parts() failed. key={}.", key,));
                }
            } else {
                generate_e_tag_hash_from_path(
                    &local_path,
                    self.config.transfer_config.multipart_chunksize as usize,
                    self.config.transfer_config.multipart_threshold as usize,
                )
                .await?
            }
        } else {
            panic!("target is not local storage.")
        };

        let source_e_tag = normalize_e_tag(&Some(source_e_tag.unwrap().to_string()));
        let target_e_tag = normalize_e_tag(&Some(target_e_tag));

        let head_target_object_output = self
            .target
            .head_object(
                key,
                None,
                Some(ChecksumMode::Enabled),
                self.config.target_sse_c.clone(),
                self.config.target_sse_c_key.clone(),
                self.config.target_sse_c_key_md5.clone(),
            )
            .await?;

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            source_object.last_modified().to_millis().unwrap(),
        ))
        .unwrap()
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();

        if source_e_tag == target_e_tag {
            if source_object.size() != head_target_object_output.content_length().unwrap() {
                panic!("ETags are same but sizes are different.");
            }

            debug!(
                name = FILTER_NAME,
                source_e_tag = source_e_tag,
                target_e_tag = target_e_tag,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = source_object.size(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. ETags are same."
            );
            return Ok(false);
        } else {
            let head_source_object_output = self
                .source
                .head_object(
                    key,
                    None,
                    None,
                    self.config.source_sse_c.clone(),
                    self.config.source_sse_c_key.clone(),
                    self.config.source_sse_c_key_md5.clone(),
                )
                .await?;

            if let Some(sse) = head_source_object_output.server_side_encryption() {
                if sse != &ServerSideEncryption::Aes256 {
                    self.target
                        .send_stats(SyncWarning {
                            key: key.to_string(),
                        })
                        .await;

                    warn!(
                        name = FILTER_NAME,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        source_last_modified = source_last_modified,
                        target_last_modified = target_last_modified,
                        source_size = head_source_object_output.content_length().unwrap(),
                        target_size = head_target_object_output.content_length().unwrap(),
                        key = key,
                        "object filtered. Only ServerSideEncryption::Aes256 is supported."
                    );

                    return Ok(false);
                }
            }

            debug!(
                name = FILTER_NAME,
                source_e_tag = source_e_tag,
                target_e_tag = target_e_tag,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = source_object.size(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "ETags are different."
            );
        }

        Ok(true)
    }

    async fn are_different_checksums(
        &self,
        key: &str,
        head_target_object_output: &HeadObjectOutput,
    ) -> Result<bool> {
        let head_source_object_output = self
            .source
            .head_object(
                key,
                None,
                Some(ChecksumMode::Enabled),
                self.config.source_sse_c.clone(),
                self.config.source_sse_c_key.clone(),
                self.config.source_sse_c_key_md5.clone(),
            )
            .await?;

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_source_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();

        let source_checksum = types::get_additional_checksum_with_head_object(
            &head_source_object_output,
            self.config.filter_config.check_checksum_algorithm.clone(),
        );
        let target_checksum = types::get_additional_checksum_with_head_object(
            head_target_object_output,
            self.config.filter_config.check_checksum_algorithm.clone(),
        );

        if source_checksum.is_none() {
            self.target
                .send_stats(SyncWarning {
                    key: key.to_string(),
                })
                .await;

            warn!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = source_checksum.unwrap_or_default(),
                target_checksum = target_checksum.unwrap_or_default(),
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Source checksum not found."
            );
            return Ok(false);
        }

        if source_checksum == target_checksum
            && source_checksum.is_some()
            && target_checksum.is_some()
        {
            if head_source_object_output.content_length().unwrap()
                != head_target_object_output.content_length().unwrap()
            {
                panic!("Checksums are same but sizes are different.");
            }

            debug!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = source_checksum.unwrap_or_default(),
                target_checksum = target_checksum.unwrap_or_default(),
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Checksums are same."
            );
            return Ok(false);
        } else {
            debug!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = source_checksum.unwrap_or_default(),
                target_checksum = target_checksum.unwrap_or_default(),
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "Checksums are different or not found."
            );
        }

        Ok(true)
    }

    async fn is_source_local_checksum_different_from_target_s3(
        &self,
        key: &str,
        head_target_object_output: &HeadObjectOutput,
    ) -> Result<bool> {
        let mut local_path = self.source.get_local_path();
        local_path.push(key);

        let head_source_object_output = self
            .source
            .head_object(
                key,
                None,
                Some(ChecksumMode::Enabled),
                self.config.source_sse_c.clone(),
                self.config.source_sse_c_key.clone(),
                self.config.source_sse_c_key_md5.clone(),
            )
            .await?;

        let target_checksum = types::get_additional_checksum_with_head_object(
            head_target_object_output,
            self.config.filter_config.check_checksum_algorithm.clone(),
        );

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_source_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();

        if target_checksum.is_none() {
            self.target
                .send_stats(SyncWarning {
                    key: key.to_string(),
                })
                .await;

            warn!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = "",
                target_checksum = target_checksum.unwrap_or_default(),
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Target checksum not found."
            );

            return Ok(false);
        }

        let mut target_object_parts = self
            .target
            .get_object_parts_attributes(
                key,
                None,
                self.config.max_keys,
                self.config.target_sse_c.clone(),
                self.config.target_sse_c_key.clone(),
                self.config.target_sse_c_key_md5.clone(),
            )
            .await?;

        let multipart = !target_object_parts.is_empty();
        if !multipart {
            target_object_parts.push(
                ObjectPartBuilder::default()
                    .size(head_target_object_output.content_length().unwrap())
                    .build(),
            );
        }

        let source_checksum = generate_checksum_from_path_for_check(
            &local_path,
            self.config
                .filter_config
                .check_checksum_algorithm
                .clone()
                .unwrap(),
            multipart,
            target_object_parts
                .iter()
                .map(|part| part.size().unwrap())
                .collect(),
        )
        .await?;

        if source_checksum == target_checksum.as_ref().unwrap().as_str() {
            if head_source_object_output.content_length().unwrap()
                != head_target_object_output.content_length().unwrap()
            {
                panic!("Checksums are same but sizes are different.");
            }

            debug!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = source_checksum,
                target_checksum = target_checksum.unwrap_or_default(),
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Checksums are same."
            );
            return Ok(false);
        } else {
            debug!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = source_checksum,
                target_checksum = target_checksum.unwrap_or_default(),
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "Checksums are different."
            );
        }

        Ok(true)
    }

    async fn is_target_local_checksum_different_from_source_s3(
        &self,
        key: &str,
        head_target_object_output: &HeadObjectOutput,
    ) -> Result<bool> {
        let local_path = fs_util::key_to_file_path(self.target.get_local_path(), key);

        let head_source_object_output = self
            .source
            .head_object(
                key,
                None,
                Some(ChecksumMode::Enabled),
                self.config.source_sse_c.clone(),
                self.config.source_sse_c_key.clone(),
                self.config.source_sse_c_key_md5.clone(),
            )
            .await?;

        let source_checksum = types::get_additional_checksum_with_head_object(
            &head_source_object_output,
            self.config.filter_config.check_checksum_algorithm.clone(),
        );

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_source_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))
        .unwrap()
        .to_rfc3339();

        if source_checksum.is_none() {
            self.target
                .send_stats(SyncWarning {
                    key: key.to_string(),
                })
                .await;

            warn!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = source_checksum.unwrap_or_default(),
                target_checksum = "",
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Source checksum not found."
            );

            return Ok(false);
        }

        let mut source_object_parts = self
            .source
            .get_object_parts_attributes(
                key,
                None,
                self.config.max_keys,
                self.config.source_sse_c.clone(),
                self.config.source_sse_c_key.clone(),
                self.config.source_sse_c_key_md5.clone(),
            )
            .await?;

        let multipart = !source_object_parts.is_empty();
        if !multipart {
            source_object_parts.push(
                ObjectPartBuilder::default()
                    .size(head_source_object_output.content_length().unwrap())
                    .build(),
            );
        }

        let target_checksum = generate_checksum_from_path_for_check(
            &local_path,
            self.config
                .filter_config
                .check_checksum_algorithm
                .clone()
                .unwrap(),
            multipart,
            source_object_parts
                .iter()
                .map(|part| part.size().unwrap())
                .collect(),
        )
        .await?;

        if source_checksum.is_some()
            && source_checksum.as_ref().unwrap().as_str() == target_checksum
        {
            if head_source_object_output.content_length().unwrap()
                != head_target_object_output.content_length().unwrap()
            {
                panic!("Checksums are same but sizes are different.");
            }

            debug!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = source_checksum.unwrap_or_default(),
                target_checksum = target_checksum,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Checksums are same."
            );
            return Ok(false);
        } else {
            debug!(
                name = FILTER_NAME,
                checksum_algorithm = self
                    .config
                    .filter_config
                    .check_checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .to_string(),
                source_checksum = source_checksum.unwrap_or_default(),
                target_checksum = target_checksum,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "Checksums are different or not found."
            );
        }

        Ok(true)
    }

    fn is_head_object_check_required(&self) -> bool {
        if self.config.transfer_config.auto_chunksize && self.config.filter_config.check_etag {
            return true;
        }

        if self.config.filter_config.check_checksum_algorithm.is_some() {
            return true;
        }

        is_head_object_check_required(
            self.target.is_local_storage(),
            self.config.head_each_target,
            self.config.sync_latest_tagging,
        )
    }

    fn check_target_local_storage_allow_overwrite(&self) -> bool {
        check_target_local_storage_allow_overwrite(
            self.target.is_local_storage(),
            self.config.filter_config.remove_modified_filter,
            self.config.head_each_target,
        )
    }
}

fn is_head_object_check_required(
    local_storage: bool,
    head_each_target: bool,
    sync_latest_tagging: bool,
) -> bool {
    if local_storage {
        return true;
    }

    head_each_target || sync_latest_tagging
}

fn is_object_modified(source_object: &S3syncObject, target_object: &HeadObjectOutput) -> bool {
    if source_object.size() == 0 && target_object.content_length().unwrap() == 0 {
        return false;
    }

    // GetObjectOutput doesn't have nanos
    if target_object.last_modified().unwrap().secs() < source_object.last_modified().secs() {
        return true;
    }

    let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
        source_object.last_modified().to_millis().unwrap(),
    ))
    .unwrap()
    .to_rfc3339();
    let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
        target_object.last_modified().unwrap().to_millis().unwrap(),
    ))
    .unwrap()
    .to_rfc3339();
    let key = source_object.key();
    debug!(
        name = FILTER_NAME,
        source_last_modified = source_last_modified,
        target_last_modified = target_last_modified,
        key = key,
        "object filtered."
    );

    false
}

fn check_target_local_storage_allow_overwrite(
    local_storage: bool,
    remove_modified_filter: bool,
    head_each_target: bool,
) -> bool {
    if local_storage && remove_modified_filter && !head_each_target {
        return true;
    }

    false
}

fn is_head_object_not_found_error(result: &anyhow::Error) -> bool {
    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<HeadObjectError, Response<SdkBody>>>()
    {
        if e.err().is_not_found() {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::operation::head_object;
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;
    use aws_smithy_runtime_api::http::{Response, StatusCode};

    use crate::config::args::parse_from_args;
    use crate::pipeline::storage_factory::create_storage_pair;
    use crate::storage::StoragePair;
    use crate::types::token::create_pipeline_cancellation_token;

    use super::*;

    #[test]
    fn is_object_modified_false() {
        init_dummy_tracing_subscriber();

        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .size(777)
                .last_modified(DateTime::from_secs(1))
                .build(),
        );

        let target_object = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(777))
            .last_modified(DateTime::from_secs(2))
            .build();
        assert!(!is_object_modified(&source_object, &target_object));

        let target_object = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(777))
            .last_modified(DateTime::from_secs(2))
            .build();
        assert!(!is_object_modified(&source_object, &target_object));
    }

    #[test]
    fn is_object_modified_false_size_zero() {
        init_dummy_tracing_subscriber();

        let source_object_size_zero = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .size(0)
                .last_modified(DateTime::from_secs(1))
                .build(),
        );
        let target_object = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(0))
            .last_modified(DateTime::from_secs(0))
            .build();
        assert!(!is_object_modified(
            &source_object_size_zero,
            &target_object
        ));
    }

    #[test]
    fn is_object_modified_true() {
        init_dummy_tracing_subscriber();

        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .size(777)
                .last_modified(DateTime::from_secs(1))
                .build(),
        );
        let target_object = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(777))
            .last_modified(DateTime::from_secs(1))
            .build();

        assert!(!is_object_modified(&source_object, &target_object));

        let target_object = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(777))
            .last_modified(DateTime::from_secs(0))
            .build();
        assert!(is_object_modified(&source_object, &target_object));
    }

    #[test]
    fn is_head_object_required_true() {
        init_dummy_tracing_subscriber();

        assert!(is_head_object_check_required(false, true, false));
        assert!(is_head_object_check_required(true, false, false));
        assert!(is_head_object_check_required(true, true, false));

        assert!(is_head_object_check_required(false, true, true));
        assert!(is_head_object_check_required(true, false, true));
        assert!(is_head_object_check_required(true, true, true));
        assert!(is_head_object_check_required(false, false, true));
    }

    #[test]
    fn is_head_object_required_false() {
        init_dummy_tracing_subscriber();

        assert!(!is_head_object_check_required(false, false, false));
    }

    #[test]
    fn check_target_local_storage_allow_overwrite_true() {
        init_dummy_tracing_subscriber();

        assert!(check_target_local_storage_allow_overwrite(
            true, true, false
        ));
    }

    #[test]
    fn check_target_local_storage_allow_overwrite_false() {
        init_dummy_tracing_subscriber();

        assert!(!check_target_local_storage_allow_overwrite(
            true, false, false
        ));
        assert!(!check_target_local_storage_allow_overwrite(
            false, true, false
        ));
        assert!(!check_target_local_storage_allow_overwrite(
            false, false, false
        ));

        assert!(!check_target_local_storage_allow_overwrite(
            true, true, true
        ));
    }

    #[test]
    fn is_head_object_not_found_error_test() {
        init_dummy_tracing_subscriber();

        assert!(is_head_object_not_found_error(&anyhow!(
            build_head_object_service_not_found_error()
        )));
        assert!(!is_head_object_not_found_error(&anyhow!(
            build_head_object_timeout_error()
        )));
        assert!(!is_head_object_not_found_error(&anyhow!("test error")));
    }

    #[tokio::test]
    async fn head_object_check_size() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--check-size",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } =
            create_storage_pair(config.clone(), cancellation_token.clone(), stats_sender).await;

        let head_object_checker = HeadObjectChecker::new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            1,
        );

        let source_object =
            S3syncObject::NotVersioning(Object::builder().key("6byte.dat").size(6).build());
        assert!(!head_object_checker
            .is_old_object(&source_object)
            .await
            .unwrap());

        let source_object =
            S3syncObject::NotVersioning(Object::builder().key("6byte.dat").size(5).build());
        assert!(head_object_checker
            .is_old_object(&source_object)
            .await
            .unwrap());
    }

    fn build_head_object_service_not_found_error() -> SdkError<HeadObjectError, Response<SdkBody>> {
        let not_found = aws_sdk_s3::types::error::NotFound::builder().build();
        let head_object_error = HeadObjectError::NotFound(not_found);
        let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(head_object_error, response)
    }

    fn build_head_object_timeout_error() -> SdkError<HeadObjectError, Response<SdkBody>> {
        SdkError::timeout_error("timeout_error")
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
