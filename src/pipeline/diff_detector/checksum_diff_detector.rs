use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::builders::ObjectPartBuilder;
use aws_sdk_s3::types::ChecksumMode;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::{debug, warn};

use crate::pipeline::diff_detector::{DiffDetectionStrategy, DiffDetector};
use crate::storage::additional_checksum_verify::{
    generate_checksum_from_path_for_check, generate_checksum_from_path_with_chunksize,
};
use crate::storage::local::fs_util;
use crate::storage::Storage;
use crate::types::SyncStatistics::SyncWarning;
use crate::types::{is_full_object_checksum, S3syncObject};
use crate::{types, Config};

const FILTER_NAME: &str = "ChecksumDiffDetector";
pub struct ChecksumDiffDetector {
    config: Config,
    source: Storage,
    target: Storage,
}

#[async_trait]
impl DiffDetectionStrategy for ChecksumDiffDetector {
    async fn is_different(
        &self,
        source_object: &S3syncObject,
        target_object: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        let key = source_object.key();
        if !self.source.is_local_storage() && !self.target.is_local_storage() {
            self.are_different_checksums(
                key,
                source_object.version_id().map(|v| v.to_string()),
                target_object,
            )
            .await
        } else if self.source.is_local_storage() && !self.target.is_local_storage() {
            self.is_source_local_checksum_different_from_target_s3(key, target_object)
                .await
        } else if !self.source.is_local_storage() && self.target.is_local_storage() {
            self.is_target_local_checksum_different_from_source_s3(key, target_object)
                .await
        } else {
            panic!("source and target are both local storage.")
        }
    }
}

impl ChecksumDiffDetector {
    pub fn boxed_new(config: Config, source: Storage, target: Storage) -> DiffDetector {
        Box::new(ChecksumDiffDetector {
            config,
            source,
            target,
        })
    }

    async fn are_different_checksums(
        &self,
        key: &str,
        source_version_id: Option<String>,
        head_target_object_output: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        let head_source_object_output = self
            .source
            .head_object(
                key,
                source_version_id,
                Some(ChecksumMode::Enabled),
                None,
                self.config.source_sse_c.clone(),
                self.config.source_sse_c_key.clone(),
                self.config.source_sse_c_key_md5.clone(),
            )
            .await?;

        let key = key.to_string();

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_source_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))?
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))?
        .to_rfc3339();

        let checksum_algorithm = if self.config.filter_config.check_checksum_algorithm.is_some() {
            self.config.filter_config.check_checksum_algorithm.clone()
        } else if self
            .config
            .filter_config
            .check_mtime_and_additional_checksum
            .is_some()
        {
            self.config
                .filter_config
                .check_mtime_and_additional_checksum
                .clone()
        } else {
            panic!("No checksum algorithm configured for filtering.");
        };

        let source_checksum = types::get_additional_checksum_with_head_object(
            &head_source_object_output,
            checksum_algorithm.clone(),
        );
        let target_checksum = types::get_additional_checksum_with_head_object(
            head_target_object_output,
            checksum_algorithm.clone(),
        );
        let checksum_algorithm = checksum_algorithm.unwrap().to_string();

        if source_checksum.is_none() || target_checksum.is_none() {
            warn!(
                name = FILTER_NAME,
                checksum_algorithm = checksum_algorithm,
                source_checksum = source_checksum.unwrap_or_default(),
                target_checksum = target_checksum.unwrap_or_default(),
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Checksum not found."
            );

            self.target.send_stats(SyncWarning { key }).await;
            self.target.set_warning();

            return Ok(false);
        }

        if source_checksum == target_checksum {
            debug!(
                name = FILTER_NAME,
                checksum_algorithm = checksum_algorithm,
                source_checksum = source_checksum.clone().unwrap_or_default(),
                target_checksum = target_checksum.clone().unwrap_or_default(),
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Checksums are same."
            );

            if head_source_object_output.content_length().unwrap()
                != head_target_object_output.content_length().unwrap()
            {
                warn!(
                    name = FILTER_NAME,
                    checksum_algorithm = checksum_algorithm,
                    source_checksum = source_checksum.unwrap_or_default(),
                    target_checksum = target_checksum,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = head_source_object_output.content_length().unwrap(),
                    target_size = head_target_object_output.content_length().unwrap(),
                    key = key,
                    "Checksums are same but sizes are different."
                );

                self.target.send_stats(SyncWarning { key }).await;
                self.target.set_warning();
            }

            return Ok(false);
        } else {
            debug!(
                name = FILTER_NAME,
                checksum_algorithm = checksum_algorithm,
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
    ) -> anyhow::Result<bool> {
        let mut local_path = self.source.get_local_path();
        local_path.push(key);

        let head_source_object_output = self
            .source
            .head_object(
                key,
                None,
                Some(ChecksumMode::Enabled),
                None,
                self.config.source_sse_c.clone(),
                self.config.source_sse_c_key.clone(),
                self.config.source_sse_c_key_md5.clone(),
            )
            .await?;

        let checksum_algorithm = if self.config.filter_config.check_checksum_algorithm.is_some() {
            self.config.filter_config.check_checksum_algorithm.clone()
        } else if self
            .config
            .filter_config
            .check_mtime_and_additional_checksum
            .is_some()
        {
            self.config
                .filter_config
                .check_mtime_and_additional_checksum
                .clone()
        } else {
            panic!("No checksum algorithm configured for filtering.");
        };

        let target_checksum = types::get_additional_checksum_with_head_object(
            head_target_object_output,
            checksum_algorithm.clone(),
        );

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_source_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))?
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))?
        .to_rfc3339();

        if target_checksum.is_none() {
            self.target
                .send_stats(SyncWarning {
                    key: key.to_string(),
                })
                .await;
            self.target.set_warning();

            warn!(
                name = FILTER_NAME,
                checksum_algorithm = checksum_algorithm.as_ref().unwrap().to_string(),
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

        let full_object_checksum = is_full_object_checksum(&target_checksum);
        let target_object_parts = if !full_object_checksum {
            self.target
                .get_object_parts_attributes(
                    key,
                    None,
                    self.config.max_keys,
                    self.config.target_sse_c.clone(),
                    self.config.target_sse_c_key.clone(),
                    self.config.target_sse_c_key_md5.clone(),
                )
                .await?
        } else {
            vec![]
        };

        let multipart_checksum = !target_object_parts.is_empty();
        let source_checksum = if multipart_checksum {
            generate_checksum_from_path_for_check(
                &local_path,
                checksum_algorithm.clone().unwrap(),
                multipart_checksum,
                target_object_parts
                    .iter()
                    .map(|part| part.size().unwrap())
                    .collect(),
                full_object_checksum,
            )
            .await?
        } else {
            // Use the config chunk size for calculating checksum.
            generate_checksum_from_path_with_chunksize(
                &local_path,
                checksum_algorithm.clone().unwrap(),
                self.config.transfer_config.multipart_chunksize as usize,
                self.config.transfer_config.multipart_threshold as usize,
                full_object_checksum,
            )
            .await?
        };

        if source_checksum == target_checksum.as_ref().unwrap().as_str() {
            if head_source_object_output.content_length().unwrap()
                != head_target_object_output.content_length().unwrap()
            {
                self.target
                    .send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;
                self.target.set_warning();

                warn!(
                    name = FILTER_NAME,
                    checksum_algorithm = checksum_algorithm.as_ref().unwrap().to_string(),
                    source_checksum = source_checksum,
                    target_checksum = target_checksum.clone().unwrap_or_default(),
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = head_source_object_output.content_length().unwrap(),
                    target_size = head_target_object_output.content_length().unwrap(),
                    key = key,
                    "Checksums are same but sizes are different."
                );
            }

            debug!(
                name = FILTER_NAME,
                checksum_algorithm = checksum_algorithm.as_ref().unwrap().to_string(),
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
                checksum_algorithm = checksum_algorithm.as_ref().unwrap().to_string(),
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
    ) -> anyhow::Result<bool> {
        let local_path = fs_util::key_to_file_path(self.target.get_local_path(), key);

        let key = key.to_string();

        let head_source_object_output = self
            .source
            .head_object(
                &key,
                None,
                Some(ChecksumMode::Enabled),
                None,
                self.config.source_sse_c.clone(),
                self.config.source_sse_c_key.clone(),
                self.config.source_sse_c_key_md5.clone(),
            )
            .await?;

        let checksum_algorithm = if self.config.filter_config.check_checksum_algorithm.is_some() {
            self.config.filter_config.check_checksum_algorithm.clone()
        } else if self
            .config
            .filter_config
            .check_mtime_and_additional_checksum
            .is_some()
        {
            self.config
                .filter_config
                .check_mtime_and_additional_checksum
                .clone()
        } else {
            panic!("No checksum algorithm configured for filtering.");
        };

        let source_checksum = types::get_additional_checksum_with_head_object(
            &head_source_object_output,
            checksum_algorithm.clone(),
        );

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_source_object_output
                .last_modified()
                .unwrap()
                .to_millis()?,
        ))?
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            head_target_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        ))?
        .to_rfc3339();

        if source_checksum.is_none() {
            warn!(
                name = FILTER_NAME,
                checksum_algorithm = checksum_algorithm.as_ref().unwrap().to_string(),
                source_checksum = source_checksum.unwrap_or_default(),
                target_checksum = "",
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Source checksum not found."
            );

            self.target.send_stats(SyncWarning { key }).await;
            self.target.set_warning();

            return Ok(false);
        }

        let full_object_checksum = is_full_object_checksum(&source_checksum);
        let mut source_object_parts = if !full_object_checksum {
            self.source
                .get_object_parts_attributes(
                    &key,
                    None,
                    self.config.max_keys,
                    self.config.source_sse_c.clone(),
                    self.config.source_sse_c_key.clone(),
                    self.config.source_sse_c_key_md5.clone(),
                )
                .await?
        } else {
            vec![]
        };

        let multipart_checksum = !source_object_parts.is_empty();
        let target_checksum = if multipart_checksum {
            generate_checksum_from_path_for_check(
                &local_path,
                checksum_algorithm.clone().unwrap(),
                multipart_checksum,
                source_object_parts
                    .iter()
                    .map(|part| part.size().unwrap())
                    .collect(),
                full_object_checksum,
            )
            .await?
        } else {
            // Use the config chunk size for calculating checksum.
            generate_checksum_from_path_with_chunksize(
                &local_path,
                checksum_algorithm.clone().unwrap(),
                self.config.transfer_config.multipart_chunksize as usize,
                self.config.transfer_config.multipart_threshold as usize,
                full_object_checksum,
            )
            .await?
        };

        let multipart = !source_object_parts.is_empty();
        if !multipart {
            source_object_parts.push(
                ObjectPartBuilder::default()
                    .size(head_source_object_output.content_length().unwrap())
                    .build(),
            );
        }

        if source_checksum.as_ref().unwrap().as_str() == target_checksum {
            debug!(
                name = FILTER_NAME,
                checksum_algorithm = checksum_algorithm.as_ref().unwrap().to_string(),
                source_checksum = source_checksum.clone().unwrap_or_default(),
                target_checksum = target_checksum,
                source_last_modified = source_last_modified,
                target_last_modified = target_last_modified,
                source_size = head_source_object_output.content_length().unwrap(),
                target_size = head_target_object_output.content_length().unwrap(),
                key = key,
                "object filtered. Checksums are same."
            );

            if head_source_object_output.content_length().unwrap()
                != head_target_object_output.content_length().unwrap()
            {
                warn!(
                    name = FILTER_NAME,
                    checksum_algorithm = checksum_algorithm.as_ref().unwrap().to_string(),
                    source_checksum = source_checksum.unwrap_or_default(),
                    target_checksum = target_checksum,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = head_source_object_output.content_length().unwrap(),
                    target_size = head_target_object_output.content_length().unwrap(),
                    key = key,
                    "Checksums are same but sizes are different."
                );

                self.target.send_stats(SyncWarning { key }).await;
                self.target.set_warning();
            }

            return Ok(false);
        } else {
            debug!(
                name = FILTER_NAME,
                checksum_algorithm = checksum_algorithm.as_ref().unwrap().to_string(),
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
}

#[cfg(test)]
mod tests {
    use crate::config::args::parse_from_args;
    use crate::pipeline::storage_factory::create_storage_pair;
    use crate::storage::StoragePair;
    use crate::types::token::create_pipeline_cancellation_token;
    use aws_sdk_s3::operation::head_object;
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::ChecksumAlgorithm;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tracing_subscriber::EnvFilter;

    use super::*;

    const TEST_OBJECT_CHECKSUM: &str = "CVmbZh4IWzA=";

    #[tokio::test]
    async fn is_source_local_checksum_different_from_target_s3_same() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--check-additional-checksum",
            "CRC64NVME",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        config.additional_checksum_algorithm = Some(ChecksumAlgorithm::Crc64Nvme);
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ChecksumDiffDetector {
            config: config.clone(),
            source: dyn_clone::clone_box(&*(source)),
            target: dyn_clone::clone_box(&*(target)),
        };

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .e_tag("e_tag")
            .checksum_crc64_nvme(TEST_OBJECT_CHECKSUM)
            .build();

        assert_eq!(
            diff_detector
                .is_source_local_checksum_different_from_target_s3("6byte.dat", &head_object_output)
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    async fn is_source_local_checksum_different_from_target_s3_different() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--check-additional-checksum",
            "CRC64NVME",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        config.additional_checksum_algorithm = Some(ChecksumAlgorithm::Crc64Nvme);
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ChecksumDiffDetector {
            config: config.clone(),
            source: dyn_clone::clone_box(&*(source)),
            target: dyn_clone::clone_box(&*(target)),
        };

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(5))
            .last_modified(DateTime::from_secs(1))
            .e_tag("e_tag")
            .checksum_crc64_nvme(TEST_OBJECT_CHECKSUM)
            .build();

        assert_eq!(
            diff_detector
                .is_source_local_checksum_different_from_target_s3("6byte.dat", &head_object_output)
                .await
                .unwrap(),
            false
        );
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
