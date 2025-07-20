use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::ServerSideEncryption;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

use crate::Config;
use crate::pipeline::diff_detector::{DiffDetectionStrategy, DiffDetector};
use crate::storage::Storage;
use crate::storage::e_tag_verify::{
    generate_e_tag_hash_from_path, generate_e_tag_hash_from_path_with_auto_chunksize,
    normalize_e_tag,
};
use crate::storage::local::fs_util;
use crate::types::SyncStatistics::SyncWarning;
use crate::types::{
    S3syncObject, SYNC_REPORT_ETAG_TYPE, SYNC_REPORT_RECORD_NAME, SYNC_STATUS_MATCHES,
    SYNC_STATUS_MISMATCH, SYNC_STATUS_UNKNOWN, SyncStatsReport,
};

const FILTER_NAME: &str = "ETagDiffDetector";

pub struct ETagDiffDetector {
    config: Config,
    source: Storage,
    target: Storage,
    sync_stats_report: Arc<Mutex<SyncStatsReport>>,
}

#[async_trait]
impl DiffDetectionStrategy for ETagDiffDetector {
    async fn is_different(
        &self,
        source_object: &S3syncObject,
        target_object: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        let key = source_object.key();
        if !self.source.is_local_storage() && !self.target.is_local_storage() {
            self.are_different_e_tags(key, source_object, target_object)
                .await
        } else if self.source.is_local_storage() && !self.target.is_local_storage() {
            Ok(self
                .is_source_local_e_tag_different_from_target_s3(key, target_object)
                .await?)
        } else if !self.source.is_local_storage() && self.target.is_local_storage() {
            Ok(self
                .is_target_local_e_tag_different_from_source_s3(key, source_object)
                .await?)
        } else {
            panic!("source and target are both local storage.")
        }
    }
}

impl ETagDiffDetector {
    pub fn boxed_new(
        config: Config,
        source: Storage,
        target: Storage,
        sync_stats_report: Arc<Mutex<SyncStatsReport>>,
    ) -> DiffDetector {
        Box::new(ETagDiffDetector {
            config,
            source,
            target,
            sync_stats_report,
        })
    }

    async fn are_different_e_tags(
        &self,
        key: &str,
        source_object: &S3syncObject,
        head_target_object_output: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        let source_e_tag = normalize_e_tag(&Some(source_object.e_tag().unwrap().to_string()));
        let target_e_tag = normalize_e_tag(&Some(
            head_target_object_output.e_tag.clone().unwrap().to_string(),
        ));

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            source_object.last_modified().to_millis()?,
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

        if let Some(sse) = head_target_object_output.server_side_encryption() {
            if sse != &ServerSideEncryption::Aes256 {
                self.target
                    .send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;
                self.target.set_warning();

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

                if self.config.report_sync_status {
                    info!(
                        name = SYNC_REPORT_RECORD_NAME,
                        type = SYNC_REPORT_ETAG_TYPE,
                        status = SYNC_STATUS_UNKNOWN,
                        key = key,
                        source_version_id = source_object.version_id().unwrap_or_default(),
                        target_version_id = head_target_object_output
                            .version_id
                            .as_ref()
                            .unwrap_or(&"".to_string()),
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        source_last_modified = source_last_modified,
                        target_last_modified = target_last_modified,
                        source_size = source_object.size(),
                        target_size = head_target_object_output.content_length().unwrap(),
                        "Unknown. Only ServerSideEncryption::Aes256 is supported."
                    );

                    self.sync_stats_report
                        .lock()
                        .unwrap()
                        .increment_etag_unknown();
                }

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

            if self.config.report_sync_status {
                info!(
                    name = SYNC_REPORT_RECORD_NAME,
                    type = SYNC_REPORT_ETAG_TYPE,
                    status = SYNC_STATUS_MATCHES,
                    key = key,
                    source_version_id = source_object.version_id().unwrap_or_default(),
                    target_version_id = head_target_object_output
                        .version_id
                        .as_ref()
                        .unwrap_or(&"".to_string()),
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = source_object.size(),
                    target_size = head_target_object_output.content_length().unwrap(),
                );

                self.sync_stats_report
                    .lock()
                    .unwrap()
                    .increment_etag_matches();
            }

            return Ok(false);
        } else {
            let head_source_object_output = self
                .source
                .head_object(
                    key,
                    None,
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
                    self.target.set_warning();

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

                    if self.config.report_sync_status {
                        info!(
                        name = SYNC_REPORT_RECORD_NAME,
                        type = SYNC_REPORT_ETAG_TYPE,
                            status = SYNC_STATUS_UNKNOWN,
                            key = key,
                            source_version_id = source_object.version_id().unwrap_or_default(),
                            target_version_id = head_target_object_output
                                .version_id
                                .as_ref()
                                .unwrap_or(&"".to_string()),
                            source_e_tag = source_e_tag,
                            target_e_tag = target_e_tag,
                            source_last_modified = source_last_modified,
                            target_last_modified = target_last_modified,
                            source_size = head_source_object_output.content_length().unwrap(),
                            target_size = head_target_object_output.content_length().unwrap(),
                            "Unknown. Only ServerSideEncryption::Aes256 is supported."
                        );

                        self.sync_stats_report
                            .lock()
                            .unwrap()
                            .increment_etag_unknown();
                    }

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

            if self.config.report_sync_status {
                info!(
                    name = SYNC_REPORT_RECORD_NAME,
                    type = SYNC_REPORT_ETAG_TYPE,
                    status = SYNC_STATUS_MISMATCH,
                    key = key,
                    source_version_id = source_object.version_id().unwrap_or_default(),
                    target_version_id = head_target_object_output
                        .version_id
                        .as_ref()
                        .unwrap_or(&"".to_string()),
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = source_object.size(),
                    target_size = head_target_object_output.content_length().unwrap(),
                );

                self.sync_stats_report
                    .lock()
                    .unwrap()
                    .increment_etag_mismatch();
                self.target.set_warning();
            }
        }

        Ok(true)
    }

    async fn is_source_local_e_tag_different_from_target_s3(
        &self,
        key: &str,
        head_target_object_output: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        let source_e_tag = if self.source.is_local_storage() {
            let mut local_path = self.source.get_local_path();
            local_path.push(key);

            if self.config.transfer_config.auto_chunksize {
                match self
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
                    Ok(object_parts) => {
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
                            .await?
                        }
                    }
                    _ => {
                        return Err(anyhow!("get_object_parts() failed. key={}.", key,));
                    }
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

        let head_source_object_output = self
            .source
            .head_object(
                key,
                None,
                None,
                None,
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

        if let Some(sse) = head_target_object_output.server_side_encryption() {
            if sse != &ServerSideEncryption::Aes256 {
                self.target
                    .send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;
                self.target.set_warning();

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

                if self.config.report_sync_status {
                    info!(
                        name = SYNC_REPORT_RECORD_NAME,
                        type = SYNC_REPORT_ETAG_TYPE,
                        status = SYNC_STATUS_UNKNOWN,
                        key = key,
                        source_version_id = "",
                        target_version_id = head_target_object_output
                            .version_id
                            .as_ref()
                            .unwrap_or(&"".to_string()),
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        source_last_modified = source_last_modified,
                        target_last_modified = target_last_modified,
                        source_size = head_source_object_output.content_length().unwrap(),
                        target_size = head_target_object_output.content_length().unwrap(),
                        "Unknown. Only ServerSideEncryption::Aes256 is supported."
                    );

                    self.sync_stats_report
                        .lock()
                        .unwrap()
                        .increment_etag_unknown();
                }

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

            if self.config.report_sync_status {
                info!(
                    name = SYNC_REPORT_RECORD_NAME,
                    type = SYNC_REPORT_ETAG_TYPE,
                    status = SYNC_STATUS_MATCHES,
                    key = key,
                    source_version_id = "",
                    target_version_id = head_target_object_output
                        .version_id
                        .as_ref()
                        .unwrap_or(&"".to_string()),
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = head_source_object_output.content_length().unwrap(),
                    target_size = head_target_object_output.content_length().unwrap(),
                );

                self.sync_stats_report
                    .lock()
                    .unwrap()
                    .increment_etag_matches();
            }
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

            if self.config.report_sync_status {
                info!(
                    name = SYNC_REPORT_RECORD_NAME,
                    type = SYNC_REPORT_ETAG_TYPE,
                    status = SYNC_STATUS_MISMATCH,
                    key = key,
                    source_version_id = "",
                    target_version_id = head_target_object_output
                        .version_id
                        .as_ref()
                        .unwrap_or(&"".to_string()),
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = head_source_object_output.content_length().unwrap(),
                    target_size = head_target_object_output.content_length().unwrap(),
                );

                self.sync_stats_report
                    .lock()
                    .unwrap()
                    .increment_etag_mismatch();
                self.target.set_warning();
            }
        }

        Ok(true)
    }

    async fn is_target_local_e_tag_different_from_source_s3(
        &self,
        key: &str,
        source_object: &S3syncObject,
    ) -> anyhow::Result<bool> {
        let source_e_tag = source_object.e_tag();
        let target_e_tag = if self.target.is_local_storage() {
            let local_path = fs_util::key_to_file_path(self.target.get_local_path(), key);

            if self.config.transfer_config.auto_chunksize {
                match self
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
                    Ok(object_parts) => {
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
                            .await?
                        }
                    }
                    _ => {
                        return Err(anyhow!("get_object_parts() failed. key={}.", key,));
                    }
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
                None,
                None,
                self.config.target_sse_c.clone(),
                self.config.target_sse_c_key.clone(),
                self.config.target_sse_c_key_md5.clone(),
            )
            .await?;

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            source_object.last_modified().to_millis()?,
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

            if self.config.report_sync_status {
                info!(
                    name = SYNC_REPORT_RECORD_NAME,
                    type = SYNC_REPORT_ETAG_TYPE,
                    status = SYNC_STATUS_MATCHES,
                    key = key,
                    source_version_id = source_object.version_id().unwrap_or_default(),
                    target_version_id = "",
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = source_object.size(),
                    target_size = head_target_object_output.content_length().unwrap(),
                );

                self.sync_stats_report
                    .lock()
                    .unwrap()
                    .increment_etag_matches();
            }

            return Ok(false);
        } else {
            let head_source_object_output = self
                .source
                .head_object(
                    key,
                    None,
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
                    self.target.set_warning();

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

                    if self.config.report_sync_status {
                        info!(
                            name = SYNC_REPORT_RECORD_NAME,
                            type = SYNC_REPORT_ETAG_TYPE,
                            status = SYNC_STATUS_UNKNOWN,
                            key = key,
                            source_version_id = source_object.version_id().unwrap_or_default(),
                            target_version_id = "",
                            source_e_tag = source_e_tag,
                            target_e_tag = target_e_tag,
                            source_last_modified = source_last_modified,
                            target_last_modified = target_last_modified,
                            source_size = head_source_object_output.content_length().unwrap(),
                            target_size = head_target_object_output.content_length().unwrap(),
                            "Unknown. Only ServerSideEncryption::Aes256 is supported."
                        );

                        self.sync_stats_report
                            .lock()
                            .unwrap()
                            .increment_etag_unknown();
                    }

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

            if self.config.report_sync_status {
                info!(
                    name = SYNC_REPORT_RECORD_NAME,
                    type = SYNC_REPORT_ETAG_TYPE,
                    status = SYNC_STATUS_MISMATCH,
                    key = key,
                    source_version_id = source_object.version_id().unwrap_or_default(),
                    target_version_id = head_target_object_output
                        .version_id
                        .as_ref()
                        .unwrap_or(&"".to_string()),
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    source_last_modified = source_last_modified,
                    target_last_modified = target_last_modified,
                    source_size = source_object.size(),
                    target_size = head_target_object_output.content_length().unwrap(),
                );

                self.sync_stats_report
                    .lock()
                    .unwrap()
                    .increment_etag_mismatch();
                self.target.set_warning();
            }
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
    use aws_sdk_s3::types::Object;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use tracing_subscriber::EnvFilter;

    use super::*;

    const TEST_OBJECT_ETAG: &str = "e10adc3949ba59abbe56e057f20f883e";

    #[tokio::test]
    async fn check_both_s3_e_tags_same() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "s3://dummy-source-bucket/dir1/",
            "s3://dummy-target-bucket/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ETagDiffDetector::boxed_new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .e_tag("e_tag")
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(6)
                .last_modified(DateTime::from_secs(1))
                .e_tag("e_tag")
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output)
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn check_both_s3_e_tags_same_but_size_different() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "s3://dummy-source-bucket/dir1/",
            "s3://dummy-target-bucket/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ETagDiffDetector::boxed_new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .e_tag("e_tag")
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(5)
                .last_modified(DateTime::from_secs(1))
                .e_tag("e_tag")
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output,)
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    async fn check_both_s3_not_aes256() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "s3://dummy-source-bucket/dir1/",
            "s3://dummy-target-bucket/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ETagDiffDetector::boxed_new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .e_tag("e_tag")
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(6)
                .last_modified(DateTime::from_secs(1))
                .e_tag("different_e_tag")
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output,)
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    async fn check_target_s3_e_tags_same() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "./test_data/source/dir1/",
            "s3://dummy-target-bucket/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ETagDiffDetector::boxed_new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .e_tag(TEST_OBJECT_ETAG)
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(6)
                .last_modified(DateTime::from_secs(1))
                .e_tag("e_tag")
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output,)
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn check_target_s3_e_tags_same_different_size() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "./test_data/source/dir1/",
            "s3://dummy-target-bucket/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ETagDiffDetector::boxed_new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(5))
            .last_modified(DateTime::from_secs(1))
            .e_tag(TEST_OBJECT_ETAG)
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(6)
                .last_modified(DateTime::from_secs(1))
                .e_tag("e_tag")
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output,)
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    async fn check_target_s3_not_aes256() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "./test_data/source/dir1/",
            "s3://dummy-target-bucket/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ETagDiffDetector::boxed_new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .e_tag(TEST_OBJECT_ETAG)
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(5)
                .last_modified(DateTime::from_secs(1))
                .e_tag("different_e_tag")
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output,)
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    async fn check_source_s3_e_tags_same() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "s3://dummy-source-bucket/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ETagDiffDetector::boxed_new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .e_tag(TEST_OBJECT_ETAG)
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(6)
                .last_modified(DateTime::from_secs(1))
                .e_tag(TEST_OBJECT_ETAG)
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output,)
                .await
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn check_source_s3_e_tags_same_different_size() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "s3://dummy-source-bucket/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let diff_detector = ETagDiffDetector::boxed_new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(5))
            .last_modified(DateTime::from_secs(1))
            .e_tag(TEST_OBJECT_ETAG)
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(7)
                .last_modified(DateTime::from_secs(1))
                .e_tag(TEST_OBJECT_ETAG)
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output,)
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
