use crate::pipeline::head_object_checker::HeadObjectChecker;
use crate::pipeline::versioning_info_collector::VersioningInfoCollector;
use crate::storage::{
    convert_head_to_get_object_output, e_tag_verify, get_range_from_content_range,
    parse_range_header_string,
};
use crate::types::error::S3syncError;
use crate::types::SyncStatistics::{SyncComplete, SyncDelete, SyncError, SyncSkip, SyncWarning};
use crate::types::{
    format_metadata, format_tags, get_additional_checksum,
    get_additional_checksum_with_head_object, is_full_object_checksum, ObjectChecksum,
    S3syncObject, SseCustomerKey, SyncReportStats, METADATA_SYNC_REPORT_LOG_NAME,
    MINIMUM_CHUNKSIZE, S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY,
    S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY, SYNC_REPORT_CACHE_CONTROL_METADATA_KEY,
    SYNC_REPORT_CONTENT_DISPOSITION_METADATA_KEY, SYNC_REPORT_CONTENT_ENCODING_METADATA_KEY,
    SYNC_REPORT_CONTENT_LANGUAGE_METADATA_KEY, SYNC_REPORT_CONTENT_TYPE_METADATA_KEY,
    SYNC_REPORT_EXPIRES_METADATA_KEY, SYNC_REPORT_METADATA_TYPE, SYNC_REPORT_TAGGING_TYPE,
    SYNC_REPORT_USER_DEFINED_METADATA_KEY, SYNC_REPORT_WEBSITE_REDIRECT_METADATA_KEY,
    SYNC_STATUS_MATCHES, SYNC_STATUS_MISMATCH, TAGGING_SYNC_REPORT_LOG_NAME,
};
use anyhow::{anyhow, Context, Error, Result};
use aws_sdk_s3::operation::delete_object::{DeleteObjectError, DeleteObjectOutput};
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingError;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::get_object_attributes::GetObjectAttributesError;
use aws_sdk_s3::operation::get_object_tagging::{GetObjectTaggingError, GetObjectTaggingOutput};
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsError;
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingError;
use aws_sdk_s3::types::builders::ObjectPartBuilder;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode, ObjectPart, Tag, Tagging};
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response;
use aws_smithy_types::body::SdkBody;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, trace, warn};

use super::stage::Stage;

const INCLUDE_CONTENT_TYPE_REGEX_FILTER_NAME: &str = "include_content_type_regex_filter";
const EXCLUDE_CONTENT_TYPE_REGEX_FILTER_NAME: &str = "exclude_content_type_regex_filter";
const INCLUDE_METADATA_REGEX_FILTER_NAME: &str = "include_metadata_regex_filter";
const EXCLUDE_METADATA_REGEX_FILTER_NAME: &str = "exclude_metadata_regex_filter";

const INCLUDE_TAG_REGEX_FILTER_NAME: &str = "include_tag_regex_filter";
const EXCLUDE_TAG_REGEX_FILTER_NAME: &str = "exclude_tag_regex_filter";

pub struct ObjectSyncer {
    worker_index: u16,
    base: Stage,
    sync_report_stats: Arc<Mutex<SyncReportStats>>,
}

impl ObjectSyncer {
    pub fn new(
        base: Stage,
        worker_index: u16,
        sync_report_stats: Arc<Mutex<SyncReportStats>>,
    ) -> Self {
        Self {
            worker_index,
            base,
            sync_report_stats,
        }
    }

    pub async fn sync(&self) -> Result<()> {
        trace!(worker_index = self.worker_index, "sync worker has started.");
        self.receive_and_sync().await
    }

    async fn receive_and_sync(&self) -> Result<()> {
        loop {
            tokio::select! {
                recv_result = self.base.receiver.as_ref().unwrap().recv() => {
                    match recv_result {
                        Ok(object) => {
                            if self.sync_object_with_force_retry(object).await.is_err() {
                                self.base.cancellation_token.cancel();
                                error!(worker_index = self.worker_index, "sync worker has been cancelled with error.");
                                return Err(anyhow!("sync worker has been cancelled with error."));
                            }
                        },
                        Err(_) => {
                            // normal shutdown
                            trace!(worker_index = self.worker_index, "sync worker has been completed.");
                            break;
                        }
                    }
                },
                _ = self.base.cancellation_token.cancelled() => {
                    info!(worker_index = self.worker_index, "sync worker has been cancelled.");
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn sync_object_with_force_retry(&self, object: S3syncObject) -> Result<()> {
        let key = object.key();

        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(feature = "e2e_test_dangerous_simulations") {
            self.do_cancel_simulation("sync_object_with_force_retry");
        }

        for _ in 0..=self.base.config.force_retry_config.force_retry_count {
            let result =
                if self.base.config.enable_versioning || self.base.config.point_in_time.is_some() {
                    self.sync_object_versions(object.clone()).await
                } else {
                    self.sync_object(object.clone()).await
                };

            if self.base.cancellation_token.is_cancelled() {
                info!(
                    worker_index = self.worker_index,
                    key = key,
                    "cancellation_token has been cancelled."
                );

                return Ok(());
            }

            return if result.is_ok() {
                Ok(())
            } else {
                let e = result.unwrap_err();
                let error = e.to_string();

                if is_force_retryable_error(&e) {
                    self.base
                        .send_stats(SyncWarning {
                            key: key.to_string(),
                        })
                        .await;
                    self.base.set_warning();

                    warn!(
                        worker_index = self.worker_index,
                        key = key,
                        error = error,
                        source = e.source(),
                        "force retryable error has occurred."
                    );

                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.base
                            .config
                            .force_retry_config
                            .force_retry_interval_milliseconds,
                    ))
                    .await;

                    continue;
                }

                if is_not_found_error(&e) {
                    self.base
                        .send_stats(SyncWarning {
                            key: key.to_string(),
                        })
                        .await;
                    self.base.set_warning();
                    warn!(
                        worker_index = self.worker_index,
                        key = key,
                        error = error,
                        source = e.source(),
                        "object not found. skipping."
                    );

                    if self.base.config.warn_as_error {
                        return Err(e);
                    }

                    return Ok(());
                }

                if is_access_denied_error(&e) {
                    self.base
                        .send_stats(SyncWarning {
                            key: key.to_string(),
                        })
                        .await;
                    self.base.set_warning();
                    warn!(
                        worker_index = self.worker_index,
                        key = key,
                        error = error,
                        source = e.source(),
                        "access denied. skipping."
                    );

                    if self.base.config.warn_as_error {
                        return Err(e);
                    }

                    return Ok(());
                }

                self.base
                    .send_stats(SyncError {
                        key: key.to_string(),
                    })
                    .await;
                error!(
                    worker_index = self.worker_index,
                    key = key,
                    error = e.to_string(),
                    source = e.source(),
                    "non force retryable error has occurred."
                );

                Err(e)
            };
        }

        self.base
            .send_stats(SyncError {
                key: key.to_string(),
            })
            .await;

        error!(
            worker_index = self.worker_index,
            key = key,
            "force retry count exceeded."
        );
        Err(anyhow!("force retry count exceeded. key={}.", key,))
    }

    async fn sync_object(&self, object: S3syncObject) -> Result<()> {
        let key = object.key();

        if self.is_incompatible_object_with_local_storage(&object) {
            self.base
                .send_stats(SyncSkip {
                    key: object.key().to_string(),
                })
                .await;

            warn!(
                worker_index = self.worker_index,
                key = key,
                size = object.size(),
                "skip directory name suffix and non zero size object that is incompatible for local storage "
            );

            return Ok(());
        }

        let head_object_checker = HeadObjectChecker::new(
            self.base.config.clone(),
            dyn_clone::clone_box(&*(*self.base.source.as_ref().unwrap())),
            dyn_clone::clone_box(&*(*self.base.target.as_ref().unwrap())),
            self.worker_index,
            self.get_sync_report_stats(),
        );

        if self.base.config.report_sync_status {
            self.get_sync_report_stats()
                .lock()
                .unwrap()
                .increment_number_of_objects();
        }

        if self.base.config.report_sync_status
            && !self.base.config.report_metadata_sync_status
            && !self.base.config.report_tagging_sync_status
        {
            head_object_checker.is_sync_required(&object).await?;
            return Ok(());
        } else {
            let sync_required = head_object_checker
                .is_sync_required(&object)
                .await
                .context("pipeline::syncer::sync_object() failed.")?;
            // Even if the object is not required to sync, we need to check tagging and metadata if report_sync_status is enabled.
            if sync_required || self.base.config.report_sync_status {
                return self.sync_or_delete_object(object).await;
            }
        }

        if self.base.config.sync_latest_tagging && self.sync_tagging(key).await? {
            self.base
                .send_stats(SyncComplete {
                    key: key.to_string(),
                })
                .await;

            return Ok(());
        }

        self.base
            .send_stats(SyncSkip {
                key: key.to_string(),
            })
            .await;

        Ok(())
    }

    fn is_incompatible_object_with_local_storage(&self, object: &S3syncObject) -> bool {
        self.base.target.as_ref().unwrap().is_local_storage()
            && is_object_with_directory_name_suffix_and_none_zero_size(object)
    }

    async fn sync_object_versions(&self, object: S3syncObject) -> Result<()> {
        let versioning_info_collector = VersioningInfoCollector::new(
            self.base.config.clone(),
            dyn_clone::clone_box(&*(*self.base.target.as_ref().unwrap())),
            self.worker_index,
        );

        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(feature = "e2e_test_dangerous_simulations") {
            self.do_cancel_simulation("sync_object_versions");
        }

        let objects_to_sync = versioning_info_collector
            .collect_object_versions_to_sync(&object)
            .await?;

        for object in objects_to_sync {
            if self.base.config.enable_versioning {
                self.sync_or_delete_object(object).await?;
            } else {
                // If point-in-time is enabled, head_object_checker may be used.
                self.sync_object(object).await?;
            }
        }

        Ok(())
    }

    // skipcq: RS-R1000
    async fn sync_or_delete_object(&self, object: S3syncObject) -> Result<()> {
        let key = object.key();

        if object.is_delete_marker() {
            self.delete_object(key).await?;

            self.base
                .send_stats(SyncDelete {
                    key: key.to_string(),
                })
                .await;

            return Ok(());
        }

        let size = object.size();

        // Get the first chunk range if multipart upload is required.
        // If not, the whole object will be downloaded.
        let range = self.get_first_chunk_range(object.clone()).await?;

        debug!(
            worker_index = self.worker_index,
            key = key,
            size = size,
            range = range.as_deref(),
            "first chunk range for the object",
        );
        let get_object_output = if self.base.config.server_side_copy {
            // If server-side copy is enabled, we can use head_object to get the object attributes.
            let head_object_result = self
                .base
                .source
                .as_ref()
                .unwrap()
                .head_object(
                    key,
                    object.version_id().map(|version_id| version_id.to_string()),
                    self.base.config.additional_checksum_mode.clone(),
                    range.clone(),
                    self.base.config.source_sse_c.clone(),
                    self.base.config.source_sse_c_key.clone(),
                    self.base.config.source_sse_c_key_md5.clone(),
                )
                .await?;
            Ok(convert_head_to_get_object_output(head_object_result))
        } else {
            self.get_object(
                key,
                object.version_id().map(|version_id| version_id.to_string()),
                self.base.config.additional_checksum_mode.clone(),
                range.clone(),
                self.base.config.source_sse_c.clone(),
                self.base.config.source_sse_c_key.clone(),
                self.base.config.source_sse_c_key_md5.clone(),
            )
            .await
        };

        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(feature = "e2e_test_dangerous_simulations") {
            self.do_cancel_simulation("sync_or_delete_object");
        }

        if self.base.cancellation_token.is_cancelled() {
            info!(
                worker_index = self.worker_index,
                key = key,
                "get_object() has been cancelled."
            );

            return Ok(());
        }

        match get_object_output {
            Ok(get_object_output) => {
                if range.is_some() {
                    if get_object_output.content_range().is_none() {
                        error!("get_object() returned no content range. This is unexpected.");
                        return Err(anyhow!(
                            "get_object() returned no content range. This is unexpected. key={}.",
                            key
                        ));
                    }
                    let (request_start, request_end) =
                        parse_range_header_string(&range.clone().unwrap())
                            .context("failed to parse request range header")?;
                    let (response_start, response_end) =
                        get_range_from_content_range(&get_object_output)
                            .context("get_object() returned no content range")?;
                    if (request_start != response_start) || (request_end != response_end) {
                        return Err(anyhow!(
                            "get_object() returned unexpected content range. \
                            expected: {}-{}, actual: {}-{}",
                            request_start,
                            request_end,
                            response_start,
                            response_end,
                        ));
                    }
                }

                // Content-Type filtering
                if !self
                    .decide_sync_target_by_include_content_type_regex(
                        key,
                        get_object_output.content_type(),
                    )
                    .await
                {
                    return Ok(());
                }
                if !self
                    .decide_sync_target_by_exclude_content_type_regex(
                        key,
                        get_object_output.content_type(),
                    )
                    .await
                {
                    return Ok(());
                }

                // Metadata filtering
                if !self
                    .decide_sync_target_by_include_metadata_regex(key, get_object_output.metadata())
                    .await
                {
                    return Ok(());
                }
                if !self
                    .decide_sync_target_by_exclude_metadata_regex(key, get_object_output.metadata())
                    .await
                {
                    return Ok(());
                }

                if self.base.config.report_metadata_sync_status {
                    self.check_metadata_sync_status(key, &get_object_output)
                        .await?;
                    if !self.base.config.report_tagging_sync_status {
                        return Ok(());
                    }
                }

                let mut get_object_tagging_output = None;
                let get_object_output_tagging;
                let source_tagging;
                if self.base.config.filter_config.include_tag_regex.is_some()
                    || self.base.config.filter_config.exclude_tag_regex.is_some()
                    || self.base.config.report_tagging_sync_status
                {
                    get_object_tagging_output =
                        self.get_object_tagging(key, &get_object_output).await?;
                    if get_object_tagging_output.is_some() {
                        get_object_output_tagging = get_object_tagging_output.clone().unwrap();
                        source_tagging = Some(get_object_output_tagging.tag_set());
                    } else {
                        source_tagging = None
                    }

                    // Tagging filtering
                    if self.base.config.filter_config.include_tag_regex.is_some()
                        || self.base.config.filter_config.exclude_tag_regex.is_some()
                    {
                        if !self
                            .decide_sync_target_by_include_tag_regex(key, source_tagging)
                            .await
                        {
                            return Ok(());
                        }
                        if !self
                            .decide_sync_target_by_exclude_tag_regex(key, source_tagging)
                            .await
                        {
                            return Ok(());
                        }
                    }
                } else {
                    source_tagging = None;
                }

                if self.base.config.report_tagging_sync_status {
                    self.check_tagging_sync_status(key, object.version_id(), source_tagging)
                        .await?;

                    // If report_tagging_sync_status is enabled, we don't need to sync the object.
                    return Ok(());
                }

                let tagging = if self.base.config.disable_tagging {
                    None
                } else if self.base.config.tagging.is_some() {
                    self.base.config.tagging.clone()
                } else {
                    if get_object_tagging_output.is_none() {
                        get_object_tagging_output =
                            self.get_object_tagging(key, &get_object_output).await?;
                    }
                    if get_object_tagging_output.is_some() {
                        trace!(
                            worker_index = self.worker_index,
                            key = key,
                            "tagging = {:?}.",
                            get_object_tagging_output.as_ref().unwrap().tag_set()
                        );
                        generate_tagging_string(&get_object_tagging_output)
                    } else {
                        None
                    }
                };

                // If multipart upload is required, the first chunk does not contain the final checksum.
                // So, we need to get the final checksum from the head object.
                let final_checksum = self
                    .get_final_checksum(
                        &get_object_output,
                        range,
                        object.clone(),
                        object.checksum_algorithm(),
                    )
                    .await;

                let object_checksum = self
                    .build_object_checksum(
                        key,
                        &get_object_output,
                        object.checksum_algorithm(),
                        final_checksum.clone(),
                    )
                    .await?;

                let put_object_output = self
                    .put_object(
                        key,
                        size as u64,
                        final_checksum,
                        get_object_output,
                        tagging,
                        object_checksum,
                    )
                    .await;
                if let Err(e) = put_object_output {
                    return self.handle_put_object_error(key, e).await;
                }
            }
            Err(e) => {
                return Err(e);
            }
        }

        self.base
            .send_stats(SyncComplete {
                key: key.to_string(),
            })
            .await;

        self.base.send(object).await?;
        Ok(())
    }

    async fn sync_tagging(&self, key: &str) -> Result<bool> {
        let source_tagging = self
            .base
            .source
            .as_ref()
            .unwrap()
            .get_object_tagging(key, None)
            .await?;
        let target_tagging = self
            .base
            .target
            .as_ref()
            .unwrap()
            .get_object_tagging(key, None)
            .await?;

        let source_tagging_map = tag_set_to_map(source_tagging.tag_set());
        let target_tagging_map = tag_set_to_map(target_tagging.tag_set());

        if source_tagging_map != target_tagging_map {
            trace!(
                worker_index = self.worker_index,
                key = key,
                "new tagging = {:?}.",
                source_tagging.tag_set()
            );

            if source_tagging.tag_set().as_ref().is_empty() {
                self.base
                    .target
                    .as_ref()
                    .unwrap()
                    .delete_object_tagging(key, None)
                    .await?;
            } else {
                self.base
                    .target
                    .as_ref()
                    .unwrap()
                    .put_object_tagging(key, None, build_tagging(source_tagging.tag_set()))
                    .await?;
            }

            return Ok(true);
        }

        Ok(false)
    }

    async fn handle_put_object_error(&self, key: &str, e: Error) -> Result<()> {
        self.base
            .send_stats(SyncWarning {
                key: key.to_string(),
            })
            .await;
        self.base.set_warning();

        if is_cancelled_error(&e) {
            warn!(
                worker_index = self.worker_index,
                key = key,
                "put_object() has been cancelled."
            );

            return Ok(());
        }

        if is_directory_traversal_error(&e) {
            warn!(
                worker_index = self.worker_index,
                key = key,
                "object references a parent directory."
            );

            if self.base.config.warn_as_error {
                return Err(e);
            }

            return Ok(());
        }

        warn!(
            worker_index = self.worker_index,
            key = key,
            error = e.to_string(),
            source = e.source(),
            "put_object() failed."
        );

        Err(e)
    }

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
    ) -> Result<GetObjectOutput> {
        self.base
            .source
            .as_ref()
            .unwrap()
            .get_object(
                key,
                version_id,
                checksum_mode,
                range,
                sse_c,
                sse_c_key,
                sse_c_key_md5,
            )
            .await
            .context("pipeline::syncer::get_object() failed.")
    }

    async fn put_object(
        &self,
        key: &str,
        source_size: u64,
        source_additional_checksum: Option<String>,
        get_object_output: GetObjectOutput,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Result<PutObjectOutput> {
        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(feature = "e2e_test_dangerous_simulations") {
            self.do_cancel_simulation("put_object");
        }

        self.base
            .target
            .as_ref()
            .unwrap()
            .put_object(
                key,
                dyn_clone::clone_box(&*(*self.base.source.as_ref().unwrap())),
                source_size,
                source_additional_checksum,
                get_object_output,
                tagging,
                object_checksum,
            )
            .await
            .context("pipeline::syncer::put_object() failed.")
    }

    async fn delete_object(&self, key: &str) -> Result<DeleteObjectOutput> {
        self.base
            .target
            .as_ref()
            .unwrap()
            .delete_object(key, None)
            .await
            .context("pipeline::syncer::delete_object() failed.")
    }

    async fn get_object_tagging(
        &self,
        key: &str,
        get_object_output: &GetObjectOutput,
    ) -> Result<Option<GetObjectTaggingOutput>> {
        if get_object_output.tag_count().is_none_or(|count| count == 0) {
            return Ok(None);
        }

        let get_object_tagging_output = self
            .base
            .source
            .as_ref()
            .unwrap()
            .get_object_tagging(
                key,
                get_object_output
                    .version_id()
                    .map(|version_id| version_id.to_string()),
            )
            .await
            .context("pipeline::syncer::get_object_tagging() failed.")?;

        Ok(Some(get_object_tagging_output))
    }

    async fn get_final_checksum(
        &self,
        get_object_output: &GetObjectOutput,
        range: Option<String>,
        object: S3syncObject,
        checksum_algorithm: Option<&[ChecksumAlgorithm]>,
    ) -> Option<String> {
        let additional_checksum_algorithm = if let Some(algorithm) = checksum_algorithm {
            if algorithm.is_empty()
                || (self.base.config.additional_checksum_mode.is_none()
                    && !self.base.target.as_ref().unwrap().is_local_storage())
            {
                None
            } else {
                // Only one algorithm supported
                Some(algorithm[0].clone())
            }
        } else {
            None
        };

        // If the object is from local storage, we can get the additional checksum directly.
        if self.base.source.as_ref().unwrap().is_local_storage() {
            return get_additional_checksum(
                get_object_output,
                self.base.config.additional_checksum_algorithm.clone(),
            );
        }

        // If additional_checksum_mode is not set in remote storage, we cannot get the final checksum.
        self.base.config.additional_checksum_mode.as_ref()?;

        // If range option is not specified, the final checksum is already calculated.
        if range.is_none() {
            return get_additional_checksum(
                get_object_output,
                self.base.config.additional_checksum_algorithm.clone(),
            );
        }

        // if range option is specified, we need to get the final checksum from the head object.
        let head_object_result = self
            .base
            .source
            .as_ref()
            .unwrap()
            .head_object(
                object.key(),
                object.version_id().map(|version_id| version_id.to_string()),
                self.base.config.additional_checksum_mode.clone(),
                None,
                self.base.config.source_sse_c.clone(),
                self.base.config.source_sse_c_key.clone(),
                self.base.config.source_sse_c_key_md5.clone(),
            )
            .await
            .context("pipeline::syncer::get_final_checksum() failed.");

        if head_object_result.is_err() {
            warn!(
                    worker_index = self.worker_index,
                    key = object.key(),
                    "failed to get object parts information. checksum verification may fail. \
                    this is most likely due to the lack of HeadObject support for partNumber parameter"
                );

            self.base
                .send_stats(SyncWarning {
                    key: object.key().to_string(),
                })
                .await;
            self.base.set_warning();

            return None;
        }

        get_additional_checksum_with_head_object(
            &head_object_result.unwrap(),
            additional_checksum_algorithm,
        )
    }

    async fn get_first_chunk_range(&self, object: S3syncObject) -> Result<Option<String>> {
        // If the object size is less than the minimum chunk size, no need to get the first chunk range.
        if self.base.config.dry_run || object.size() < MINIMUM_CHUNKSIZE as i64 {
            return Ok(None);
        }

        if self.base.source.as_ref().unwrap().is_local_storage() {
            if self
                .base
                .config
                .transfer_config
                .is_multipart_upload_required(object.size() as u64)
            {
                let first_chunk_size = if object.size()
                    < self.base.config.transfer_config.multipart_chunksize as i64
                {
                    object.size() as u64
                } else {
                    self.base.config.transfer_config.multipart_chunksize
                };
                return Ok(Some(format!("bytes=0-{}", first_chunk_size - 1)));
            }
            return Ok(None);
        }

        // If auto_chunksize is enabled, we need to get the first chunk size from the head object.
        // Without auto_chunksize, we do not need to get the first chunk range, even if the object has a additional checksum,
        if self.base.config.transfer_config.auto_chunksize {
            let head_object_result = self
                .base
                .source
                .as_ref()
                .unwrap()
                .head_object_first_part(
                    object.key(),
                    object.version_id().map(|version_id| version_id.to_string()),
                    Some(ChecksumMode::Enabled),
                    self.base.config.source_sse_c.clone(),
                    self.base.config.source_sse_c_key.clone(),
                    self.base.config.source_sse_c_key_md5.clone(),
                )
                .await
                .context("pipeline::syncer::get_first_chunk_range() failed.");

            if head_object_result.is_err() {
                error!(
                    worker_index = self.worker_index,
                    key = object.key(),
                    "pipeline::syncer::get_first_chunk_range() failed."
                );

                self.base
                    .send_stats(SyncError {
                        key: object.key().to_string(),
                    })
                    .await;

                return Err(anyhow!("pipeline::syncer::get_first_chunk_range() failed."));
            }

            return Ok(Some(format!(
                "bytes=0-{}",
                head_object_result?.content_length.unwrap() - 1
            )));
        }

        let first_chunk_size =
            if object.size() < self.base.config.transfer_config.multipart_chunksize as i64 {
                object.size() as u64
            } else {
                self.base.config.transfer_config.multipart_chunksize
            };
        Ok(Some(format!("bytes=0-{}", first_chunk_size - 1)))
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_object_parts_if_necessary(
        &self,
        key: &str,
        version_id: Option<&str>,
        e_tag: Option<&str>,
        first_chunk_content_length: i64,
        checksum_algorithm: Option<&[ChecksumAlgorithm]>,
        full_object_checksum: bool,
        range: Option<&str>,
    ) -> Result<Option<Vec<ObjectPart>>> {
        if (!e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string()))
            && range.is_none())
            || self.base.config.dry_run
        {
            return Ok(None);
        }

        let key = key.to_string();

        // If auto_chunksize is disabled, we still need to get the object parts if the checksum algorithm is specified.
        if let Some(algorithm) = checksum_algorithm {
            // A full object checksum has no object parts.
            if !algorithm.is_empty() && !full_object_checksum {
                let object_parts = self
                    .base
                    .source
                    .as_ref()
                    .unwrap()
                    .get_object_parts_attributes(
                        &key,
                        version_id.map(|version_id| version_id.to_string()),
                        self.base.config.max_keys,
                        self.base.config.source_sse_c.clone(),
                        self.base.config.source_sse_c_key.clone(),
                        self.base.config.source_sse_c_key_md5.clone(),
                    )
                    .await
                    .context("pipeline::syncer::get_object_parts_if_necessary() failed.")?;

                if object_parts.is_empty()
                    && e_tag_verify::is_multipart_upload_e_tag(
                        &e_tag.map(|e_tag| e_tag.to_string()),
                    )
                {
                    error!(
                        worker_index = self.worker_index,
                        key = key,
                        "failed to get object attributes information. \
                            Please remove --auto-chunksize option and retry."
                    );

                    self.base.send_stats(SyncError { key }).await;

                    return Err(anyhow!("failed to get object attributes information."));
                }

                if self.base.config.transfer_config.auto_chunksize
                    && object_parts[0].size.unwrap() != first_chunk_content_length
                {
                    error!(
                        worker_index = self.worker_index,
                        key = key,
                        "object parts(attribute) size does not match content length. \
                        This is unexpected. Please remove --auto-chunksize option and retry."
                    );

                    self.base.send_stats(SyncError { key }).await;

                    return Err(anyhow!(
                        "object parts(attribute) size does not match content length."
                    ));
                }

                return Ok(Some(object_parts));
            }
        }

        if self.base.config.transfer_config.auto_chunksize
            && !self.base.source.as_ref().unwrap().is_local_storage()
        {
            let object_parts = self
                .base
                .source
                .as_ref()
                .unwrap()
                .get_object_parts(
                    &key,
                    version_id.map(|version_id| version_id.to_string()),
                    self.base.config.source_sse_c.clone(),
                    self.base.config.source_sse_c_key.clone(),
                    self.base.config.source_sse_c_key_md5.clone(),
                )
                .await
                .context("pipeline::syncer::get_object_parts_if_necessary() failed.")?;

            if e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string())) {
                // If the object is a multipart upload, and the object parts are empty, it should be a error.
                if object_parts.is_empty() {
                    error!(
                        worker_index = self.worker_index,
                        key = key,
                        "failed to get object parts information. \
                        this is most likely due to the lack of HeadObject support for partNumber parameter. \
                        Please remove --auto-chunksize option and retry."
                    );

                    self.base.send_stats(SyncError { key }).await;

                    return Err(anyhow!("failed to get object parts information."));
                }
            } else {
                // Even if the object is not a multipart upload, we need to return the object parts for auto-chunksize.
                let object_parts = vec![ObjectPartBuilder::default()
                    .size(first_chunk_content_length)
                    .build()];
                return Ok(Some(object_parts));
            }

            if object_parts[0].size.unwrap() != first_chunk_content_length {
                error!(
                    worker_index = self.worker_index,
                    key = key,
                    "object parts size does not match content length. \
                    This is unexpected. Please remove --auto-chunksize option and retry."
                );

                self.base.send_stats(SyncError { key }).await;

                return Err(anyhow!("object parts size does not match content length."));
            }

            Ok(Some(object_parts))
        } else {
            Ok(None)
        }
    }

    async fn build_object_checksum(
        &self,
        key: &str,
        get_object_output: &GetObjectOutput,
        checksum_algorithm: Option<&[ChecksumAlgorithm]>,
        final_checksum: Option<String>,
    ) -> Result<Option<ObjectChecksum>> {
        let additional_checksum_algorithm = if let Some(algorithm) = checksum_algorithm {
            if algorithm.is_empty()
                || (self.base.config.additional_checksum_mode.is_none()
                    && !self.base.target.as_ref().unwrap().is_local_storage())
            {
                None
            } else {
                // Only one algorithm supported
                Some(algorithm[0].clone())
            }
        } else {
            None
        };

        // If additional_checksum_mode is not set, do not build object checksum regardless of the existence of checksum.
        let checksum_algorithm = if self.base.config.additional_checksum_mode.is_none() {
            None
        } else {
            checksum_algorithm
        };

        let object_parts = self
            .get_object_parts_if_necessary(
                key,
                get_object_output.version_id(),
                get_object_output.e_tag(),
                get_object_output.content_length.unwrap(),
                checksum_algorithm,
                is_full_object_checksum(&final_checksum),
                get_object_output.content_range(),
            )
            .await?;

        Ok(Some(ObjectChecksum {
            key: key.to_string(),
            version_id: get_object_output
                .version_id()
                .map(|version_id| version_id.to_string()),
            checksum_algorithm: additional_checksum_algorithm.clone(),
            checksum_type: get_object_output.checksum_type().cloned(),
            object_parts,
            final_checksum: get_additional_checksum(
                get_object_output,
                additional_checksum_algorithm,
            ),
        }))
    }

    fn do_cancel_simulation(&self, cancellation_point: &str) {
        const CANCEL_DANGEROUS_SIMULATION_ENV: &str = "S3SYNC_CANCEL_DANGEROUS_SIMULATION";
        const CANCEL_DANGEROUS_SIMULATION_ENV_ALLOW: &str = "ALLOW";

        if std::env::var(CANCEL_DANGEROUS_SIMULATION_ENV)
            .is_ok_and(|v| v == CANCEL_DANGEROUS_SIMULATION_ENV_ALLOW)
            && self
                .base
                .config
                .cancellation_point
                .as_ref()
                .is_some_and(|point| point == cancellation_point)
        {
            error!(
                "cancel simulation has been triggered. This message should not be shown in the production.",
            );
            self.base.cancellation_token.cancel();
        }
    }

    async fn decide_sync_target_by_include_metadata_regex(
        &self,
        key: &str,
        metadata: Option<&HashMap<String, String>>,
    ) -> bool {
        if self
            .base
            .config
            .filter_config
            .include_metadata_regex
            .is_none()
        {
            return true;
        }

        if metadata.is_none() {
            debug!(
                name = INCLUDE_METADATA_REGEX_FILTER_NAME,
                worker_index = self.worker_index,
                key = key,
                "metadata = None",
            );
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;

            return false;
        }

        let formatted = format_metadata(metadata.as_ref().unwrap());
        let is_match = self
            .base
            .config
            .filter_config
            .include_metadata_regex
            .as_ref()
            .unwrap()
            .is_match(&formatted);

        debug!(
            name = INCLUDE_METADATA_REGEX_FILTER_NAME,
            worker_index = self.worker_index,
            key = key,
            metadata = formatted,
            is_match = is_match,
            "decide_sync_target_by_include_metadata_regex() called."
        );

        if !is_match {
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;
        }

        is_match
    }

    async fn decide_sync_target_by_exclude_metadata_regex(
        &self,
        key: &str,
        metadata: Option<&HashMap<String, String>>,
    ) -> bool {
        if self
            .base
            .config
            .filter_config
            .exclude_metadata_regex
            .is_none()
        {
            return true;
        }

        if metadata.is_none() {
            debug!(
                name = EXCLUDE_METADATA_REGEX_FILTER_NAME,
                worker_index = self.worker_index,
                key = key,
                "metadata = None",
            );

            return true;
        }

        let formatted = format_metadata(metadata.as_ref().unwrap());
        let is_match = self
            .base
            .config
            .filter_config
            .exclude_metadata_regex
            .as_ref()
            .unwrap()
            .is_match(&formatted);

        debug!(
            name = EXCLUDE_METADATA_REGEX_FILTER_NAME,
            worker_index = self.worker_index,
            key = key,
            metadata = formatted,
            is_match = is_match,
            "decide_sync_target_by_exclude_metadata_regex() called."
        );

        if is_match {
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;
        }

        !is_match
    }

    async fn decide_sync_target_by_include_tag_regex(
        &self,
        key: &str,
        tags: Option<&[Tag]>,
    ) -> bool {
        if self.base.config.filter_config.include_tag_regex.is_none() {
            return true;
        }

        if tags.is_none() {
            debug!(
                name = INCLUDE_TAG_REGEX_FILTER_NAME,
                worker_index = self.worker_index,
                key = key,
                "tags = None",
            );
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;

            return false;
        }

        let formatted = format_tags(tags.unwrap());
        let is_match = self
            .base
            .config
            .filter_config
            .include_tag_regex
            .as_ref()
            .unwrap()
            .is_match(&formatted);

        debug!(
            name = INCLUDE_TAG_REGEX_FILTER_NAME,
            worker_index = self.worker_index,
            key = key,
            metadata = formatted,
            is_match = is_match,
            "decide_sync_target_by_include_tag_regex() called."
        );

        if !is_match {
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;
        }

        is_match
    }

    async fn decide_sync_target_by_exclude_tag_regex(
        &self,
        key: &str,
        tags: Option<&[Tag]>,
    ) -> bool {
        if self.base.config.filter_config.exclude_tag_regex.is_none() {
            return true;
        }

        if tags.is_none() {
            debug!(
                name = EXCLUDE_TAG_REGEX_FILTER_NAME,
                worker_index = self.worker_index,
                key = key,
                "tags = None",
            );

            return true;
        }

        let formatted = format_tags(tags.unwrap());
        let is_match = self
            .base
            .config
            .filter_config
            .exclude_tag_regex
            .as_ref()
            .unwrap()
            .is_match(&formatted);

        debug!(
            name = EXCLUDE_TAG_REGEX_FILTER_NAME,
            worker_index = self.worker_index,
            key = key,
            metadata = formatted,
            is_match = is_match,
            "decide_sync_target_by_exclude_tag_regex() called."
        );

        if is_match {
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;
        }

        !is_match
    }

    async fn decide_sync_target_by_include_content_type_regex(
        &self,
        key: &str,
        content_type: Option<&str>,
    ) -> bool {
        if self
            .base
            .config
            .filter_config
            .include_content_type_regex
            .is_none()
        {
            return true;
        }

        if content_type.is_none() {
            debug!(
                name = INCLUDE_CONTENT_TYPE_REGEX_FILTER_NAME,
                worker_index = self.worker_index,
                key = key,
                "Content-Type = None",
            );
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;

            return false;
        }

        let is_match = self
            .base
            .config
            .filter_config
            .include_content_type_regex
            .as_ref()
            .unwrap()
            .is_match(content_type.unwrap());

        debug!(
            name = INCLUDE_CONTENT_TYPE_REGEX_FILTER_NAME,
            worker_index = self.worker_index,
            key = key,
            content_type = content_type.unwrap(),
            is_match = is_match,
            "decide_sync_target_by_include_content_type_regex() called."
        );

        if !is_match {
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;
        }

        is_match
    }

    async fn decide_sync_target_by_exclude_content_type_regex(
        &self,
        key: &str,
        content_type: Option<&str>,
    ) -> bool {
        if self
            .base
            .config
            .filter_config
            .exclude_content_type_regex
            .is_none()
        {
            return true;
        }

        if content_type.is_none() {
            debug!(
                name = EXCLUDE_CONTENT_TYPE_REGEX_FILTER_NAME,
                worker_index = self.worker_index,
                key = key,
                "Content-Type = None",
            );

            return true;
        }

        let is_match = self
            .base
            .config
            .filter_config
            .exclude_content_type_regex
            .as_ref()
            .unwrap()
            .is_match(content_type.unwrap());

        debug!(
            name = EXCLUDE_CONTENT_TYPE_REGEX_FILTER_NAME,
            worker_index = self.worker_index,
            key = key,
            content_type = content_type.unwrap(),
            is_match = is_match,
            "decide_sync_target_by_exclude_content_type_regex() called."
        );

        if is_match {
            self.base
                .send_stats(SyncSkip {
                    key: key.to_string(),
                })
                .await;
        }

        !is_match
    }

    async fn check_metadata_sync_status(
        &self,
        key: &str,
        source_get_object_output: &GetObjectOutput,
    ) -> Result<()> {
        let target_head_object_output = self
            .base
            .target
            .as_ref()
            .unwrap()
            .head_object(
                key,
                None,
                None,
                None,
                self.base.config.target_sse_c.clone(),
                self.base.config.target_sse_c_key.clone(),
                self.base.config.target_sse_c_key_md5.clone(),
            )
            .await;
        if let Err(e) = target_head_object_output {
            // This is a report mode, so we do not return an error if the target object is not found.
            if is_not_found_error(&e) {
                return Ok(());
            }
            return Err(e);
        }

        let target_head_object_output = target_head_object_output.unwrap();
        let mut mismatched_metadata = false;

        let source_content_disposition = source_get_object_output.content_disposition();
        let target_content_disposition = target_head_object_output.content_disposition();
        if source_content_disposition == target_content_disposition {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CONTENT_DISPOSITION_METADATA_KEY,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_content_disposition = source_content_disposition.unwrap_or_default(),
                target_content_disposition = target_content_disposition.unwrap_or_default(),
            );
        } else {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CONTENT_DISPOSITION_METADATA_KEY,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_content_disposition = source_content_disposition.unwrap_or_default(),
                target_content_disposition = target_content_disposition.unwrap_or_default(),
            );

            mismatched_metadata = true;
        }

        let source_content_encoding = source_get_object_output.content_encoding();
        let target_content_encoding = target_head_object_output.content_encoding();
        if source_content_encoding == target_content_encoding {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CONTENT_ENCODING_METADATA_KEY,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_content_encoding = source_content_encoding.unwrap_or_default(),
                target_content_encoding = target_content_encoding.unwrap_or_default(),
            );
        } else {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CONTENT_ENCODING_METADATA_KEY,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_content_encoding = source_content_encoding.unwrap_or_default(),
                target_content_encoding = target_content_encoding.unwrap_or_default(),
            );

            mismatched_metadata = true;
        }

        let source_content_language = source_get_object_output.content_language();
        let target_content_language = target_head_object_output.content_language();
        if source_content_language == target_content_language {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CONTENT_LANGUAGE_METADATA_KEY,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_content_language = source_content_language.unwrap_or_default(),
                target_content_language = target_content_language.unwrap_or_default(),
            );
        } else {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CONTENT_LANGUAGE_METADATA_KEY,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_content_language = source_content_language.unwrap_or_default(),
                target_content_language = target_content_language.unwrap_or_default(),
            );

            mismatched_metadata = true;
        }

        let source_content_type = source_get_object_output.content_type();
        let target_content_type = target_head_object_output.content_type();
        if source_content_type == target_content_type {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CONTENT_TYPE_METADATA_KEY,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_content_type = source_content_type.unwrap_or_default(),
                target_content_type = target_content_type.unwrap_or_default(),
            );
        } else {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CONTENT_TYPE_METADATA_KEY,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_content_type = source_content_type.unwrap_or_default(),
                target_content_type = target_content_type.unwrap_or_default(),
            );

            mismatched_metadata = true;
        }

        let source_cache_control = source_get_object_output.cache_control();
        let target_cache_control = target_head_object_output.cache_control();
        if source_cache_control == target_cache_control {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CACHE_CONTROL_METADATA_KEY,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_cache_control = source_cache_control.unwrap_or_default(),
                target_cache_control = target_cache_control.unwrap_or_default(),
            );
        } else {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_CACHE_CONTROL_METADATA_KEY,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_cache_control = source_cache_control.unwrap_or_default(),
                target_cache_control = target_cache_control.unwrap_or_default(),
            );

            mismatched_metadata = true;
        }

        let source_expires = source_get_object_output.expires_string();
        let target_expires = target_head_object_output.expires_string();
        if source_expires == target_expires {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_EXPIRES_METADATA_KEY,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_expires = source_expires.map(|expires| expires.to_string()).unwrap_or_default(),
                target_expires = target_expires.map(|expires| expires.to_string()).unwrap_or_default(),
            );
        } else {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_EXPIRES_METADATA_KEY,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_expires = source_expires.map(|expires| expires.to_string()).unwrap_or_default(),
                target_expires = target_expires.map(|expires| expires.to_string()).unwrap_or_default(),
            );

            mismatched_metadata = true;
        }

        let source_website_redirect = source_get_object_output.website_redirect_location();
        let target_website_redirect = target_head_object_output.website_redirect_location();
        if source_website_redirect == target_website_redirect {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_WEBSITE_REDIRECT_METADATA_KEY,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_website_redirect = source_website_redirect.unwrap_or_default(),
                target_website_redirect = target_website_redirect.unwrap_or_default(),
            );
        } else {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_WEBSITE_REDIRECT_METADATA_KEY,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_website_redirect = source_website_redirect.unwrap_or_default(),
                target_website_redirect = target_website_redirect.unwrap_or_default(),
            );

            mismatched_metadata = true;
        }

        // skipcq: RS-W1031
        let mut source_metadata = source_get_object_output
            .metadata()
            .unwrap_or(&HashMap::new())
            .clone();
        // skipcq: RS-W1031
        let mut target_metadata = target_head_object_output
            .metadata()
            .unwrap_or(&HashMap::new())
            .clone();

        // Remove s3sync origin metadata keys
        source_metadata.remove(S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY);
        source_metadata.remove(S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY);
        target_metadata.remove(S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY);
        target_metadata.remove(S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY);

        if source_metadata == target_metadata {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_USER_DEFINED_METADATA_KEY,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_metadata = format_metadata(&source_metadata),
                target_metadata = format_metadata(&target_metadata),
            );
        } else {
            info!(
                name = METADATA_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_METADATA_TYPE,
                metadata_name = SYNC_REPORT_USER_DEFINED_METADATA_KEY,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_get_object_output.version_id().unwrap_or_default(),
                target_version_id = target_head_object_output.version_id().unwrap_or_default(),
                source_metadata = format_metadata(&source_metadata),
                target_metadata = format_metadata(&target_metadata),
            );

            mismatched_metadata = true;
        }

        if !mismatched_metadata {
            self.sync_report_stats
                .lock()
                .unwrap()
                .increment_metadata_matches();
        } else {
            self.sync_report_stats
                .lock()
                .unwrap()
                .increment_metadata_mismatch();
            self.base.set_warning();
        }

        Ok(())
    }

    async fn check_tagging_sync_status(
        &self,
        key: &str,
        source_version_id: Option<&str>,
        source_tagging: Option<&[Tag]>,
    ) -> Result<()> {
        let target_get_object_tagging_output_result = self
            .base
            .target
            .as_ref()
            .unwrap()
            .get_object_tagging(key, None)
            .await;
        if let Err(e) = target_get_object_tagging_output_result {
            // This is a report mode, so we do not return an error if the target object is not found.
            if is_not_found_error(&e) {
                return Ok(());
            }
            return Err(e);
        }

        let target_tagging;

        let target_get_object_tagging_output;
        if target_get_object_tagging_output_result.is_ok() {
            // skipcq: RS-W1070
            target_get_object_tagging_output =
                target_get_object_tagging_output_result.unwrap().clone();
            target_tagging = Some(target_get_object_tagging_output.tag_set());
        } else {
            target_tagging = None
        }

        let source_tagging_string = format_tags(source_tagging.unwrap_or_default());
        let target_tagging_string = format_tags(target_tagging.unwrap_or_default());

        if source_tagging_string == target_tagging_string {
            info!(
                name = TAGGING_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_TAGGING_TYPE,
                status = SYNC_STATUS_MATCHES,
                key = key,
                source_version_id = source_version_id.unwrap_or_default(),
                target_version_id = "",
                source_tagging = source_tagging_string,
                target_tagging = target_tagging_string,
            );
            self.sync_report_stats
                .lock()
                .unwrap()
                .increment_tagging_matches();
        } else {
            info!(
                name = TAGGING_SYNC_REPORT_LOG_NAME,
                type = SYNC_REPORT_TAGGING_TYPE,
                status = SYNC_STATUS_MISMATCH,
                key = key,
                source_version_id = source_version_id.unwrap_or_default(),
                target_version_id = "",
                source_tagging = source_tagging_string,
                target_tagging = target_tagging_string,
            );
            self.sync_report_stats
                .lock()
                .unwrap()
                .increment_tagging_mismatch();
            self.base.set_warning();
        }

        Ok(())
    }
    pub fn get_sync_report_stats(&self) -> Arc<Mutex<SyncReportStats>> {
        self.sync_report_stats.clone()
    }
}

fn is_force_retryable_error(e: &Error) -> bool {
    if let Some(error) = e.downcast_ref::<SdkError<HeadObjectError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if let Some(error) = e.downcast_ref::<SdkError<GetObjectError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if let Some(error) = e.downcast_ref::<SdkError<GetObjectTaggingError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if let Some(error) = e.downcast_ref::<SdkError<GetObjectAttributesError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if let Some(error) = e.downcast_ref::<SdkError<PutObjectError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if let Some(error) = e.downcast_ref::<SdkError<PutObjectTaggingError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if let Some(error) = e.downcast_ref::<SdkError<DeleteObjectError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if let Some(error) = e.downcast_ref::<SdkError<DeleteObjectTaggingError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if let Some(error) = e.downcast_ref::<SdkError<ListObjectVersionsError, Response<SdkBody>>>() {
        return is_force_sdk_retryable_error(error);
    }

    if e.downcast_ref::<S3syncError>().is_some() {
        return true;
    }

    false
}

fn is_force_sdk_retryable_error<E, R>(e: &SdkError<E, R>) -> bool {
    !matches!(
        e,
        SdkError::ConstructionFailure(_) | SdkError::ServiceError(_)
    )
}

fn is_not_found_error(result: &Error) -> bool {
    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<GetObjectError, Response<SdkBody>>>()
    {
        if e.err().is_no_such_key() {
            return true;
        }
    }
    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<HeadObjectError, Response<SdkBody>>>()
    {
        if e.err().is_not_found() {
            return true;
        }
    }
    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<GetObjectTaggingError, Response<SdkBody>>>()
    {
        if e.raw().status().as_u16() == 404 {
            return true;
        }
    }

    false
}

fn is_access_denied_error(result: &Error) -> bool {
    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<GetObjectError, Response<SdkBody>>>()
    {
        if let Some(code) = e.err().meta().code() {
            return code == "AccessDenied";
        }
    }

    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<GetObjectTaggingError, Response<SdkBody>>>()
    {
        if let Some(code) = e.err().meta().code() {
            return code == "AccessDenied";
        }
    }

    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<PutObjectError, Response<SdkBody>>>()
    {
        if let Some(code) = e.err().meta().code() {
            return code == "AccessDenied";
        }
    }

    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<PutObjectTaggingError, Response<SdkBody>>>()
    {
        if let Some(code) = e.err().meta().code() {
            return code == "AccessDenied";
        }
    }

    false
}

fn is_directory_traversal_error(e: &Error) -> bool {
    if let Some(err) = e.downcast_ref::<S3syncError>() {
        return *err == S3syncError::DirectoryTraversalError;
    }

    false
}

fn is_cancelled_error(e: &Error) -> bool {
    if let Some(err) = e.downcast_ref::<S3syncError>() {
        return *err == S3syncError::Cancelled;
    }

    false
}

fn tag_set_to_map(tag_set: &[Tag]) -> HashMap<String, String> {
    let mut map = HashMap::<_, _>::new();
    for tag in tag_set {
        map.insert(tag.key().to_string(), tag.value().to_string());
    }

    map
}

fn generate_tagging_string(
    get_object_tagging_output: &Option<GetObjectTaggingOutput>,
) -> Option<String> {
    if get_object_tagging_output.is_none() {
        return None;
    }

    let mut tags_key_value_string = "".to_string();
    for tag in get_object_tagging_output.clone().unwrap().tag_set() {
        let tag_string = format!(
            "{}={}",
            urlencoding::encode(tag.key()),
            urlencoding::encode(tag.value()),
        );
        if !tags_key_value_string.is_empty() {
            tags_key_value_string = tags_key_value_string.add("&");
        }
        tags_key_value_string = tags_key_value_string.add(&tag_string);
    }

    Some(tags_key_value_string)
}

fn build_tagging(tag_set: &[Tag]) -> Tagging {
    let mut tagging_builder = Tagging::builder();
    for tag in tag_set {
        tagging_builder = tagging_builder.tag_set(tag.clone());
    }

    tagging_builder.build().unwrap()
}

fn is_object_with_directory_name_suffix_and_none_zero_size(object: &S3syncObject) -> bool {
    object.key().ends_with('/') && object.size() != 0
}

#[cfg(test)]
mod tests {
    use crate::config::args::parse_from_args;
    use crate::pipeline::storage_factory::create_storage_pair;
    use crate::storage::StoragePair;
    use crate::types::token::create_pipeline_cancellation_token;
    use crate::Config;
    use aws_sdk_s3::operation::head_object::HeadObjectError;
    use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsError;
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::error::NotFound;
    use aws_sdk_s3::types::Object;
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use aws_smithy_types::body::SdkBody;

    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tracing_subscriber::EnvFilter;

    use super::*;

    #[test]
    fn is_force_retry_available_test() {
        init_dummy_tracing_subscriber();

        assert!(!is_force_retryable_error(&anyhow!(
            build_head_object_service_not_found_error()
        )));
        assert!(!is_force_retryable_error(&anyhow!(
            build_head_object_construction_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_head_object_timeout_error()
        )));

        assert!(!is_force_retryable_error(&anyhow!(
            build_get_object_no_such_key_error()
        )));
        assert!(!is_force_retryable_error(&anyhow!(
            build_get_object_construction_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_get_object_timeout_error()
        )));

        assert!(!is_force_retryable_error(&anyhow!(
            build_put_object_construction_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_put_object_timeout_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_get_object_tagging_timeout_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_put_object_tagging_timeout_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_delete_object_tagging_timeout_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_get_object_attributes_timeout_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_delete_object_timeout_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_delete_object_tagging_timeout_error()
        )));
        assert!(is_force_retryable_error(&anyhow!(
            build_list_object_versions_timeout_error()
        )));

        assert!(!is_force_retryable_error(&anyhow!("error")));

        assert!(is_force_retryable_error(&anyhow!(
            S3syncError::DownloadForceRetryableError
        )));
    }

    #[test]
    fn is_directory_traversal_error_test() {
        init_dummy_tracing_subscriber();

        assert!(is_directory_traversal_error(&anyhow!(
            S3syncError::DirectoryTraversalError
        )));
        assert!(!is_directory_traversal_error(&anyhow!("Error")));
        assert!(!is_directory_traversal_error(&anyhow!(
            build_head_object_service_not_found_error()
        )));
    }

    #[test]
    fn is_cancelled_error_test() {
        init_dummy_tracing_subscriber();

        assert!(is_cancelled_error(&anyhow!(S3syncError::Cancelled)));
        assert!(!is_cancelled_error(&anyhow!(
            S3syncError::DirectoryTraversalError
        )));
        assert!(!is_directory_traversal_error(&anyhow!("Error")));
        assert!(!is_directory_traversal_error(&anyhow!(
            build_head_object_service_not_found_error()
        )));
    }

    #[tokio::test]
    async fn sync_object_skip() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--head-each-target",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, stats_receiver) = async_channel::unbounded();

        let StoragePair { source, target } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        sender
            .send(S3syncObject::NotVersioning(
                Object::builder()
                    .key("6byte.dat")
                    .size(6)
                    .last_modified(DateTime::from_secs(0))
                    .build(),
            ))
            .await
            .unwrap();
        sender.close();

        let result = ObjectSyncer::new(
            Stage::new(
                config.clone(),
                Some(dyn_clone::clone_box(&*source)),
                Some(dyn_clone::clone_box(&*target)),
                Some(receiver),
                None,
                cancellation_token.clone(),
                Arc::new(AtomicBool::new(false)),
            ),
            0,
            Arc::new(Mutex::new(SyncReportStats::default())),
        )
        .sync()
        .await;

        assert!(result.is_ok());

        let stats = stats_receiver.recv().await.unwrap();
        match stats {
            SyncSkip { .. } => {}
            _ => panic!("skip object not found"),
        }
    }

    #[tokio::test]
    async fn sync_object_not_skip() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--head-each-target",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, stats_receiver) = async_channel::unbounded();

        let StoragePair { source, target } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        let (next_sender, _next_receiver) = async_channel::bounded::<S3syncObject>(1000);

        sender
            .send(S3syncObject::NotVersioning(
                Object::builder()
                    .key("6byte.dat")
                    .size(6)
                    .last_modified(DateTime::from_secs(i32::MAX as i64))
                    .build(),
            ))
            .await
            .unwrap();
        sender.close();

        let result = ObjectSyncer::new(
            Stage::new(
                config.clone(),
                Some(dyn_clone::clone_box(&*source)),
                Some(dyn_clone::clone_box(&*target)),
                Some(receiver),
                Some(next_sender),
                cancellation_token.clone(),
                Arc::new(AtomicBool::new(false)),
            ),
            0,
            Arc::new(Mutex::new(SyncReportStats::default())),
        )
        .sync()
        .await;

        assert!(result.is_ok());

        let stats = stats_receiver.recv().await.unwrap();
        if matches!(stats, SyncSkip { .. }) {
            panic!("synced object not found")
        }
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn sync_object_head_object_error() {
        init_dummy_tracing_subscriber();

        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        assert!(
            !nix::unistd::geteuid().is_root(),
            "tests must not run as root"
        );

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--head-each-target",
            "./test_data/source/dir1/",
            "./test_data/denied_dir1/",
        ];
        let mut permissions = fs::metadata("./test_data/denied_dir1")
            .unwrap()
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions("./test_data/denied_dir1", permissions).unwrap();

        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { source, target } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        let (next_sender, _next_receiver) = async_channel::bounded::<S3syncObject>(1000);

        sender
            .send(S3syncObject::NotVersioning(
                Object::builder()
                    .key("6byte.dat")
                    .size(6)
                    .last_modified(DateTime::from_secs(i32::MAX as i64))
                    .build(),
            ))
            .await
            .unwrap();
        sender.close();

        let result = ObjectSyncer::new(
            Stage::new(
                config.clone(),
                Some(dyn_clone::clone_box(&*source)),
                Some(dyn_clone::clone_box(&*target)),
                Some(receiver),
                Some(next_sender),
                cancellation_token.clone(),
                Arc::new(AtomicBool::new(false)),
            ),
            0,
            Arc::new(Mutex::new(SyncReportStats::default())),
        )
        .sync()
        .await;

        assert!(result.is_err());

        permissions = fs::metadata("./test_data/denied_dir1")
            .unwrap()
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions("./test_data/denied_dir1", permissions).unwrap();
    }

    #[tokio::test]
    async fn sync_object_with_put_object_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--aws-max-attempts",
            "1",
            "--target-endpoint-url",
            "https://invalid-s3-endpoint-url.6329313.local:65535",
            "--force-retry-count",
            "1",
            "--force-retry-interval-milliseconds",
            "1",
            "./test_data/source/dir1/",
            "s3://invalid_bucket?name!",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { source, target } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        sender
            .send(S3syncObject::NotVersioning(
                Object::builder()
                    .set_key(Some("6byte.dat".to_string()))
                    .size(1)
                    .build(),
            ))
            .await
            .unwrap();
        sender.close();

        let result = ObjectSyncer::new(
            Stage::new(
                config.clone(),
                Some(dyn_clone::clone_box(&*source)),
                Some(dyn_clone::clone_box(&*target)),
                Some(receiver),
                None,
                cancellation_token.clone(),
                Arc::new(AtomicBool::new(false)),
            ),
            0,
            Arc::new(Mutex::new(SyncReportStats::default())),
        )
        .sync()
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sync_object_with_get_object_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--aws-max-attempts",
            "1",
            "--source-endpoint-url",
            "https://invalid-s3-endpoint-url.6329313.local:65535",
            "--force-retry-count",
            "1",
            "--force-retry-interval-milliseconds",
            "1",
            "s3://invalid_bucket?name!",
            "./test_data/empty",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { source, target } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        sender
            .send(S3syncObject::NotVersioning(
                Object::builder()
                    .set_key(Some("data4".to_string()))
                    .size(1)
                    .build(),
            ))
            .await
            .unwrap();
        sender.close();

        let result = ObjectSyncer::new(
            Stage::new(
                config.clone(),
                Some(dyn_clone::clone_box(&*source)),
                Some(dyn_clone::clone_box(&*target)),
                Some(receiver),
                None,
                cancellation_token.clone(),
                Arc::new(AtomicBool::new(false)),
            ),
            0,
            Arc::new(Mutex::new(SyncReportStats::default())),
        )
        .sync()
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sync_object_cancelled() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--aws-max-attempts",
            "1",
            "--target-endpoint-url",
            "https://invalid-s3-endpoint-url.6329313.local:65535",
            "--force-retry-count",
            "1",
            "--force-retry-interval-milliseconds",
            "1",
            "./test_data/source/dir1",
            "s3://invalid_bucket?name!",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { source, target } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let (_, receiver) = async_channel::bounded::<S3syncObject>(1000);

        cancellation_token.cancel();

        let result = ObjectSyncer::new(
            Stage::new(
                config.clone(),
                Some(dyn_clone::clone_box(&*source)),
                Some(dyn_clone::clone_box(&*target)),
                Some(receiver),
                None,
                cancellation_token.clone(),
                Arc::new(AtomicBool::new(false)),
            ),
            0,
            Arc::new(Mutex::new(SyncReportStats::default())),
        )
        .sync()
        .await;

        assert!(result.is_ok());
    }

    #[test]
    fn is_not_found_error_test() {
        init_dummy_tracing_subscriber();

        assert!(is_not_found_error(&anyhow!(
            build_get_object_no_such_key_error()
        )));

        assert!(is_not_found_error(&anyhow!(
            build_head_object_not_found_error()
        )));

        assert!(is_not_found_error(&anyhow!(
            build_get_object_tagging_not_found_error()
        )));

        assert!(!is_not_found_error(&anyhow!(
            build_get_object_timeout_error()
        )));
        assert!(!is_not_found_error(&anyhow!("test error")));
    }

    #[test]
    fn is_access_denied_error_test() {
        init_dummy_tracing_subscriber();

        assert!(is_access_denied_error(&anyhow!(
            build_get_object_access_denied_error()
        )));

        assert!(is_access_denied_error(&anyhow!(
            build_get_object_tagging_access_denied_error()
        )));

        assert!(is_access_denied_error(&anyhow!(
            build_put_object_access_denied_error()
        )));

        assert!(is_access_denied_error(&anyhow!(
            build_put_object_tagging_access_denied_error()
        )));

        assert!(!is_access_denied_error(&anyhow!(
            build_get_object_no_such_key_error()
        )));

        assert!(!is_access_denied_error(&anyhow!(
            build_get_object_timeout_error()
        )));
        assert!(!is_access_denied_error(&anyhow!("test error")));
    }

    #[test]
    fn build_tagging_test() {
        init_dummy_tracing_subscriber();

        let tags = [
            Tag::builder()
                .key("key1".to_string())
                .value("value1".to_string())
                .build()
                .unwrap(),
            Tag::builder()
                .key("key2".to_string())
                .value("value2".to_string())
                .build()
                .unwrap(),
        ];

        let tagging = build_tagging(&tags);
        assert_eq!(tagging.tag_set().len(), 2);
    }

    #[test]
    fn tag_set_to_map_test() {
        init_dummy_tracing_subscriber();

        let tags = [
            Tag::builder()
                .key("key1".to_string())
                .value("value1".to_string())
                .build()
                .unwrap(),
            Tag::builder()
                .key("key2".to_string())
                .value("value2".to_string())
                .build()
                .unwrap(),
        ];

        let tag_map = tag_set_to_map(&tags);
        assert_eq!(tag_map.len(), 2);
    }

    #[test]
    fn generate_tagging_string_empty() {
        init_dummy_tracing_subscriber();

        let get_object_tagging_output = None;
        assert!(generate_tagging_string(&get_object_tagging_output).is_none());
    }

    #[test]
    fn generate_tagging_string_ok() {
        init_dummy_tracing_subscriber();

        let get_object_tagging_output = Some(
            GetObjectTaggingOutput::builder()
                .tag_set(
                    Tag::builder()
                        .key("somekey1".to_string())
                        .value("somevalue1".to_string())
                        .build()
                        .unwrap(),
                )
                .tag_set(
                    Tag::builder()
                        .key("test!_key".to_string())
                        .value("あいうえお".to_string())
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        );
        let expected_value =
            "somekey1=somevalue1&test%21_key=%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A";
        assert_eq!(
            generate_tagging_string(&get_object_tagging_output).unwrap(),
            expected_value
        );
    }
    #[test]
    fn is_object_with_directory_name_suffix_and_size_zero_size_test() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test/")
                .size(1)
                .last_modified(DateTime::from_secs(0))
                .build(),
        );
        assert!(is_object_with_directory_name_suffix_and_none_zero_size(
            &object
        ));

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test/")
                .size(0)
                .last_modified(DateTime::from_secs(0))
                .build(),
        );
        assert!(!is_object_with_directory_name_suffix_and_none_zero_size(
            &object
        ));

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .size(0)
                .last_modified(DateTime::from_secs(0))
                .build(),
        );
        assert!(!is_object_with_directory_name_suffix_and_none_zero_size(
            &object
        ));

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .size(1)
                .last_modified(DateTime::from_secs(0))
                .build(),
        );
        assert!(!is_object_with_directory_name_suffix_and_none_zero_size(
            &object
        ));
    }

    fn build_head_object_service_not_found_error() -> SdkError<HeadObjectError, Response<SdkBody>> {
        let head_object_error =
            HeadObjectError::NotFound(aws_sdk_s3::types::error::NotFound::builder().build());
        let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(head_object_error, response)
    }

    fn build_get_object_tagging_not_found_error(
    ) -> SdkError<GetObjectTaggingError, Response<SdkBody>> {
        let unhandled_error = GetObjectTaggingError::unhandled("Not Found");

        let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(unhandled_error, response)
    }

    fn build_head_object_construction_error() -> SdkError<HeadObjectError, Response<SdkBody>> {
        SdkError::construction_failure("construction_failure")
    }

    fn build_head_object_timeout_error() -> SdkError<HeadObjectError, Response<SdkBody>> {
        SdkError::timeout_error("timeout_error")
    }

    fn build_get_object_no_such_key_error() -> SdkError<GetObjectError, Response<SdkBody>> {
        let get_object_error = GetObjectError::NoSuchKey(
            aws_sdk_s3::types::error::builders::NoSuchKeyBuilder::default().build(),
        );
        let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(get_object_error, response)
    }

    fn build_head_object_not_found_error() -> SdkError<HeadObjectError, Response<SdkBody>> {
        let not_found_error = NotFound::builder().build();
        let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(HeadObjectError::NotFound(not_found_error), response)
    }

    fn build_get_object_access_denied_error() -> SdkError<GetObjectError, Response<SdkBody>> {
        let unhandled_error = GetObjectError::generic(
            aws_sdk_s3::error::ErrorMetadata::builder()
                .code("AccessDenied")
                .build(),
        );

        let response = Response::new(StatusCode::try_from(403).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(unhandled_error, response)
    }

    fn build_get_object_tagging_access_denied_error(
    ) -> SdkError<GetObjectTaggingError, Response<SdkBody>> {
        let unhandled_error = GetObjectTaggingError::generic(
            aws_sdk_s3::error::ErrorMetadata::builder()
                .code("AccessDenied")
                .build(),
        );

        let response = Response::new(StatusCode::try_from(403).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(unhandled_error, response)
    }

    fn build_put_object_access_denied_error() -> SdkError<PutObjectError, Response<SdkBody>> {
        let unhandled_error = PutObjectError::generic(
            aws_sdk_s3::error::ErrorMetadata::builder()
                .code("AccessDenied")
                .build(),
        );

        let response = Response::new(StatusCode::try_from(403).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(unhandled_error, response)
    }

    fn build_put_object_tagging_access_denied_error(
    ) -> SdkError<PutObjectTaggingError, Response<SdkBody>> {
        let unhandled_error = PutObjectTaggingError::generic(
            aws_sdk_s3::error::ErrorMetadata::builder()
                .code("AccessDenied")
                .build(),
        );

        let response = Response::new(StatusCode::try_from(403).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(unhandled_error, response)
    }

    fn build_get_object_construction_error() -> SdkError<GetObjectError, Response<SdkBody>> {
        SdkError::construction_failure("construction_failure")
    }

    fn build_get_object_timeout_error() -> SdkError<GetObjectError, Response<SdkBody>> {
        SdkError::timeout_error("timeout_error")
    }

    fn build_put_object_construction_error() -> SdkError<PutObjectError, Response<SdkBody>> {
        SdkError::construction_failure("construction_failure")
    }

    fn build_put_object_timeout_error() -> SdkError<PutObjectError, Response<SdkBody>> {
        SdkError::timeout_error("timeout_error")
    }

    fn build_get_object_tagging_timeout_error() -> SdkError<GetObjectTaggingError, Response<SdkBody>>
    {
        SdkError::timeout_error("timeout_error")
    }

    fn build_put_object_tagging_timeout_error() -> SdkError<PutObjectTaggingError, Response<SdkBody>>
    {
        SdkError::timeout_error("timeout_error")
    }

    fn build_delete_object_tagging_timeout_error(
    ) -> SdkError<DeleteObjectTaggingError, Response<SdkBody>> {
        SdkError::timeout_error("timeout_error")
    }

    fn build_get_object_attributes_timeout_error(
    ) -> SdkError<GetObjectAttributesError, Response<SdkBody>> {
        SdkError::timeout_error("timeout_error")
    }

    fn build_delete_object_timeout_error() -> SdkError<DeleteObjectError, Response<SdkBody>> {
        SdkError::timeout_error("timeout_error")
    }

    fn build_list_object_versions_timeout_error(
    ) -> SdkError<ListObjectVersionsError, Response<SdkBody>> {
        SdkError::timeout_error("timeout_error")
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
