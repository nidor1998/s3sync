use std::collections::HashMap;
use std::ops::Add;

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
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode, ObjectPart, Tag, Tagging};
use aws_smithy_http::body::SdkBody;
use aws_smithy_http::result::SdkError;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use http::{Response, StatusCode};
use tracing::{error, info, trace, warn};

use crate::pipeline::head_object_checker::HeadObjectChecker;
use crate::pipeline::versioning_info_collector::VersioningInfoCollector;
use crate::storage::e_tag_verify;
use crate::types;
use crate::types::error::S3syncError;
use crate::types::SyncStatistics::{SyncComplete, SyncDelete, SyncError, SyncSkip, SyncWarning};
use crate::types::{ObjectChecksum, S3syncObject, SseCustomerKey};

use super::stage::Stage;

pub struct ObjectSyncer {
    worker_index: u16,
    base: Stage,
}

impl ObjectSyncer {
    pub fn new(base: Stage, worker_index: u16) -> Self {
        Self { worker_index, base }
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

        for _ in 0..=self.base.config.force_retry_config.force_retry_count {
            let result = if self.base.config.enable_versioning {
                self.sync_object_versions(object.clone()).await
            } else {
                self.sync_object(object.clone()).await
            };

            return if result.is_ok() {
                Ok(())
            } else {
                if self.base.cancellation_token.is_cancelled() {
                    info!(
                        worker_index = self.worker_index,
                        key = key,
                        "cancellation_token has been cancelled."
                    );

                    return Ok(());
                }

                let e = result.unwrap_err();
                let error = e.to_string();

                if is_force_retryable_error(&e) {
                    self.base
                        .send_stats(SyncWarning {
                            key: key.to_string(),
                        })
                        .await;
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
                "skip directory name suffix and non zero size object for local storage "
            );

            return Ok(());
        }

        let head_object_checker = HeadObjectChecker::new(
            self.base.config.clone(),
            dyn_clone::clone_box(&*(*self.base.target.as_ref().unwrap())),
            self.worker_index,
        );

        if head_object_checker.is_sync_required(&object).await? {
            return self.sync_or_delete_object(object).await;
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

        let objects_to_sync = versioning_info_collector
            .collect_object_versions_to_sync(&object)
            .await?;

        for object in objects_to_sync {
            self.sync_or_delete_object(object).await?;
        }

        Ok(())
    }

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

        let get_object_output = self
            .get_object(
                key,
                object.version_id().map(|version_id| version_id.to_string()),
                self.base.config.additional_checksum_mode.clone(),
                self.base.config.source_sse_c.clone(),
                self.base.config.source_sse_c_key.clone(),
                self.base.config.source_sse_c_key_md5.clone(),
            )
            .await;

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
                let tagging = if self.base.config.disable_tagging {
                    None
                } else if self.base.config.tagging.is_some() {
                    self.base.config.tagging.clone()
                } else {
                    let get_object_tagging_output =
                        self.get_object_tagging(key, &get_object_output).await?;
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

                let object_checksum = self
                    .build_object_checksum(key, &get_object_output, object.checksum_algorithm())
                    .await?;

                let put_object_output = self
                    .put_object(key, get_object_output, tagging, object_checksum)
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

        self.base.send(object).await
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

        let source_tagging_map = tag_set_to_map(source_tagging.tag_set().unwrap());
        let target_tagging_map = tag_set_to_map(target_tagging.tag_set().unwrap());

        if source_tagging_map != target_tagging_map {
            trace!(
                worker_index = self.worker_index,
                key = key,
                "new tagging = {:?}.",
                source_tagging.tag_set().as_ref().unwrap()
            );

            if source_tagging.tag_set().as_ref().unwrap().is_empty() {
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
                    .put_object_tagging(key, None, build_tagging(source_tagging.tag_set().unwrap()))
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

    async fn get_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
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
        get_object_output: GetObjectOutput,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Result<PutObjectOutput> {
        self.base
            .target
            .as_ref()
            .unwrap()
            .put_object(key, get_object_output, tagging, object_checksum)
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
        if get_object_output.tag_count() == 0 {
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

    async fn get_object_parts_if_necessary(
        &self,
        key: &str,
        version_id: Option<&str>,
        e_tag: Option<&str>,
        checksum_algorithm: Option<&[ChecksumAlgorithm]>,
    ) -> Result<Option<Vec<ObjectPart>>> {
        if !e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string()))
            || self.base.config.dry_run
        {
            return Ok(None);
        }

        if let Some(algorithm) = checksum_algorithm {
            if !algorithm.is_empty() {
                return Ok(Some(
                    self.base
                        .source
                        .as_ref()
                        .unwrap()
                        .get_object_parts_attributes(
                            key,
                            version_id.map(|version_id| version_id.to_string()),
                            self.base.config.max_keys,
                            self.base.config.source_sse_c.clone(),
                            self.base.config.source_sse_c_key.clone(),
                            self.base.config.source_sse_c_key_md5.clone(),
                        )
                        .await
                        .context("pipeline::syncer::get_object_parts_if_necessary() failed.")?,
                ));
            }
        }

        if self.base.config.transfer_config.auto_chunksize {
            let object_parts = self
                .base
                .source
                .as_ref()
                .unwrap()
                .get_object_parts(
                    key,
                    version_id.map(|version_id| version_id.to_string()),
                    self.base.config.source_sse_c.clone(),
                    self.base.config.source_sse_c_key.clone(),
                    self.base.config.source_sse_c_key_md5.clone(),
                )
                .await
                .context("pipeline::syncer::get_object_parts_if_necessary() failed.")?;

            if object_parts.is_empty() {
                self.base
                    .send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;

                warn!(
                    worker_index = self.worker_index,
                    key = key,
                    "failed to get object parts information. e-tag verification may fail. \
                    this is most likely due to the lack of HeadObject support for partNumber parameter"
                );

                return Ok(None);
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
    ) -> Result<Option<ObjectChecksum>> {
        let object_parts = self
            .get_object_parts_if_necessary(
                key,
                get_object_output.version_id(),
                get_object_output.e_tag(),
                checksum_algorithm,
            )
            .await?;

        let algorithm = if let Some(algorithm) = checksum_algorithm {
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

        Ok(Some(ObjectChecksum {
            key: key.to_string(),
            version_id: get_object_output
                .version_id()
                .map(|version_id| version_id.to_string()),
            checksum_algorithm: algorithm.clone(),
            object_parts,
            final_checksum: types::get_additional_checksum(get_object_output, algorithm),
        }))
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
        result.downcast_ref::<SdkError<GetObjectTaggingError, Response<SdkBody>>>()
    {
        if e.raw().http().status() == StatusCode::NOT_FOUND {
            return true;
        }
    }

    false
}

fn is_access_denied_error(result: &Error) -> bool {
    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<GetObjectError, Response<SdkBody>>>()
    {
        if let Some(code) = e.err().code() {
            return code == "AccessDenied";
        }
    }

    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<GetObjectTaggingError, Response<SdkBody>>>()
    {
        if let Some(code) = e.err().code() {
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
        map.insert(
            tag.key().as_ref().unwrap().to_string(),
            tag.value().as_ref().unwrap().to_string(),
        );
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
    for tag in get_object_tagging_output
        .clone()
        .unwrap()
        .tag_set()
        .unwrap()
    {
        let tag_string = format!(
            "{}={}",
            urlencoding::encode(tag.key().unwrap()),
            urlencoding::encode(tag.value().unwrap()),
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

    tagging_builder.build()
}

fn is_object_with_directory_name_suffix_and_none_zero_size(object: &S3syncObject) -> bool {
    object.key().ends_with('/') && object.size() != 0
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsError;
    use aws_sdk_s3::types::Object;
    use aws_smithy_http::body::SdkBody;
    use aws_smithy_types::DateTime;
    use http::Response;

    use crate::config::args::parse_from_args;
    use crate::pipeline::storage_factory::create_storage_pair;
    use crate::storage::StoragePair;
    use crate::types::token::create_pipeline_cancellation_token;
    use crate::Config;

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

        let StoragePair { source, target } =
            create_storage_pair(config.clone(), cancellation_token.clone(), stats_sender).await;
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
            ),
            0,
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

        let StoragePair { source, target } =
            create_storage_pair(config.clone(), cancellation_token.clone(), stats_sender).await;
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
            ),
            0,
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

        if nix::unistd::Uid::effective().is_root() {
            panic!("run tests from root");
        }

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--head-each-target",
            "./test_data/source/dir1/",
            "./test_data/denied_dir/",
        ];
        let mut permissions = fs::metadata("./test_data/denied_dir")
            .unwrap()
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions("./test_data/denied_dir", permissions).unwrap();

        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, _) = async_channel::unbounded();

        let StoragePair { source, target } =
            create_storage_pair(config.clone(), cancellation_token.clone(), stats_sender).await;
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
            ),
            0,
        )
        .sync()
        .await;

        assert!(result.is_err());
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

        let StoragePair { source, target } =
            create_storage_pair(config.clone(), cancellation_token.clone(), stats_sender).await;
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        sender
            .send(S3syncObject::NotVersioning(
                Object::builder()
                    .set_key(Some("6byte.dat".to_string()))
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
            ),
            0,
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

        let StoragePair { source, target } =
            create_storage_pair(config.clone(), cancellation_token.clone(), stats_sender).await;
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        sender
            .send(S3syncObject::NotVersioning(
                Object::builder().set_key(Some("data4".to_string())).build(),
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
            ),
            0,
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

        let StoragePair { source, target } =
            create_storage_pair(config.clone(), cancellation_token.clone(), stats_sender).await;
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
            ),
            0,
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
                .build(),
            Tag::builder()
                .key("key2".to_string())
                .value("value2".to_string())
                .build(),
        ];

        let tagging = build_tagging(&tags);
        assert_eq!(tagging.tag_set().unwrap().len(), 2);
    }

    #[test]
    fn tag_set_to_map_test() {
        init_dummy_tracing_subscriber();

        let tags = [
            Tag::builder()
                .key("key1".to_string())
                .value("value1".to_string())
                .build(),
            Tag::builder()
                .key("key2".to_string())
                .value("value2".to_string())
                .build(),
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
                        .build(),
                )
                .tag_set(
                    Tag::builder()
                        .key("test!_key".to_string())
                        .value("あいうえお".to_string())
                        .build(),
                )
                .build(),
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
        let response = http::Response::builder()
            .status(404)
            .body(SdkBody::from(r#""#))
            .unwrap();

        SdkError::service_error(head_object_error, response)
    }

    fn build_get_object_tagging_not_found_error(
    ) -> SdkError<GetObjectTaggingError, Response<SdkBody>> {
        let unhandled_error =
            GetObjectTaggingError::create_unhandled_error(anyhow!("Not found").into(), None);

        let response = http::Response::builder()
            .status(404)
            .body(SdkBody::from(r#""#))
            .unwrap();

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
        let response = http::Response::builder()
            .status(404)
            .body(SdkBody::from(r#""#))
            .unwrap();

        SdkError::service_error(get_object_error, response)
    }

    fn build_get_object_access_denied_error() -> SdkError<GetObjectError, Response<SdkBody>> {
        let unhandled_error = GetObjectError::create_unhandled_error(
            anyhow!("Access Denied").into(),
            Some(
                aws_smithy_types::error::ErrorMetadata::builder()
                    .code("AccessDenied")
                    .build(),
            ),
        );

        let response = http::Response::builder()
            .status(403)
            .body(SdkBody::from(r#""#))
            .unwrap();

        SdkError::service_error(unhandled_error, response)
    }

    fn build_get_object_tagging_access_denied_error(
    ) -> SdkError<GetObjectTaggingError, Response<SdkBody>> {
        let unhandled_error = GetObjectTaggingError::create_unhandled_error(
            anyhow!("Access Denied").into(),
            Some(
                aws_smithy_types::error::ErrorMetadata::builder()
                    .code("AccessDenied")
                    .build(),
            ),
        );

        let response = http::Response::builder()
            .status(403)
            .body(SdkBody::from(r#""#))
            .unwrap();

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
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
