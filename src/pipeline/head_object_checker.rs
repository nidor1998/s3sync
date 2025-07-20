use anyhow::{Result, anyhow};
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::types::ChecksumMode;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response;
use aws_smithy_types::body::SdkBody;
use std::sync::{Arc, Mutex};
use tracing::{error, info};

use crate::pipeline::diff_detector::always_different_diff_detector::AlwaysDifferentDiffDetector;
use crate::pipeline::diff_detector::checksum_diff_detector::ChecksumDiffDetector;
use crate::pipeline::diff_detector::etag_diff_detector::ETagDiffDetector;
use crate::pipeline::diff_detector::size_diff_detector::SizeDiffDetector;
use crate::pipeline::diff_detector::standard_diff_detector::StandardDiffDetector;

use crate::Config;
use crate::storage::Storage;
use crate::types::SyncStatistics::SyncError;
use crate::types::{
    S3syncObject, SYNC_REPORT_EXISTENCE_TYPE, SYNC_REPORT_RECORD_NAME, SYNC_STATUS_NOT_FOUND,
    SyncStatsReport,
};

pub struct HeadObjectChecker {
    worker_index: u16,
    config: Config,
    source: Storage,
    target: Storage,
    sync_stats_report: Arc<Mutex<SyncStatsReport>>,
}

impl HeadObjectChecker {
    pub fn new(
        config: Config,
        source: Storage,
        target: Storage,
        worker_index: u16,
        sync_stats_report: Arc<Mutex<SyncStatsReport>>,
    ) -> Self {
        Self {
            config,
            source,
            target,
            worker_index,
            sync_stats_report,
        }
    }

    pub(crate) async fn is_sync_required(&self, source_object: &S3syncObject) -> Result<bool> {
        if !self.config.report_sync_status {
            if !self.is_head_object_check_required() {
                return Ok(true);
            }

            if self.config.filter_config.check_checksum_algorithm.is_none()
                && self.check_target_local_storage_allow_overwrite()
            {
                return Ok(true);
            }
        }

        match &source_object {
            S3syncObject::NotVersioning(_) => self.is_old_object(source_object).await,
            S3syncObject::Versioning(_) => {
                if self.config.point_in_time.is_some() {
                    return self.is_old_object(source_object).await;
                }
                panic!("versioning object has been detected.")
            }
            _ => {
                panic!("unexpected object has been detected.")
            }
        }
    }

    async fn is_old_object(&self, source_object: &S3syncObject) -> Result<bool> {
        let checksum_mode = if self.config.filter_config.check_checksum_algorithm.is_some()
            || self
                .config
                .filter_config
                .check_mtime_and_additional_checksum
                .is_some()
        {
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
                None,
                self.config.target_sse_c.clone(),
                self.config.target_sse_c_key.clone(),
                self.config.target_sse_c_key_md5.clone(),
            )
            .await;

        if let Ok(target_object) = head_target_object_output {
            let diff_detector = if self.config.filter_config.check_size {
                SizeDiffDetector::boxed_new()
            } else if (self.config.filter_config.check_etag
                && (self.config.head_each_target || self.config.transfer_config.auto_chunksize))
                || self.config.filter_config.check_mtime_and_etag
            {
                ETagDiffDetector::boxed_new(
                    self.config.clone(),
                    dyn_clone::clone_box(self.source.as_ref()),
                    dyn_clone::clone_box(self.target.as_ref()),
                    self.sync_stats_report.clone(),
                )
            } else if self.config.filter_config.check_etag {
                // ETag has been checked by modified filter
                AlwaysDifferentDiffDetector::boxed_new()
            } else if self.config.filter_config.check_checksum_algorithm.is_some()
                || self
                    .config
                    .filter_config
                    .check_mtime_and_additional_checksum
                    .is_some()
            {
                ChecksumDiffDetector::boxed_new(
                    self.config.clone(),
                    dyn_clone::clone_box(self.source.as_ref()),
                    dyn_clone::clone_box(self.target.as_ref()),
                    self.sync_stats_report.clone(),
                )
            } else {
                StandardDiffDetector::boxed_new()
            };

            return diff_detector
                .is_different(source_object, &target_object)
                .await;
        }

        if is_head_object_not_found_error(head_target_object_output.as_ref().err().unwrap()) {
            if self.config.report_sync_status {
                info!(
                    name = SYNC_REPORT_RECORD_NAME,
                    type = SYNC_REPORT_EXISTENCE_TYPE,
                    status = SYNC_STATUS_NOT_FOUND,
                    key = key,
                    source_version_id = source_object.version_id().unwrap_or(""),
                    source_e_tag = source_object.e_tag().unwrap_or(""),
                    source_last_modified = source_object.last_modified().to_string(),
                    source_size = source_object.size(),
                );

                self.sync_stats_report.lock().unwrap().increment_not_found();
                self.target.set_warning();
            }

            return Ok(true);
        }

        self.target
            .send_stats(SyncError {
                key: key.to_string(),
            })
            .await;

        let error = head_target_object_output
            .as_ref()
            .err()
            .unwrap()
            .to_string();
        let source = head_target_object_output.as_ref().err().unwrap().source();
        error!(
            worker_index = self.worker_index,
            error = error,
            source = source,
            "head_object() failed."
        );

        Err(anyhow!("head_object() failed. key={}.", key,))
    }

    fn is_head_object_check_required(&self) -> bool {
        if (self.config.transfer_config.auto_chunksize && self.config.filter_config.check_etag)
            || self.config.filter_config.check_mtime_and_etag
        {
            return true;
        }

        if self.config.filter_config.check_checksum_algorithm.is_some()
            || self
                .config
                .filter_config
                .check_mtime_and_additional_checksum
                .is_some()
        {
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
    use crate::config::args::parse_from_args;
    use crate::pipeline::storage_factory::create_storage_pair;
    use crate::storage::StoragePair;
    use crate::types::token::create_pipeline_cancellation_token;
    use aws_sdk_s3::types::Object;
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use tracing_subscriber::EnvFilter;

    use super::*;

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

        let StoragePair { target, source } = create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let head_object_checker = HeadObjectChecker::new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            1,
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let source_object =
            S3syncObject::NotVersioning(Object::builder().key("6byte.dat").size(6).build());
        assert!(
            !head_object_checker
                .is_old_object(&source_object)
                .await
                .unwrap()
        );

        let source_object =
            S3syncObject::NotVersioning(Object::builder().key("6byte.dat").size(5).build());
        assert!(
            head_object_checker
                .is_old_object(&source_object)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn head_object_check_etag_panic() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--check-etag",
            "--auto-chunksize",
            "./test_data/source/dir1/",
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

        let head_object_checker = HeadObjectChecker::new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            1,
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let source_object =
            S3syncObject::NotVersioning(Object::builder().key("6byte.dat").size(6).build());
        head_object_checker
            .is_old_object(&source_object)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn head_object_check_checksum_panic() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--check-additional-checksum",
            "SHA256",
            "./test_data/source/dir1/",
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

        let head_object_checker = HeadObjectChecker::new(
            config.clone(),
            dyn_clone::clone_box(&*(source)),
            dyn_clone::clone_box(&*(target)),
            1,
            Arc::new(Mutex::new(SyncStatsReport::default())),
        );

        let source_object =
            S3syncObject::NotVersioning(Object::builder().key("6byte.dat").size(6).build());
        head_object_checker
            .is_old_object(&source_object)
            .await
            .unwrap();
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
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
