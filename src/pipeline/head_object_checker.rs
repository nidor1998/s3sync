use anyhow::{anyhow, Result};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_smithy_http::result::SdkError;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::{debug, warn};

use crate::storage::Storage;
use crate::types::S3syncObject;
use crate::types::SyncStatistics::SyncWarning;
use crate::Config;

const FILTER_NAME: &str = "HeadObjectChecker";

pub struct HeadObjectChecker {
    worker_index: u16,
    config: Config,
    target: Storage,
}

impl HeadObjectChecker {
    pub fn new(config: Config, target: Storage, worker_index: u16) -> Self {
        Self {
            config,
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
        let key = source_object.key();
        let head_object_result = self
            .target
            .head_object(
                key,
                None,
                self.config.target_sse_c.clone(),
                self.config.target_sse_c_key.clone(),
                self.config.target_sse_c_key_md5.clone(),
            )
            .await;

        if let Ok(target_object) = head_object_result {
            return if self.config.filter_config.check_size {
                let different_size = source_object.size() != target_object.content_length();
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
            } else {
                Ok(is_object_modified(source_object, &target_object))
            };
        }

        if is_head_object_not_found_error(head_object_result.as_ref().err().unwrap()) {
            return Ok(true);
        }

        self.target
            .send_stats(SyncWarning {
                key: key.to_string(),
            })
            .await;

        let error = head_object_result.as_ref().err().unwrap().to_string();
        let source = head_object_result.as_ref().err().unwrap().source();
        warn!(
            worker_index = self.worker_index,
            error = error,
            source = source,
            "head_object() failed."
        );

        Err(anyhow!("head_object() failed. key={}.", key,))
    }

    fn is_head_object_check_required(&self) -> bool {
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
    if source_object.size() == 0 && target_object.content_length() == 0 {
        return false;
    }

    // GetObjectOutput doesn't have nanos
    if target_object.last_modified().unwrap().secs() < source_object.last_modified().secs() {
        return true;
    }

    let source_last_modified = DateTime::to_chrono_utc(source_object.last_modified())
        .unwrap()
        .to_rfc3339();
    let target_last_modified = DateTime::to_chrono_utc(target_object.last_modified().unwrap())
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
    if let Some(SdkError::ServiceError(e)) = result.downcast_ref::<SdkError<HeadObjectError>>() {
        if e.err().is_not_found() {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::operation::head_object;
    use aws_sdk_s3::types::Object;
    use aws_smithy_http::body::SdkBody;
    use aws_smithy_http::operation::Response;
    use aws_smithy_types::DateTime;

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

        let StoragePair { target, .. } =
            create_storage_pair(config.clone(), cancellation_token.clone(), stats_sender).await;

        let head_object_checker =
            HeadObjectChecker::new(config.clone(), dyn_clone::clone_box(&*(target)), 1);

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

    fn build_head_object_service_not_found_error() -> SdkError<HeadObjectError> {
        let not_found = aws_sdk_s3::types::error::NotFound::builder().build();
        let head_object_error = HeadObjectError::NotFound(not_found);
        let response = Response::new(
            http::Response::builder()
                .status(404)
                .body(SdkBody::from(r#""#))
                .unwrap(),
        );

        SdkError::service_error(head_object_error, response)
    }

    fn build_head_object_timeout_error() -> SdkError<HeadObjectError> {
        SdkError::timeout_error("timeout_error")
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
