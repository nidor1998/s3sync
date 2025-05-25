use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::debug;

use crate::pipeline::diff_detector::{DiffDetectionStrategy, DiffDetector};
use crate::types::S3syncObject;

pub struct StandardDiffDetector;

const FILTER_NAME: &str = "StandardDiffDetector";
#[async_trait]
impl DiffDetectionStrategy for StandardDiffDetector {
    async fn is_different(
        &self,
        source_object: &S3syncObject,
        target_object: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        if source_object.size() == 0 && target_object.content_length().unwrap() == 0 {
            return Ok(false);
        }

        // GetObjectOutput doesn't have nanos
        if target_object.last_modified().unwrap().secs() < source_object.last_modified().secs() {
            return Ok(true);
        }

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            source_object.last_modified().to_millis().unwrap(),
        ))?
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            target_object.last_modified().unwrap().to_millis().unwrap(),
        ))?
        .to_rfc3339();
        let key = source_object.key();
        debug!(
            name = FILTER_NAME,
            source_last_modified = source_last_modified,
            target_last_modified = target_last_modified,
            key = key,
            "object filtered."
        );

        Ok(false)
    }
}

impl StandardDiffDetector {
    pub fn boxed_new() -> DiffDetector {
        Box::new(StandardDiffDetector {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::head_object;
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;
    use tracing_subscriber::EnvFilter;

    #[tokio::test]
    async fn check_same() {
        init_dummy_tracing_subscriber();

        let diff_detector = StandardDiffDetector::boxed_new();

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
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
    async fn check_same_size_zero() {
        init_dummy_tracing_subscriber();

        let diff_detector = StandardDiffDetector::boxed_new();

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(0))
            .last_modified(DateTime::from_secs(1))
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(0)
                .last_modified(DateTime::from_secs(2))
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
    async fn check_different() {
        init_dummy_tracing_subscriber();

        let diff_detector = StandardDiffDetector::boxed_new();

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(5)
                .last_modified(DateTime::from_secs(2))
                .e_tag("e_tag")
                .build(),
        );
        assert_eq!(
            diff_detector
                .is_different(&source_object, &head_object_output)
                .await
                .unwrap(),
            true
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
