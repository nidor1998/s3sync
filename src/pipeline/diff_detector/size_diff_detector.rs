use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use tracing::debug;

use crate::pipeline::diff_detector::{DiffDetectionStrategy, DiffDetector};
use crate::types::S3syncObject;

pub struct SizeDiffDetector;

const FILTER_NAME: &str = "SizeDiffDetector";
#[async_trait]
impl DiffDetectionStrategy for SizeDiffDetector {
    async fn is_different(
        &self,
        source_object: &S3syncObject,
        target_object: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        let different_size = source_object.size() != target_object.content_length().unwrap();
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
    }
}

impl SizeDiffDetector {
    pub fn boxed_new() -> DiffDetector {
        Box::new(SizeDiffDetector {})
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
    async fn check_same_size() {
        init_dummy_tracing_subscriber();

        let diff_detector = SizeDiffDetector::boxed_new();

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
    async fn check_different_size() {
        init_dummy_tracing_subscriber();

        let diff_detector = SizeDiffDetector::boxed_new();

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
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
