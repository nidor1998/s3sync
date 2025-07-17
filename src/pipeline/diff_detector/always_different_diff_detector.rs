use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;

use crate::pipeline::diff_detector::{DiffDetectionStrategy, DiffDetector};
use crate::types::S3syncObject;

pub struct AlwaysDifferentDiffDetector;
#[async_trait]
impl DiffDetectionStrategy for AlwaysDifferentDiffDetector {
    async fn is_different(
        &self,
        _source_object: &S3syncObject,
        _target_object: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }
}

impl AlwaysDifferentDiffDetector {
    pub fn boxed_new() -> DiffDetector {
        Box::new(AlwaysDifferentDiffDetector {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::head_object;
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;

    #[tokio::test]
    async fn check_different() {
        let diff_detector = AlwaysDifferentDiffDetector::boxed_new();

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
                .is_different(&source_object, &head_object_output,)
                .await
                .unwrap(),
            true
        );
    }
}
