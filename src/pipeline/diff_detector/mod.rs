pub mod always_different_diff_detector;
pub mod checksum_diff_detector;
pub mod etag_diff_detector;
pub mod size_diff_detector;
pub mod standard_diff_detector;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;

use crate::types::S3syncObject;

pub type DiffDetector = Box<dyn DiffDetectionStrategy + Send + Sync>;
#[async_trait]
pub trait DiffDetectionStrategy {
    async fn is_different(
        &self,
        source_object: &S3syncObject,
        target_object: &HeadObjectOutput,
    ) -> Result<bool>;
}
