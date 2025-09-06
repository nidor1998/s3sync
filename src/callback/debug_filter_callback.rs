use anyhow::Result;
use async_trait::async_trait;

use crate::types::S3syncObject;
use crate::types::filter_callback::FilterCallback;
// This struct represents a user-defined filter callback.
// It can be used to implement custom filtering logic for objects while listing them in the source.
// While preprocess callback is invoked after the source object data is fetched, this callback is invoked while listing objects.
// So this callback is preferred to be used for filtering objects based on basic properties like key, size, etc.
pub struct DebugFilterCallback;
#[async_trait]
#[cfg_attr(coverage, coverage(off))]
impl FilterCallback for DebugFilterCallback {
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    // This callback is invoked while listing objects in the source.
    // This function should return false if the object should be filtered out (not uploaded)
    // and true if the object should be uploaded.
    // If an error occurs, it should be handled gracefully, and the function should return an error, and the pipeline will be cancelled.
    #[cfg_attr(coverage, coverage(off))]
    async fn filter(&mut self, source_object: &S3syncObject) -> Result<bool> {
        Ok(!source_object.key().starts_with("dir21/"))
    }
}
