use anyhow::Result;
use async_trait::async_trait;

use crate::types::S3syncObject;

#[async_trait]
pub trait FilterCallback {
    // This trait defines a callback for filtering objects in the source.
    // While a preprocess callback is invoked after the source object data is fetched, this callback is invoked while listing source objects.
    // So this callback is preferred to be used for filtering objects based on basic properties like key, size, etc.
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    // This function should return false if the object should be filtered out (not uploaded)
    // and true if the object should be uploaded.
    // If an error occurs, it should be handled gracefully, and the function should return an error, and the pipeline will be cancelled.
    async fn filter(&mut self, source_object: &S3syncObject) -> Result<bool>;
}
