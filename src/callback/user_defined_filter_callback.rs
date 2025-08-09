use anyhow::Result;
use async_trait::async_trait;

use crate::types::S3syncObject;
use crate::types::filter_callback::FilterCallback;

// This struct represents a user-defined filter callback.
// It can be used to implement custom filtering logic for objects while listing them in the source.
// While preprocess callback is invoked after the source object data is fetched, this callback is invoked while listing objects.
// So this callback is preferred to be used for filtering objects based on basic properties like key, size, etc.
pub struct UserDefinedFilterCallback {
    pub enable: bool,
}

impl UserDefinedFilterCallback {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // Todo: If you need to enable the callback, set `enable` to `true`
        Self { enable: false }
    }

    pub fn is_enabled(&self) -> bool {
        self.enable
    }
}

#[async_trait]
#[cfg(not(tarpaulin_include))]
impl FilterCallback for UserDefinedFilterCallback {
    // If you want to implement a custom filter callback, you can do so by modifying this function.
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    // This callback is invoked while listing objects in the source.
    // This function should return false if the object should be filtered out (not uploaded)
    // and true if the object should be uploaded.
    // If an error occurs, it should be handled gracefully, and the function should return an error, and the pipeline will be cancelled.
    #[cfg(not(tarpaulin_include))]
    async fn filter(&mut self, _source_object: &S3syncObject) -> Result<bool> {
        let _key: &str = _source_object.key();
        // Todo: Implement your custom filtering logic here.
        Ok(true)
    }
}
