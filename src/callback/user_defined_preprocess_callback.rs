use crate::types::preprocess_callback::PreprocessError;
use crate::types::preprocess_callback::{PreprocessCallback, UploadMetadata};
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectOutput;

// Uncomment the following line if you need to use `HashMap` for user-defined metadata.
// use std::collections::HashMap;

// This struct represents a user-defined preprocessed callback.
// It can be used to implement custom preprocessing logic before uploading objects to S3.
pub struct UserDefinedPreprocessCallback {
    pub enable: bool,
}

impl UserDefinedPreprocessCallback {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // If you need to enable the callback, set `enable` to `true`
        Self { enable: false }
    }

    pub fn is_enabled(&self) -> bool {
        self.enable
    }
}

#[async_trait]
impl PreprocessCallback for UserDefinedPreprocessCallback {
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    async fn preprocess_before_upload(
        &mut self,
        _key: &str,                       // The key of the object being uploaded
        _source_object: &GetObjectOutput, // The source object being uploaded(read only)
        _metadata: &mut UploadMetadata,   // The metadata for the upload, which can be modified
    ) -> Result<()> {
        // If we want to cancel the upload, return an error with PreprocessError::Cancelled
        Err(anyhow::Error::from(PreprocessError::Cancelled))

        // The following code is an example of how to modify the user-defined metadata before uploading based on the source object.

        /*
        let content_length = _source_object.content_length.unwrap().to_string();
        if let Some(user_defined_metadata) = _metadata.metadata.as_mut() {
            user_defined_metadata.insert("mycontent-length".to_string(), content_length);
        } else {
            let mut user_defined_metadata = HashMap::new();
            user_defined_metadata.insert("mycontent-length".to_string(), content_length);
            _metadata.metadata = Some(user_defined_metadata);
        }
        */

        // Ok(())
    }
}
