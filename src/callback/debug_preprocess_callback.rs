use crate::types::preprocess_callback::{PreprocessCallback, PreprocessError, UploadMetadata};
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use std::collections::HashMap;

pub struct DebugPreprocessCallback;

#[async_trait]
impl PreprocessCallback for DebugPreprocessCallback {
    async fn preprocess_before_upload(
        &mut self,
        key: &str,
        source_object: &GetObjectOutput,
        metadata: &mut UploadMetadata,
    ) -> Result<()> {
        // For cancel test purposes
        if key == "callback_cancel_test" || key == "data1" {
            return Err(anyhow::Error::from(PreprocessError::Cancelled));
        }

        let content_length = source_object.content_length.unwrap().to_string();
        if let Some(user_defined_metadata) = metadata.metadata.as_mut() {
            user_defined_metadata.insert("mycontent-length".to_string(), content_length);
        } else {
            let mut user_defined_metadata = HashMap::new();
            user_defined_metadata.insert("mycontent-length".to_string(), content_length);
            metadata.metadata = Some(user_defined_metadata);
        }

        Ok(())
    }
}
