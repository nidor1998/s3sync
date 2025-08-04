use anyhow::Error as AnyhowError;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::types::{ObjectCannedAcl, RequestPayer, StorageClass};
use aws_smithy_types::DateTime;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Default, Debug, Clone)]
pub struct UploadMetadata {
    pub acl: Option<ObjectCannedAcl>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<DateTime>,
    pub metadata: Option<HashMap<String, String>>,
    pub request_payer: Option<RequestPayer>,
    pub storage_class: Option<StorageClass>,
    pub website_redirect_location: Option<String>,
    pub tagging: Option<String>,
}

#[derive(Debug, Error)]
pub enum PreprocessError {
    #[error("Preprocess failed")]
    Other(String),
    #[error("Callback cancelled")]
    Cancelled,
}

pub fn is_callback_cancelled(err: &AnyhowError) -> bool {
    err.downcast_ref::<PreprocessError>()
        .is_some_and(|e| matches!(e, PreprocessError::Cancelled))
}

#[async_trait]
pub trait PreprocessCallback {
    /// Preprocess the object before uploading to S3.
    /// This method is called before the upload process starts.
    /// It can modify the `UploadMetadata` to set properties like content-type, user-defined metadata, etc.
    ///
    /// If the error returned is `PreprocessError::Cancelled`, the upload will be canceled.
    /// If the error is any other type, it will be treated as a not-retryable error.
    async fn preprocess_before_upload(
        &mut self,
        key: &str,
        source_object: &GetObjectOutput,
        metadata: &mut UploadMetadata,
    ) -> Result<()>;
}
