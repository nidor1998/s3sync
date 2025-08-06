use crate::types::preprocess_callback::{PreprocessCallback, UploadMetadata};
use anyhow::Result;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct PreprocessManager {
    pub preprocess_callback: Option<Arc<Mutex<Box<dyn PreprocessCallback + Send + Sync>>>>,
}

// RS-A1008 is not applicable here as this is an intentional implementation
impl Default for PreprocessManager {
    // skipcq: RS-A1008
    fn default() -> Self {
        // skipcq: RS-A1008
        Self::new()
    }
}
impl PreprocessManager {
    pub fn new() -> Self {
        Self {
            preprocess_callback: None,
        }
    }

    pub fn register_callback<T: PreprocessCallback + Send + Sync + 'static>(
        &mut self,
        callback: T,
    ) {
        self.preprocess_callback = Some(Arc::new(Mutex::new(Box::new(callback))));
    }

    pub fn is_callback_registered(&self) -> bool {
        self.preprocess_callback.is_some()
    }

    pub async fn execute_preprocessing(
        &mut self,
        key: &str,
        source: &GetObjectOutput,
        metadata: &mut UploadMetadata,
    ) -> Result<()> {
        if let Some(callback) = &self.preprocess_callback {
            callback
                .lock()
                .await
                .preprocess_before_upload(key, source, metadata)
                .await?;
        }
        Ok(())
    }
}

impl fmt::Debug for PreprocessManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreprocessManager")
            .field("preprocess_callback", &self.preprocess_callback.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_preprocess_manager() {
        // skipcq: RS-W1079
        let preprocess_manager = PreprocessManager::new();
        assert!(!preprocess_manager.is_callback_registered());
        println!("{:?}", preprocess_manager);

        let preprocess_manager = PreprocessManager::default();
        assert!(!preprocess_manager.is_callback_registered());
    }
}
