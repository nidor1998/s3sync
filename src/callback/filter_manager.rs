use anyhow::Result;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::types::S3syncObject;
use crate::types::filter_callback::FilterCallback;

#[derive(Clone)]
pub struct FilterManager {
    pub preprocess_callback: Option<Arc<Mutex<Box<dyn FilterCallback + Send + Sync>>>>,
}

// RS-A1008 is not applicable here as this is an intentional implementation
impl Default for FilterManager {
    // skipcq: RS-A1008
    fn default() -> Self {
        // skipcq: RS-A1008
        Self::new()
    }
}
impl FilterManager {
    pub fn new() -> Self {
        Self {
            preprocess_callback: None,
        }
    }

    pub fn register_callback<T: FilterCallback + Send + Sync + 'static>(&mut self, callback: T) {
        self.preprocess_callback = Some(Arc::new(Mutex::new(Box::new(callback))));
    }

    pub fn is_callback_registered(&self) -> bool {
        self.preprocess_callback.is_some()
    }

    pub async fn execute_filter(&mut self, source: &S3syncObject) -> Result<bool> {
        if let Some(callback) = &self.preprocess_callback {
            callback.lock().await.filter(source).await
        } else {
            panic!("Filter callback is not registered");
        }
    }
}

impl fmt::Debug for FilterManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterManager")
            .field("filter_callback", &self.preprocess_callback.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_preprocess_manager() {
        // skipcq: RS-W1079
        let filter_manager = FilterManager::new();
        assert!(!filter_manager.is_callback_registered());
        println!("{:?}", filter_manager);

        let filter_manager = FilterManager::default();
        assert!(!filter_manager.is_callback_registered());
    }
}
