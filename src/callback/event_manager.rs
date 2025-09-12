use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::types::event_callback::{EventCallback, EventData, EventType};

#[derive(Clone)]
pub struct EventManager {
    pub event_callback: Option<Arc<Mutex<Box<dyn EventCallback + Send + Sync>>>>,
    pub event_flags: EventType,
    pub dry_run: bool,
}

// RS-A1008 is not applicable here as this is intentional implementation
impl Default for EventManager {
    // skipcq: RS-A1008
    fn default() -> Self {
        // skipcq: RS-A1008
        Self::new()
    }
}

impl EventManager {
    pub fn new() -> Self {
        Self {
            event_callback: None,
            event_flags: EventType::ALL_EVENTS,
            dry_run: false,
        }
    }

    pub fn register_callback<T: EventCallback + Send + Sync + 'static>(
        &mut self,
        events_flag: EventType,
        callback: T,
        dry_run: bool,
    ) {
        self.event_callback = Some(Arc::new(Mutex::new(Box::new(callback))));
        self.event_flags = events_flag;
        self.dry_run = dry_run;
    }

    pub fn is_callback_registered(&self) -> bool {
        self.event_callback.is_some()
    }

    pub async fn trigger_event(&self, mut event_data: EventData) {
        if let Some(callback) = &self.event_callback {
            if self.event_flags.contains(event_data.event_type) {
                event_data.dry_run = self.dry_run;
                callback.lock().await.on_event(event_data).await;
            }
        }
    }
}

impl fmt::Debug for EventManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventManager")
            .field("event_flags", &self.event_flags)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_event_manager() {
        // skipcq: RS-W1079
        let event_manager = EventManager::new();
        assert!(event_manager.event_callback.is_none());
        println!("{:?}", event_manager);

        let event_manager = EventManager::default();
        assert!(event_manager.event_callback.is_none());
    }
}
