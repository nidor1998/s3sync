use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::types::event_callback::{EventCallback, EventData, EventType};

#[derive(Clone)]
pub struct EventManager {
    pub event_callback: Option<Arc<Mutex<Box<dyn EventCallback + Send + Sync>>>>,
    pub event_flags: EventType,
}

impl Default for EventManager {
    fn default() -> Self {
        // RS-A1008 is not applicable here as this is intentional implementation
        // skipcq: RS-A1008
        Self::new()
    }
}

impl EventManager {
    pub fn new() -> Self {
        Self {
            event_callback: None,
            event_flags: EventType::ALL_EVENTS,
        }
    }

    pub fn register_callback<T: EventCallback + Send + Sync + 'static>(
        &mut self,
        events_flag: EventType,
        callback: T,
    ) {
        self.event_callback = Some(Arc::new(Mutex::new(Box::new(callback))));
        self.event_flags = events_flag;
    }

    pub async fn trigger_event(&self, event_data: EventData) {
        if let Some(callback) = &self.event_callback {
            if self.event_flags.contains(event_data.event_type) {
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
