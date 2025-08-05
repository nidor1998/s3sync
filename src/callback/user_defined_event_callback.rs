use crate::types::event_callback::{EventCallback, EventData, EventType};
use async_trait::async_trait;

// This struct represents a user-defined preprocessed callback.
// It can be used to implement custom event handling logic, such as logging or monitoring.
pub struct UserDefinedEventCallback {
    pub enable: bool,
}

impl UserDefinedEventCallback {
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
impl EventCallback for UserDefinedEventCallback {
    // If you want to implement a custom event callback, you can do so by modifying this function.
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    #[cfg(not(tarpaulin_include))]
    async fn on_event(&mut self, event_data: EventData) {
        // Todo: Implement your custom event handling logic here.
        match event_data.event_type {
            // Sync statistics report events are not captured by the pipeline.
            // Maybe there is no need to capture these events
            // the pipeline captures only synchronization events.
            EventType::PIPELINE_START => {
                println!("Pipeline started: {event_data:?}");
            }
            EventType::PIPELINE_END => {
                println!("Pipeline ended: {event_data:?}");
            }

            // The following events occur per object during the sync process
            EventType::SYNC_START => {
                println!("Sync started: {event_data:?}");
            }
            EventType::SYNC_COMPLETE => {
                println!("Sync complete: {event_data:?}");
            }
            EventType::SYNC_DELETE => {
                println!("Sync deleted: {event_data:?}");
            }

            // The following events occur after the SYNC_COMPLETE event
            EventType::SYNC_ETAG_VERIFIED => {
                println!("Sync ETag verified: {event_data:?}");
            }
            EventType::SYNC_CHECKSUM_VERIFIED => {
                println!("Sync checksum verified: {event_data:?}");
            }
            EventType::SYNC_ETAG_MISMATCH => {
                println!("Sync ETag mismatch: {event_data:?}");
            }
            EventType::SYNC_CHECKSUM_MISMATCH => {
                println!("Sync checksum mismatch: {event_data:?}");
            }

            // Not all warnings trigger this event, but it is used for general (useful for crate user) warnings
            EventType::SYNC_WARNING => {
                println!("Sync warning: {event_data:?}");
            }

            // If an error occurs during the pipeline, this event is triggered and the pipeline is stopped
            EventType::PIPELINE_ERROR => {
                println!("Pipeline error: {event_data:?}");
            }

            // If a syncing object is cancelled by preprocess callback, this event is triggered
            EventType::SYNC_CANCEL => {
                println!("Sync cancelled: {event_data:?}");
            }

            // Currently, all events are captured by above match arms,
            _ => {
                println!("Other events: {event_data:?}");
            }
        }
    }
}
