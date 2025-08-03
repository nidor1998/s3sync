use crate::types::event_callback::{EventCallback, EventData, EventType};
use async_trait::async_trait;

pub struct DebugEventCallback;

#[async_trait]
impl EventCallback for DebugEventCallback {
    async fn on_event(&mut self, event_data: EventData) {
        match event_data.event_type {
            EventType::UNDEFINED => {
                println!("Undefined event: {event_data:?}");
            }
            EventType::PIPELINE_START => {
                println!("Pipeline started: {event_data:?}");
            }
            EventType::PIPELINE_END => {
                println!("Pipeline ended: {event_data:?}");
            }
            EventType::SYNC_START => {
                println!("Sync started: {event_data:?}");
            }
            EventType::SYNC_COMPLETE => {
                println!("Sync complete: {event_data:?}");
            }
            EventType::SYNC_DELETE => {
                println!("Sync delete: {event_data:?}");
            }
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
            EventType::SYNC_WARNING => {
                println!("Sync warning: {event_data:?}");
            }
            EventType::PIPELINE_ERROR => {
                println!("Pipeline error: {event_data:?}");
            }
            _ => {
                panic!("Unknown event type: {event_data:?}");
            }
        }
    }
}
