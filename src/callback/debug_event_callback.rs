use crate::types::event_callback::{EventCallback, EventData, EventType};
use async_trait::async_trait;

pub struct DebugEventCallback;

#[async_trait]
impl EventCallback for DebugEventCallback {
    async fn on_event(&mut self, event_data: EventData) {
        match event_data.event_type {
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
            _ => {
                println!("Other events: {event_data:?}");
            }
        }
    }
}
