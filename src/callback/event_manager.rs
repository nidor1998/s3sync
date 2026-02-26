use crate::types::event_callback::{EventCallback, EventData, EventType};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

#[derive(Default, Debug, Clone)]
pub struct SyncStats {
    pub pipeline_start_time: Option<Instant>,
    pub stats_transferred_byte: u64,
    pub stats_transferred_byte_per_sec: u64, // This field calculates after the pipeline ends
    pub stats_transferred_object: u64,
    pub stats_transferred_object_per_sec: u64, // This field calculates after the pipeline ends
    pub stats_etag_verified: u64,
    pub stats_etag_mismatch: u64,
    pub stats_checksum_verified: u64,
    pub stats_checksum_mismatch: u64,
    pub stats_deleted: u64,
    pub stats_skipped: u64,
    pub stats_error: u64,
    pub stats_warning: u64,
    pub stats_duration_sec: f64,
}

impl From<SyncStats> for EventData {
    fn from(stats: SyncStats) -> Self {
        let mut event_data = EventData::new(EventType::STATS_REPORT);
        event_data.stats_transferred_byte = Some(stats.stats_transferred_byte);
        event_data.stats_transferred_byte_per_sec = Some(stats.stats_transferred_byte_per_sec);
        event_data.stats_transferred_object = Some(stats.stats_transferred_object);
        event_data.stats_transferred_object_per_sec = Some(stats.stats_transferred_object_per_sec);
        event_data.stats_etag_verified = Some(stats.stats_etag_verified);
        event_data.stats_etag_mismatch = Some(stats.stats_etag_mismatch);
        event_data.stats_checksum_verified = Some(stats.stats_checksum_verified);
        event_data.stats_checksum_mismatch = Some(stats.stats_checksum_mismatch);
        event_data.stats_deleted = Some(stats.stats_deleted);
        event_data.stats_skipped = Some(stats.stats_skipped);
        event_data.stats_error = Some(stats.stats_error);
        event_data.stats_warning = Some(stats.stats_warning);
        event_data.stats_duration_sec = Some(stats.stats_duration_sec);
        event_data
    }
}

#[derive(Clone)]
pub struct EventManager {
    pub event_callback: Option<Arc<Mutex<Box<dyn EventCallback + Send + Sync>>>>,
    pub event_flags: EventType,
    pub dry_run: bool,
    pub sync_stats: Arc<Mutex<SyncStats>>,
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
            sync_stats: Arc::new(Mutex::new(SyncStats::default())),
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
        self.update_sync_stats(&event_data).await;

        if let Some(callback) = &self.event_callback {
            let event_type = event_data.event_type;
            if self.event_flags.contains(event_type) {
                event_data.dry_run = self.dry_run;
                callback.lock().await.on_event(event_data).await;
            }
            if event_type == EventType::PIPELINE_END
                && self.event_flags.contains(EventType::STATS_REPORT)
            {
                let sync_stats = self.sync_stats.lock().await.clone();
                let mut event_data: EventData = sync_stats.into();
                event_data.dry_run = self.dry_run;
                callback.lock().await.on_event(event_data).await;
            }
        }
    }

    pub async fn update_sync_stats(&self, event_data: &EventData) {
        let mut sync_stats = self.sync_stats.lock().await;

        match event_data.event_type {
            EventType::PIPELINE_START => {
                sync_stats.pipeline_start_time = Some(Instant::now());
            }
            EventType::PIPELINE_END => {
                sync_stats.stats_duration_sec = sync_stats
                    .pipeline_start_time
                    .unwrap()
                    .elapsed()
                    .as_secs_f64();
                if !self.dry_run {
                    if 1.0 < sync_stats.stats_duration_sec {
                        sync_stats.stats_transferred_byte_per_sec =
                            (sync_stats.stats_transferred_byte as f64
                                / sync_stats.stats_duration_sec) as u64;
                        sync_stats.stats_transferred_object_per_sec =
                            (sync_stats.stats_transferred_object as f64
                                / sync_stats.stats_duration_sec) as u64;
                    } else {
                        sync_stats.stats_transferred_byte_per_sec =
                            sync_stats.stats_transferred_byte;
                        sync_stats.stats_transferred_object_per_sec =
                            sync_stats.stats_transferred_object;
                    }
                }
            }
            EventType::SYNC_COMPLETE => {
                sync_stats.stats_transferred_object += 1;
                sync_stats.stats_transferred_byte += event_data.source_size.unwrap();
            }
            EventType::SYNC_DELETE => {
                sync_stats.stats_deleted += 1;
            }
            EventType::SYNC_ETAG_VERIFIED => {
                sync_stats.stats_etag_verified += 1;
            }
            EventType::SYNC_ETAG_MISMATCH => {
                sync_stats.stats_etag_mismatch += 1;
                sync_stats.stats_warning += 1;
            }
            EventType::SYNC_CHECKSUM_VERIFIED => {
                sync_stats.stats_checksum_verified += 1;
            }
            EventType::SYNC_CHECKSUM_MISMATCH => {
                sync_stats.stats_checksum_mismatch += 1;
                sync_stats.stats_warning += 1;
            }
            EventType::SYNC_WARNING => {
                sync_stats.stats_warning += 1;
            }
            EventType::PIPELINE_ERROR => {
                sync_stats.stats_error += 1;
            }
            EventType::SYNC_FILTERED => {
                sync_stats.stats_skipped += 1;
            }

            _ => {}
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

    #[test]
    fn from_sync_stats_to_event_data() {
        let sync_stats = SyncStats {
            pipeline_start_time: None,
            stats_transferred_byte: 1,
            stats_transferred_byte_per_sec: 2,
            stats_transferred_object: 3,
            stats_transferred_object_per_sec: 4,
            stats_etag_verified: 5,
            stats_etag_mismatch: 6,
            stats_checksum_verified: 7,
            stats_checksum_mismatch: 8,
            stats_deleted: 9,
            stats_skipped: 10,
            stats_error: 11,
            stats_warning: 12,
            stats_duration_sec: 13.0,
        };

        let event_data: EventData = sync_stats.into();
        assert_eq!(event_data.event_type, EventType::STATS_REPORT);
        assert_eq!(event_data.stats_transferred_byte, Some(1));
        assert_eq!(event_data.stats_transferred_byte_per_sec, Some(2));
        assert_eq!(event_data.stats_transferred_object, Some(3));
        assert_eq!(event_data.stats_transferred_object_per_sec, Some(4));
        assert_eq!(event_data.stats_etag_verified, Some(5));
        assert_eq!(event_data.stats_etag_mismatch, Some(6));
        assert_eq!(event_data.stats_checksum_verified, Some(7));
        assert_eq!(event_data.stats_checksum_mismatch, Some(8));
        assert_eq!(event_data.stats_deleted, Some(9));
        assert_eq!(event_data.stats_skipped, Some(10));
        assert_eq!(event_data.stats_error, Some(11));
        assert_eq!(event_data.stats_warning, Some(12));
        assert_eq!(event_data.stats_duration_sec, Some(13.0));
    }
}
