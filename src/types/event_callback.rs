use async_trait::async_trait;
use aws_sdk_s3::types::ChecksumAlgorithm;
use aws_smithy_types::DateTime;
use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    pub struct EventType: u64 {
        const UNDEFINED = 0u64;
        const PIPELINE_START = 1u64 << 1;
        const PIPELINE_END = 1u64 << 2;
        const SYNC_COMPLETE = 1u64 << 3;
        const SYNC_DELETE =  1u64 << 4;
        const SYNC_ETAG_VERIFIED = 1u64 << 5;
        const SYNC_CHECKSUM_VERIFIED = 1u64 << 6;
        const SYNC_ETAG_MISMATCH = 1u64 << 7;
        const SYNC_CHECKSUM_MISMATCH = 1u64 << 8;
        const PIPELINE_ERROR = 1u64 << 9;

        const ALL_EVENTS  = !0;
    }
}

#[derive(Default, Debug, Clone)]
pub struct EventData {
    pub event_type: EventType,
    pub key: Option<String>,
    pub source_version_id: Option<String>,
    pub target_version_id: Option<String>,
    pub source_last_modified: Option<DateTime>,
    pub target_last_modified: Option<DateTime>,
    pub source_size: Option<u64>,
    pub target_size: Option<u64>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub source_checksum: Option<String>,
    pub target_checksum: Option<String>,
    pub source_etag: Option<String>,
    pub target_etag: Option<String>,
    pub message: Option<String>,
}

impl EventData {
    pub fn new(event_type: EventType) -> Self {
        Self {
            event_type,
            ..Default::default()
        }
    }
}

#[async_trait]
pub trait EventCallback {
    async fn on_event(&mut self, event_data: EventData);
}
