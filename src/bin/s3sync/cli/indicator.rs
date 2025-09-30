use std::io;
use std::io::Write;

use async_channel::Receiver;
use indicatif::{HumanBytes, HumanCount, HumanDuration, ProgressBar, ProgressStyle};
use s3sync::types::SyncStatistics;
use s3sync::types::event_callback::{EventData, EventType};
use simple_moving_average::{SMA, SumTreeSMA};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::info;

#[derive(Default, Debug, Clone)]
pub struct SyncStats {
    pub stats_transferred_byte: Option<u64>,
    pub stats_transferred_byte_per_sec: Option<u64>,
    pub stats_transferred_object: Option<u64>,
    pub stats_transferred_object_per_sec: Option<u64>,
    pub stats_etag_verified: Option<u64>,
    pub stats_checksum_verified: Option<u64>,
    pub stats_deleted: Option<u64>,
    pub stats_skipped: Option<u64>,
    pub stats_error: Option<u64>,
    pub stats_warning: Option<u64>,
    pub stats_duration_sec: Option<f64>,
}

impl From<SyncStats> for EventData {
    fn from(stats: SyncStats) -> Self {
        let mut event_data = EventData::new(EventType::STATS_REPORT);
        event_data.stats_transferred_byte = stats.stats_transferred_byte;
        event_data.stats_transferred_byte_per_sec = stats.stats_transferred_byte_per_sec;
        event_data.stats_transferred_object = stats.stats_transferred_object;
        event_data.stats_transferred_object_per_sec = stats.stats_transferred_object_per_sec;
        event_data.stats_etag_verified = stats.stats_etag_verified;
        event_data.stats_checksum_verified = stats.stats_checksum_verified;
        event_data.stats_deleted = stats.stats_deleted;
        event_data.stats_skipped = stats.stats_skipped;
        event_data.stats_error = stats.stats_error;
        event_data.stats_warning = stats.stats_warning;
        event_data.stats_duration_sec = stats.stats_duration_sec;
        event_data
    }
}

const MOVING_AVERAGE_PERIOD_SECS: usize = 10;
const REFRESH_INTERVAL: f32 = 1.0;

pub fn show_indicator(
    stats_receiver: Receiver<SyncStatistics>,
    show_progress: bool,
    show_result: bool,
    log_sync_summary: bool,
    dry_run: bool,
) -> JoinHandle<SyncStats> {
    let progress_style = ProgressStyle::with_template("{wide_msg}").unwrap();
    let progress_text = ProgressBar::new(0);
    progress_text.set_style(progress_style);

    tokio::spawn(async move {
        let mut sync_stats = SyncStats::default();

        let start_time = Instant::now();

        let mut ma_synced_bytes = SumTreeSMA::<_, u64, MOVING_AVERAGE_PERIOD_SECS>::new();
        let mut ma_synced_count = SumTreeSMA::<_, u64, MOVING_AVERAGE_PERIOD_SECS>::new();

        let mut total_sync_count: u64 = 0;
        let mut total_sync_bytes: u64 = 0;
        let mut total_error_count: u64 = 0;
        let mut total_skip_count: u64 = 0;
        let mut total_delete_count: u64 = 0;
        let mut total_warning_count: u64 = 0;
        let mut total_e_tag_verified_count: u64 = 0;
        let mut total_checksum_verified_count: u64 = 0;

        loop {
            let mut sync_bytes: u64 = 0;
            let mut sync_count: u64 = 0;

            let period = Instant::now();
            loop {
                while let Ok(sync_stats) = stats_receiver.try_recv() {
                    match sync_stats {
                        SyncStatistics::SyncComplete { .. } => {
                            sync_count += 1;
                            total_sync_count += 1;
                        }
                        SyncStatistics::SyncBytes(size) => {
                            sync_bytes += size;
                            total_sync_bytes += size
                        }
                        SyncStatistics::SyncError { .. } => {
                            total_error_count += 1;
                        }
                        SyncStatistics::SyncSkip { .. } => {
                            total_skip_count += 1;
                        }
                        SyncStatistics::SyncDelete { .. } => {
                            total_delete_count += 1;
                        }
                        SyncStatistics::SyncWarning { .. } => {
                            total_warning_count += 1;
                        }
                        SyncStatistics::ETagVerified { .. } => {
                            total_e_tag_verified_count += 1;
                        }
                        SyncStatistics::ChecksumVerified { .. } => {
                            total_checksum_verified_count += 1;
                        }
                    }
                }

                if REFRESH_INTERVAL < period.elapsed().as_secs_f32() {
                    break;
                }

                if stats_receiver.is_closed() {
                    let elapsed = start_time.elapsed();
                    let elapsed_secs_f64 = elapsed.as_secs_f64();

                    let mut objects_per_sec = (total_sync_count as f64 / elapsed_secs_f64) as u64;
                    let mut sync_bytes_per_sec =
                        (total_sync_bytes as f64 / elapsed_secs_f64) as u64;

                    if elapsed_secs_f64 < REFRESH_INTERVAL as f64 {
                        objects_per_sec = total_sync_count;
                        sync_bytes_per_sec = total_sync_bytes;
                    }
                    if dry_run {
                        objects_per_sec = 0;
                        sync_bytes_per_sec = 0;
                    }

                    if log_sync_summary {
                        info!(
                            message = "sync summary",
                            transferred_byte = total_sync_bytes,
                            transferred_byte_per_sec = sync_bytes_per_sec,
                            transferred_object = total_sync_count,
                            transferred_object_per_sec = objects_per_sec,
                            etag_verified = total_e_tag_verified_count,
                            checksum_verified = total_checksum_verified_count,
                            deleted = total_delete_count,
                            skipped = total_skip_count,
                            error = total_error_count,
                            warning = total_warning_count,
                            duration_sec = elapsed_secs_f64,
                        );
                    }

                    if show_result {
                        progress_text.set_style(ProgressStyle::with_template("{msg}").unwrap());

                        progress_text.finish_with_message(format!(
                            "{:>3} | {:>3}/sec,  transferred {:>3} objects | {:>3} objects/sec,  etag verified {} objects,  checksum verified {} objects,  deleted {} objects,  skipped {} objects,  error {} objects, warning {} objects,  duration {}",
                            HumanBytes(total_sync_bytes),
                            HumanBytes(sync_bytes_per_sec),
                            total_sync_count,
                            HumanCount(objects_per_sec),
                            total_e_tag_verified_count,
                            total_checksum_verified_count,
                            total_delete_count,
                            total_skip_count,
                            total_error_count,
                            total_warning_count,
                            HumanDuration(elapsed),
                        ));

                        println!();
                        io::stdout().flush().unwrap()
                    }

                    sync_stats.stats_transferred_byte = Some(total_sync_bytes);
                    sync_stats.stats_transferred_byte_per_sec = Some(sync_bytes_per_sec);
                    sync_stats.stats_transferred_object = Some(total_sync_count);
                    sync_stats.stats_transferred_object_per_sec = Some(objects_per_sec);
                    sync_stats.stats_etag_verified = Some(total_e_tag_verified_count);
                    sync_stats.stats_checksum_verified = Some(total_checksum_verified_count);
                    sync_stats.stats_deleted = Some(total_delete_count);
                    sync_stats.stats_skipped = Some(total_skip_count);
                    sync_stats.stats_error = Some(total_error_count);
                    sync_stats.stats_warning = Some(total_warning_count);
                    sync_stats.stats_duration_sec = Some(elapsed_secs_f64);

                    return sync_stats;
                }

                tokio::time::sleep(std::time::Duration::from_secs_f32(0.05)).await;
            }
            ma_synced_bytes.add_sample(sync_bytes);

            if !dry_run {
                ma_synced_count.add_sample(sync_count);
            }

            if show_progress {
                progress_text.set_message(format!(
                    "{:>3} | {:>3}/sec,  transferred {:>3} objects | {:>3} objects/sec,  etag verified {} objects,  checksum verified {} objects,  deleted {} objects,  skipped {} objects,  error {} objects, warning {} objects",
                    HumanBytes(total_sync_bytes),
                    HumanBytes(ma_synced_bytes.get_average()).to_string(),
                    total_sync_count,
                    HumanCount(ma_synced_count.get_average()).to_string(),
                    total_e_tag_verified_count,
                    total_checksum_verified_count,
                    total_delete_count,
                    total_skip_count,
                    total_error_count,
                    total_warning_count,
                ));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const WAITING_TIME_MILLIS_FOR_ASYNC_INDICATOR_SET_MESSAGE: u64 = 1500;

    #[tokio::test]
    async fn indicator_test_show_result() {
        init_dummy_tracing_subscriber();

        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(stats_receiver, true, true, false, false);

        stats_sender
            .send(SyncStatistics::SyncBytes(1))
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncComplete {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncSkip {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncWarning {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncError {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ETagVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ChecksumVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_INDICATOR_SET_MESSAGE,
        ))
        .await;
        stats_sender.close();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn indicator_test_show_no_result() {
        init_dummy_tracing_subscriber();

        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(stats_receiver, true, false, true, false);

        stats_sender
            .send(SyncStatistics::SyncBytes(1))
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncComplete {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncSkip {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncError {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncDelete {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ETagVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ChecksumVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_INDICATOR_SET_MESSAGE,
        ))
        .await;
        stats_sender.close();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn indicator_test_show_result_dry_run() {
        init_dummy_tracing_subscriber();

        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(stats_receiver, true, true, true, true);

        stats_sender
            .send(SyncStatistics::SyncBytes(1))
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncComplete {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncSkip {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncWarning {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncError {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ETagVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ChecksumVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_INDICATOR_SET_MESSAGE,
        ))
        .await;
        stats_sender.close();

        join_handle.await.unwrap();
    }

    #[test]
    fn from_sync_stats_to_event_data() {
        let sync_stats = SyncStats {
            stats_transferred_byte: Some(1),
            stats_transferred_byte_per_sec: Some(2),
            stats_transferred_object: Some(3),
            stats_transferred_object_per_sec: Some(4),
            stats_etag_verified: Some(5),
            stats_checksum_verified: Some(6),
            stats_deleted: Some(7),
            stats_skipped: Some(8),
            stats_error: Some(9),
            stats_warning: Some(10),
            stats_duration_sec: Some(11.0),
        };

        let event_data: EventData = sync_stats.into();
        assert_eq!(event_data.event_type, EventType::STATS_REPORT);
        assert_eq!(event_data.stats_transferred_byte, Some(1));
        assert_eq!(event_data.stats_transferred_byte_per_sec, Some(2));
        assert_eq!(event_data.stats_transferred_object, Some(3));
        assert_eq!(event_data.stats_transferred_object_per_sec, Some(4));
        assert_eq!(event_data.stats_etag_verified, Some(5));
        assert_eq!(event_data.stats_checksum_verified, Some(6));
        assert_eq!(event_data.stats_deleted, Some(7));
        assert_eq!(event_data.stats_skipped, Some(8));
        assert_eq!(event_data.stats_error, Some(9));
        assert_eq!(event_data.stats_warning, Some(10));
        assert_eq!(event_data.stats_duration_sec, Some(11.0));
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
