use super::stage::{SendResult, Stage};
use crate::storage::e_tag_verify::normalize_e_tag;
use crate::types::SyncStatistics;
use crate::types::event_callback::{EventData, EventType};
use anyhow::{Result, anyhow};
use tracing::{error, info, trace};

pub struct UserDefinedFilter {
    base: Stage,
}

impl UserDefinedFilter {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn filter(&mut self) -> Result<()> {
        trace!("user defined filter worker started.");
        self.receive_and_filter().await
    }

    async fn receive_and_filter(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                recv_result = self.base.receiver.as_ref().unwrap().recv() => {
                    match recv_result {
                        Ok(object) => {
                            let need_sync = self.base.config.filter_config.filter_manager.execute_filter(
                                &object,
                            ).await;

                            if let Err(e) = need_sync {
                                let error = e.to_string();
                                let mut event_data = EventData::new(EventType::PIPELINE_ERROR);
                                event_data.message = Some(format!("User defined filter error: {error}"));
                                self.base.config.event_manager.trigger_event(event_data).await;

                                self.base.cancellation_token.cancel();
                                error!("user defined filter worker has been cancelled with error: {}", e);

                                return Err(anyhow!("user defined filter worker has been cancelled with error: {}", e));
                            }

                            if !need_sync? {
                                if self.base.config.event_manager.is_callback_registered() {
                                    let mut event_data = EventData::new(EventType::SYNC_FILTERED);
                                    event_data.key = Some(object.key().to_string());
                                    // skipcq: RS-W1070
                                    event_data.source_version_id =
                                        object.version_id().map(|v| v.to_string());
                                    event_data.source_etag = object
                                        .e_tag()
                                        .map(|e| normalize_e_tag(&Some(e.to_string())).unwrap());
                                    event_data.source_last_modified = Some(*object.last_modified());
                                    event_data.source_size = Some(object.size() as u64);

                                    event_data.message = Some("Object filtered by user defined filter".to_string());
                                    self.base
                                        .config
                                        .event_manager
                                        .trigger_event(event_data)
                                        .await;
                                }

                                 let _ = self
                                    .base
                                    .target
                                    .as_ref()
                                    .unwrap()
                                    .get_stats_sender()
                                    .send(SyncStatistics::SyncSkip {
                                        key: object.key().to_string(),
                                    }).await;

                                continue;
                            }

                            if self.base.send(object).await? == SendResult::Closed {
                                return Ok(());
                            }
                        },
                        Err(_) => {
                            // normal shutdown
                            trace!("user defined filter worker has been completed.");
                            break;
                        }
                    }
                },
                _ = self.base.cancellation_token.cancelled() => {
                    info!("user defined filter worker has been cancelled.");
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}
