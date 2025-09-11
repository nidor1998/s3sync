use crate::types::SyncStatistics::SyncDelete;
use crate::types::event_callback::{EventData, EventType};
use anyhow::{Result, anyhow};
use tracing::{error, info, trace};

use super::stage::{SendResult, Stage};

pub struct ObjectDeleter {
    worker_index: u16,
    base: Stage,
}

impl ObjectDeleter {
    pub fn new(base: Stage, worker_index: u16) -> Self {
        Self { base, worker_index }
    }

    pub async fn delete_target(&self) -> Result<()> {
        trace!(
            worker_index = self.worker_index,
            "delete target objects process started."
        );
        self.receive_and_delete().await
    }

    async fn receive_and_delete(&self) -> Result<()> {
        loop {
            tokio::select! {
                recv_result = self.base.receiver.as_ref().unwrap().recv() => {
                    match recv_result {
                        Ok(object) => {
                            if self.delete(object.key(), None).await.is_err() {
                                self.base.cancellation_token.cancel();
                                error!(worker_index = self.worker_index, "delete worker has been cancelled with error.");
                                return Err(anyhow!("delete worker has been cancelled with error."));
                            }

                            let mut event_data = EventData::new(EventType::SYNC_DELETE);
                            event_data.key = Some(object.key().to_string());
                            self.base.config
                                .event_manager
                                .trigger_event(event_data.clone())
                                .await;

                            if self.base.send(object).await? == SendResult::Closed {
                                return Ok(());
                            }
                        },
                        Err(_) => {
                            // normal shutdown
                            trace!(worker_index = self.worker_index, "delete worker has been completed.");
                            break;
                        }
                    }
                },
                _ = self.base.cancellation_token.cancelled() => {
                    info!(worker_index = self.worker_index, "delete worker has been cancelled.");
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn delete(&self, key: &str, version_id: Option<String>) -> Result<()> {
        self.base
            .target
            .as_ref()
            .unwrap()
            .delete_object(key, version_id)
            .await?;

        self.base
            .send_stats(SyncDelete {
                key: key.to_string(),
            })
            .await;

        Ok(())
    }
}
