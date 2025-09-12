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
            // This is special for test emulation.
            #[allow(clippy::collapsible_if)]
            if cfg!(feature = "e2e_test_dangerous_simulations") {
                panic_simulation(&self.base.config, "ObjectDeleter::receive_and_filter");

                if is_error_simulation_point(&self.base.config, "ObjectDeleter::receive_and_filter")
                {
                    error!("error simulation point has been triggered.");
                    return Err(anyhow::anyhow!(
                        "error simulation point has been triggered."
                    ));
                }
            }

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

fn panic_simulation(config: &crate::Config, panic_simulation_point: &str) {
    const PANIC_DANGEROUS_SIMULATION_ENV: &str = "S3SYNC_PANIC_DANGEROUS_SIMULATION";
    const PANIC_DANGEROUS_SIMULATION_ENV_ALLOW: &str = "ALLOW";

    if std::env::var(PANIC_DANGEROUS_SIMULATION_ENV)
        .is_ok_and(|v| v == PANIC_DANGEROUS_SIMULATION_ENV_ALLOW)
        && config
            .panic_simulation_point
            .as_ref()
            .is_some_and(|point| point == panic_simulation_point)
    {
        panic!(
            "panic simulation has been triggered. This message should not be shown in the production.",
        );
    }
}

fn is_error_simulation_point(config: &crate::Config, error_simulation_point: &str) -> bool {
    const ERROR_DANGEROUS_SIMULATION_ENV: &str = "S3SYNC_ERROR_DANGEROUS_SIMULATION";
    const ERROR_DANGEROUS_SIMULATION_ENV_ALLOW: &str = "ALLOW";

    std::env::var(ERROR_DANGEROUS_SIMULATION_ENV)
        .is_ok_and(|v| v == ERROR_DANGEROUS_SIMULATION_ENV_ALLOW)
        && config
            .error_simulation_point
            .as_ref()
            .is_some_and(|point| point == error_simulation_point)
}
