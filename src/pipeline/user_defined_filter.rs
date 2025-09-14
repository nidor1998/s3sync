use super::stage::{SendResult, Stage};
use crate::storage::e_tag_verify::normalize_e_tag;
use crate::types::SyncStatistics;
use crate::types::event_callback::{EventData, EventType};
use anyhow::{Result, anyhow};
use tracing::{debug, error, info};

pub struct UserDefinedFilter {
    base: Stage,
}

impl UserDefinedFilter {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn filter(&mut self) -> Result<()> {
        debug!("user defined filter worker started.");
        self.receive_and_filter().await
    }

    async fn receive_and_filter(&mut self) -> Result<()> {
        loop {
            // This is special for test emulation.
            #[allow(clippy::collapsible_if)]
            if cfg!(feature = "e2e_test_dangerous_simulations") {
                panic_simulation(&self.base.config, "UserDefinedFilter::receive_and_filter");

                if is_error_simulation_point(
                    &self.base.config,
                    "UserDefinedFilter::receive_and_filter",
                ) {
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
                            debug!("user defined filter worker has been completed.");
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
