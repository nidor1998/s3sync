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
            if cfg!(e2e_test_dangerous_simulations) {
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

#[cfg(all(test, feature = "lua_support"))]
mod tests {
    use super::*;
    use crate::Config;
    use crate::config::args::parse_from_args;
    use crate::types::S3syncObject;
    use crate::types::token::create_pipeline_cancellation_token;
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use tracing_subscriber::EnvFilter;

    fn build_config() -> Config {
        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--filter-callback-lua-script",
            "./test_data/script/filter_callback.lua",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        Config::try_from(parse_from_args(args).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn filter_send_next_stage_closed() {
        init_dummy_tracing_subscriber();

        let config = build_config();
        let cancellation_token = create_pipeline_cancellation_token();

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        // The next stage receiver is dropped so send() returns SendResult::Closed.
        let (next_sender, next_receiver) = async_channel::bounded::<S3syncObject>(1000);
        drop(next_receiver);

        // "dir21/" makes the lua filter return a truthy value, so the object is synced.
        sender
            .send(S3syncObject::NotVersioning(
                Object::builder()
                    .key("dir21/6byte.dat")
                    .size(6)
                    .last_modified(DateTime::from_secs(1))
                    .build(),
            ))
            .await
            .unwrap();
        sender.close();

        let stage = Stage::new(
            config,
            None,
            None,
            Some(receiver),
            Some(next_sender),
            cancellation_token.clone(),
            Arc::new(AtomicBool::new(false)),
        );
        let mut user_defined_filter = UserDefinedFilter::new(stage);

        assert!(user_defined_filter.filter().await.is_ok());
    }

    #[tokio::test]
    async fn filter_cancelled() {
        init_dummy_tracing_subscriber();

        let config = build_config();
        let cancellation_token = create_pipeline_cancellation_token();

        // Keep the sender half alive so recv() blocks and the cancellation branch is taken.
        let (_sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        let (next_sender, _next_receiver) = async_channel::bounded::<S3syncObject>(1000);

        let stage = Stage::new(
            config,
            None,
            None,
            Some(receiver),
            Some(next_sender),
            cancellation_token.clone(),
            Arc::new(AtomicBool::new(false)),
        );
        let mut user_defined_filter = UserDefinedFilter::new(stage);

        cancellation_token.cancel();

        assert!(user_defined_filter.filter().await.is_ok());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
