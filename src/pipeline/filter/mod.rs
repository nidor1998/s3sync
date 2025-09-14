use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use tracing::{debug, error};

use super::stage::{SendResult, Stage};
use crate::config::FilterConfig;
pub use crate::pipeline::filter::exclude_regex::ExcludeRegexFilter;
pub use crate::pipeline::filter::include_regex::IncludeRegexFilter;
pub use crate::pipeline::filter::larger_size::LargerSizeFilter;
pub use crate::pipeline::filter::modified::TargetModifiedFilter;
pub use crate::pipeline::filter::mtime_after::MtimeAfterFilter;
pub use crate::pipeline::filter::mtime_before::MtimeBeforeFilter;
pub use crate::pipeline::filter::smaller_size::SmallerSizeFilter;
use crate::storage::e_tag_verify::normalize_e_tag;
use crate::types::event_callback::{EventData, EventType};
use crate::types::filter_message::get_filtered_message_by_name;
use crate::types::{ObjectKey, ObjectKeyMap, S3syncObject, SyncStatistics, sha1_digest_from_key};

mod exclude_regex;
mod include_regex;
mod larger_size;
mod modified;
mod mtime_after;
mod mtime_before;
mod smaller_size;

#[async_trait]
pub trait ObjectFilter {
    async fn filter(&self) -> Result<()>;
}

pub struct ObjectFilterBase<'a> {
    name: &'a str,
    base: Stage,
    target_key_map: Option<ObjectKeyMap>,
}

impl ObjectFilterBase<'_> {
    pub async fn filter<F>(&self, filter_fn: F) -> Result<()>
    where
        F: Fn(&S3syncObject, &FilterConfig, &ObjectKeyMap) -> bool,
    {
        self.receive_and_filter(filter_fn).await
    }

    async fn receive_and_filter<F>(&self, filter_fn: F) -> Result<()>
    where
        F: Fn(&S3syncObject, &FilterConfig, &ObjectKeyMap) -> bool,
    {
        let target_key_map = if self.target_key_map.is_some() {
            self.target_key_map.as_ref().unwrap().clone()
        } else {
            ObjectKeyMap::new(Mutex::new(HashMap::new()))
        };

        // On x86_64 linux, there is a performance issue with async tasks in the case of high load,
        // So, we yield the task to allow other tasks to run.
        // Other platforms may not need this, but it is safe to keep it for consistency.
        loop {
            tokio::task::yield_now().await;
            if self.base.cancellation_token.is_cancelled() {
                debug!(name = self.name, "filter has been cancelled.");
                return Ok(());
            }

            // This is special for test emulation.
            #[allow(clippy::collapsible_if)]
            if cfg!(feature = "e2e_test_dangerous_simulations") {
                panic_simulation(&self.base.config, "ObjectFilterBase::receive_and_filter");

                if is_error_simulation_point(
                    &self.base.config,
                    "ObjectFilterBase::receive_and_filter",
                ) {
                    error!(
                        name = self.name,
                        "error simulation point has been triggered."
                    );
                    return Err(anyhow::anyhow!(
                        "error simulation point has been triggered."
                    ));
                }
            }

            tokio::task::yield_now().await;
            match self.base.receiver.as_ref().unwrap().recv().await {
                Ok(object) => {
                    tokio::task::yield_now().await;
                    if !filter_fn(&object, &self.base.config.filter_config, &target_key_map) {
                        tokio::task::yield_now().await;

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

                            {
                                let locked_target_key_map = target_key_map.lock().unwrap();
                                let mut result = locked_target_key_map.get(
                                    &ObjectKey::KeySHA1Digest(sha1_digest_from_key(object.key())),
                                );
                                if result.is_none() {
                                    result = locked_target_key_map
                                        .get(&ObjectKey::KeyString(object.key().to_string()));
                                }
                                if let Some(entry) = result {
                                    event_data.target_etag = normalize_e_tag(&entry.e_tag.clone());
                                    event_data.target_last_modified = Some(entry.last_modified);
                                    event_data.target_size = Some(entry.content_length as u64);
                                }
                            }

                            event_data.message = Some(get_filtered_message_by_name(self.name));
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
                            })
                            .await;
                        continue;
                    }

                    tokio::task::yield_now().await;
                    if self.base.send(object).await? == SendResult::Closed {
                        return Ok(());
                    }
                }
                Err(_) => {
                    debug!(name = self.name, "filter has been completed.");
                    return Ok(());
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use crate::Config;
    use crate::config::args::parse_from_args;
    use crate::storage::StorageFactory;
    use crate::storage::local::LocalStorageFactory;
    use crate::types::token;
    use crate::types::token::PipelineCancellationToken;
    use async_channel::Receiver;
    use aws_sdk_s3::types::Object;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use tracing_subscriber::EnvFilter;

    use super::*;

    #[tokio::test]
    async fn filter_true() {
        init_dummy_tracing_subscriber();

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        let cancellation_token = token::create_pipeline_cancellation_token();
        let (base, next_stage_receiver) =
            create_base_helper(receiver, cancellation_token.clone()).await;
        let filter_base = ObjectFilterBase {
            base,
            target_key_map: None,
            name: "unittest",
        };
        let object = S3syncObject::NotVersioning(Object::builder().key("test").build());

        sender.send(object).await.unwrap();
        sender.close();

        filter_base.filter(|_, _, _| true).await.unwrap();

        let received_object = next_stage_receiver.recv().await.unwrap();

        assert_eq!(received_object.key(), "test");
    }

    #[tokio::test]
    async fn filter_false() {
        init_dummy_tracing_subscriber();

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        let cancellation_token = token::create_pipeline_cancellation_token();
        let (base, next_stage_receiver) =
            create_base_helper(receiver, cancellation_token.clone()).await;
        let filter_base = ObjectFilterBase {
            base,
            target_key_map: None,
            name: "unittest",
        };
        let object = S3syncObject::NotVersioning(Object::builder().key("test").build());

        sender.send(object).await.unwrap();
        sender.close();

        filter_base.filter(|_, _, _| false).await.unwrap();

        assert!(next_stage_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn filter_canceled() {
        init_dummy_tracing_subscriber();

        let (_, receiver) = async_channel::bounded::<S3syncObject>(1000);
        let cancellation_token = token::create_pipeline_cancellation_token();
        let (base, next_stage_receiver) =
            create_base_helper(receiver, cancellation_token.clone()).await;
        let filter_base = ObjectFilterBase {
            base,
            target_key_map: None,
            name: "unittest",
        };

        cancellation_token.cancel();
        filter_base.filter(|_, _, _| false).await.unwrap();

        assert!(next_stage_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn filter_receiver_closed() {
        init_dummy_tracing_subscriber();

        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);
        let cancellation_token = token::create_pipeline_cancellation_token();
        let (base, next_stage_receiver) =
            create_base_helper(receiver, cancellation_token.clone()).await;
        let filter_base = ObjectFilterBase {
            base,
            target_key_map: None,
            name: "unittest",
        };
        let object = S3syncObject::NotVersioning(Object::builder().key("test").build());

        next_stage_receiver.close();
        sender.send(object).await.unwrap();

        filter_base.filter(|_, _, _| true).await.unwrap();
    }

    async fn create_base_helper(
        receiver: Receiver<S3syncObject>,
        cancellation_token: PipelineCancellationToken,
    ) -> (Stage, Receiver<S3syncObject>) {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let (stats_sender, _) = async_channel::unbounded();

        let storage = LocalStorageFactory::create(
            config.clone(),
            config.target.clone(),
            token::create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let (sender, next_stage_receiver) = async_channel::bounded::<S3syncObject>(1000);

        (
            Stage {
                config,
                source: None,
                target: Some(storage),
                receiver: Some(receiver),
                sender: Some(sender),
                cancellation_token,
                has_warning: Arc::new(AtomicBool::new(false)),
            },
            next_stage_receiver,
        )
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
