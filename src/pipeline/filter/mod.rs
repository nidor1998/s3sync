use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use tracing::trace;

use crate::config::FilterConfig;
pub use crate::pipeline::filter::exclude_regex::ExcludeRegexFilter;
pub use crate::pipeline::filter::include_regex::IncludeRegexFilter;
pub use crate::pipeline::filter::larger_size::LargerSizeFilter;
pub use crate::pipeline::filter::modified::TargetModifiedFilter;
pub use crate::pipeline::filter::mtime_after::MtimeAfterFilter;
pub use crate::pipeline::filter::mtime_before::MtimeBeforeFilter;
pub use crate::pipeline::filter::smaller_size::SmallerSizeFilter;
use crate::types::{ObjectKeyMap, S3syncObject, SyncStatistics};

use super::stage::Stage;

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

        loop {
            if self.base.cancellation_token.is_cancelled() {
                trace!(name = self.name, "filter has been cancelled.");
                return Ok(());
            }

            match self.base.receiver.as_ref().unwrap().recv().await {
                Ok(object) => {
                    if !filter_fn(&object, &self.base.config.filter_config, &target_key_map) {
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

                    return self.base.send(object).await;
                }
                Err(_) => {
                    trace!(name = self.name, "filter has been completed.");
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::args::parse_from_args;
    use crate::storage::local::LocalStorageFactory;
    use crate::storage::StorageFactory;
    use crate::types::token;
    use crate::types::token::PipelineCancellationToken;
    use crate::Config;
    use async_channel::Receiver;
    use aws_sdk_s3::types::Object;
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
