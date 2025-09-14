use anyhow::Result;
use tracing::{debug, error};

use crate::types::{ObjectEntry, ObjectKey, ObjectKeyMap, S3syncObject, sha1_digest_from_key};

use super::stage::{SendResult, Stage};

pub struct KeyAggregator {
    base: Stage,
}

impl KeyAggregator {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn aggregate(&self, key_map: &ObjectKeyMap) -> Result<()> {
        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(feature = "e2e_test_dangerous_simulations") {
            panic_simulation(&self.base.config, "KeyAggregator::aggregate");

            if is_error_simulation_point(&self.base.config, "KeyAggregator::aggregate") {
                error!("error simulation point has been triggered.");
                return Err(anyhow::anyhow!(
                    "error simulation point has been triggered."
                ));
            }
        }

        let sha1_digest_required = self.base.config.is_sha1_digest_listing_required();

        loop {
            tokio::select! {
                result = self.base.receiver.as_ref().unwrap().recv() => {
                    match result {
                        Ok(object) => {
                            insert_key(&object, key_map, sha1_digest_required);
                            if self.base.send(object).await? == SendResult::Closed {
                                return Ok(());
                            }
                        },
                        Err(_) => {
                            debug!("key aggregator has been completed.");
                            return Ok(());
                        }
                    }
                },
                _ = self.base.cancellation_token.cancelled() => {
                    debug!("key aggregator has been cancelled.");
                    return Ok(());
                }
            }
        }
    }
}

fn insert_key(object: &S3syncObject, key_map: &ObjectKeyMap, sha1_digest_required: bool) {
    let object_key = if sha1_digest_required {
        ObjectKey::KeySHA1Digest(sha1_digest_from_key(object.key()))
    } else {
        ObjectKey::KeyString(object.key().to_string())
    };

    let previous_value = key_map
        .lock()
        .unwrap()
        .insert(object_key, build_object_key_entry(object));

    if previous_value.is_some() {
        panic!(
            "{}",
            format!("key already exists in the key map: {}", object.key())
        );
    }
}

fn build_object_key_entry(object: &S3syncObject) -> ObjectEntry {
    ObjectEntry {
        last_modified: *object.last_modified(),
        content_length: object.size(),
        e_tag: object.e_tag().map(|e_tag| e_tag.to_string()),
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
    use crate::pipeline::key_aggregator::{KeyAggregator, build_object_key_entry, insert_key};
    use crate::pipeline::stage::Stage;
    use crate::types::token::create_pipeline_cancellation_token;
    use crate::types::{ObjectKey, ObjectKeyMap, S3syncObject, sha1_digest_from_key};
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::EnvFilter;

    #[test]
    fn build_object_key_entry_test() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test-key")
                .size(1)
                .last_modified(DateTime::from_secs(1))
                .build(),
        );

        let object_entry = build_object_key_entry(&object);
        assert_eq!(object_entry.last_modified, DateTime::from_secs(1));
    }

    #[test]
    fn insert_key_sha1_test() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test-key1")
                .size(1)
                .last_modified(DateTime::from_secs(1))
                .build(),
        );
        let key_map = ObjectKeyMap::new(Mutex::new(HashMap::new()));

        insert_key(&object, &key_map, true);

        key_map
            .lock()
            .unwrap()
            .get(&ObjectKey::KeySHA1Digest(sha1_digest_from_key("test-key1")))
            .unwrap();
    }

    #[test]
    fn insert_key_string_test() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test-key1")
                .size(1)
                .last_modified(DateTime::from_secs(1))
                .build(),
        );
        let key_map = ObjectKeyMap::new(Mutex::new(HashMap::new()));

        insert_key(&object, &key_map, false);

        key_map
            .lock()
            .unwrap()
            .get(&ObjectKey::KeyString("test-key1".to_string()))
            .unwrap();
    }

    #[tokio::test]
    async fn aggregate_sha1_test() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (previous_stage_sender, previous_stage_receiver) =
            async_channel::bounded::<S3syncObject>(1000);
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        let base = Stage {
            config,
            source: None,
            target: None,
            receiver: Some(previous_stage_receiver),
            sender: Some(sender),
            cancellation_token,
            has_warning: Arc::new(AtomicBool::new(false)),
        };

        let key_aggregator = KeyAggregator { base };
        let key_map = ObjectKeyMap::new(Mutex::new(HashMap::new()));

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test-key1")
                .size(1)
                .last_modified(DateTime::from_secs(1))
                .build(),
        );
        previous_stage_sender.send(object).await.unwrap();
        previous_stage_sender.close();

        key_aggregator.aggregate(&key_map).await.unwrap();

        assert_eq!(key_map.lock().unwrap().len(), 1);
        assert_eq!(receiver.len(), 1);
        key_map
            .lock()
            .unwrap()
            .get(&ObjectKey::KeySHA1Digest(sha1_digest_from_key("test-key1")))
            .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_sha1_test_panic() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (previous_stage_sender, previous_stage_receiver) =
            async_channel::bounded::<S3syncObject>(1000);
        let (sender, _receiver) = async_channel::bounded::<S3syncObject>(1000);

        let base = Stage {
            config,
            source: None,
            target: None,
            receiver: Some(previous_stage_receiver),
            sender: Some(sender),
            cancellation_token,
            has_warning: Arc::new(AtomicBool::new(false)),
        };

        let key_aggregator = KeyAggregator { base };
        let key_map = ObjectKeyMap::new(Mutex::new(HashMap::new()));

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test-key1")
                .last_modified(DateTime::from_secs(1))
                .build(),
        );
        previous_stage_sender.send(object.clone()).await.unwrap();
        previous_stage_sender.send(object.clone()).await.unwrap();
        previous_stage_sender.close();

        key_aggregator.aggregate(&key_map).await.unwrap();
    }

    #[tokio::test]
    async fn aggregate_string_test() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--delete",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (previous_stage_sender, previous_stage_receiver) =
            async_channel::bounded::<S3syncObject>(1000);
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        let base = Stage {
            config,
            source: None,
            target: None,
            receiver: Some(previous_stage_receiver),
            sender: Some(sender),
            cancellation_token,
            has_warning: Arc::new(AtomicBool::new(false)),
        };

        let key_aggregator = KeyAggregator { base };
        let key_map = ObjectKeyMap::new(Mutex::new(HashMap::new()));

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test-key1")
                .size(1)
                .last_modified(DateTime::from_secs(1))
                .build(),
        );
        previous_stage_sender.send(object).await.unwrap();
        previous_stage_sender.close();

        key_aggregator.aggregate(&key_map).await.unwrap();

        assert_eq!(key_map.lock().unwrap().len(), 1);
        assert_eq!(receiver.len(), 1);
        key_map
            .lock()
            .unwrap()
            .get(&ObjectKey::KeyString("test-key1".to_string()))
            .unwrap();
    }

    #[tokio::test]
    async fn aggregate_cancel_test() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--delete",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let (_previous_stage_sender, previous_stage_receiver) =
            async_channel::bounded::<S3syncObject>(1000);
        let (sender, receiver) = async_channel::bounded::<S3syncObject>(1000);

        let base = Stage {
            config,
            source: None,
            target: None,
            receiver: Some(previous_stage_receiver),
            sender: Some(sender),
            cancellation_token: cancellation_token.clone(),
            has_warning: Arc::new(AtomicBool::new(false)),
        };

        let key_aggregator = KeyAggregator { base };
        let key_map = ObjectKeyMap::new(Mutex::new(HashMap::new()));

        cancellation_token.cancel();
        key_aggregator.aggregate(&key_map).await.unwrap();

        assert_eq!(key_map.lock().unwrap().len(), 0);
        assert_eq!(receiver.len(), 0);
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
