use anyhow::Result;
use log::error;
use tracing::trace;

use crate::types::{sha1_digest_from_key, ObjectEntry, ObjectKey, ObjectKeyMap, S3syncObject};

use super::stage::Stage;

pub struct KeyAggregator {
    base: Stage,
}

impl KeyAggregator {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn aggregate(&self, key_map: &ObjectKeyMap) -> Result<()> {
        let sha1_digest_required = self.base.config.is_sha1_digest_listing_required();

        loop {
            tokio::select! {
                result = self.base.receiver.as_ref().unwrap().recv() => {
                    match result {
                        Ok(object) => {
                            insert_key(&object, key_map, sha1_digest_required);

                            if let Err(e) = self.base.send(object).await {
                                return if !self.base.is_channel_closed() {
                                    Err(e)
                                } else {
                                    Ok(())
                                };
                            }
                        },
                        Err(_) => {
                            trace!("key aggregator has been completed.");
                            return Ok(());
                        }
                    }
                },
                _ = self.base.cancellation_token.cancelled() => {
                    trace!("key aggregator has been cancelled.");
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
        let message = format!("key already exists in the key map: {}", object.key());

        error!("{}", message);
        panic!("{}", message);
    }
}

fn build_object_key_entry(object: &S3syncObject) -> ObjectEntry {
    ObjectEntry {
        last_modified: *object.last_modified(),
        content_length: object.size(),
        etag: object.e_tag().map(|etag| etag.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;

    use crate::config::args::parse_from_args;
    use crate::pipeline::key_aggregator::{build_object_key_entry, insert_key, KeyAggregator};
    use crate::pipeline::stage::Stage;
    use crate::types::token::create_pipeline_cancellation_token;
    use crate::types::{sha1_digest_from_key, ObjectKey, ObjectKeyMap, S3syncObject};
    use crate::Config;

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
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
