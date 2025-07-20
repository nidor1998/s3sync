use std::collections::HashSet;

use anyhow::Result;
use aws_sdk_s3::types::Object;
use tracing::trace;

use crate::types::{ObjectKey, ObjectKeyMap, S3syncObject};

use super::stage::{SendResult, Stage};

pub struct DiffLister {
    base: Stage,
}

impl DiffLister {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn list(
        &self,
        source_key_map: &ObjectKeyMap,
        target_key_map: &ObjectKeyMap,
    ) -> Result<()> {
        trace!("diff generator has started.");

        let diff_set = generate_diff(source_key_map, target_key_map);

        for key in diff_set {
            if self.base.cancellation_token.is_cancelled() {
                trace!("list() canceled.");
                break;
            }

            let object = S3syncObject::NotVersioning(
                Object::builder().set_key(Some(key.to_string())).build(),
            );

            if self.base.send(object).await? == SendResult::Closed {
                return Ok(());
            }
        }
        trace!("diff list has been completed.");

        Ok(())
    }
}

fn generate_diff(source_key_map: &ObjectKeyMap, target_key_map: &ObjectKeyMap) -> HashSet<String> {
    let source_key_map = source_key_map.lock().unwrap();
    let source_key_set: HashSet<&ObjectKey> = HashSet::from_iter(source_key_map.keys());

    let target_key_map = target_key_map.as_ref().lock().unwrap();
    let target_key_set: HashSet<&ObjectKey> = HashSet::from_iter(target_key_map.keys());

    let diff_to_delete = target_key_set.difference(&source_key_set);

    let mut diff_set = HashSet::new();
    for object_entry in diff_to_delete {
        if let ObjectKey::KeyString(key) = object_entry {
            diff_set.insert(key.to_string());
        } else {
            panic!("No KeyString found")
        }
    }

    diff_set
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use crate::types::{ObjectEntry, sha1_digest_from_key};
    use aws_sdk_s3::primitives::DateTime;
    use tracing_subscriber::EnvFilter;

    use super::*;

    #[test]
    fn generate_diff_test() {
        init_dummy_tracing_subscriber();

        let mut source_key_map = HashMap::new();
        source_key_map.insert(
            ObjectKey::KeyString("key1".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
                e_tag: None,
            },
        );
        source_key_map.insert(
            ObjectKey::KeyString("key3".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
                e_tag: None,
            },
        );
        source_key_map.insert(
            ObjectKey::KeyString("key5".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
                e_tag: None,
            },
        );

        let mut target_key_map = source_key_map.clone();
        target_key_map.insert(
            ObjectKey::KeyString("key2".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
                e_tag: None,
            },
        );
        target_key_map.insert(
            ObjectKey::KeyString("key4".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
                e_tag: None,
            },
        );

        let diff_set = generate_diff(
            &ObjectKeyMap::new(Mutex::new(source_key_map)),
            &ObjectKeyMap::new(Mutex::new(target_key_map)),
        );

        let expected_set = HashSet::from(["key2".to_string(), "key4".to_string()]);
        assert_eq!(diff_set, expected_set);
    }

    #[test]
    #[should_panic]
    fn generate_diff_panic_test() {
        init_dummy_tracing_subscriber();

        let mut source_key_map = HashMap::new();
        source_key_map.insert(
            ObjectKey::KeyString("key1".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
                e_tag: None,
            },
        );

        let mut target_key_map = source_key_map.clone();
        target_key_map.insert(
            ObjectKey::KeySHA1Digest(sha1_digest_from_key("test")),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
                e_tag: None,
            },
        );

        generate_diff(
            &ObjectKeyMap::new(Mutex::new(source_key_map)),
            &ObjectKeyMap::new(Mutex::new(target_key_map)),
        );
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
