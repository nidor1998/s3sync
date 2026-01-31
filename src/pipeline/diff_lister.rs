use std::collections::HashSet;

use anyhow::Result;
use aws_sdk_s3::types::Object;
use tracing::{debug, error};

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
        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(e2e_test_dangerous_simulations) {
            panic_simulation(&self.base.config, "DiffLister::receive_and_filter");

            if is_error_simulation_point(&self.base.config, "DiffLister::receive_and_filter") {
                error!("error simulation point has been triggered.");
                return Err(anyhow::anyhow!(
                    "error simulation point has been triggered."
                ));
            }
        }

        debug!("diff generator has started.");

        let diff_set = generate_diff(source_key_map, target_key_map);

        for key in diff_set {
            if self.base.cancellation_token.is_cancelled() {
                debug!("list() canceled.");
                break;
            }

            // We don't build a filter pipeline Because a filter pipeline is too complex for this operation.
            if !self.base.config.delete_excluded
                && self.base.config.filter_config.exclude_regex.is_some()
            {
                let match_result = self
                    .base
                    .config
                    .filter_config
                    .exclude_regex
                    .as_ref()
                    .unwrap()
                    .is_match(&key)?;

                if match_result {
                    let exclude_regex = self
                        .base
                        .config
                        .filter_config
                        .exclude_regex
                        .as_ref()
                        .unwrap()
                        .as_str();

                    debug!(
                        key = key,
                        exclude_regex = exclude_regex,
                        "delete object filtered."
                    );

                    continue;
                }
            }

            let object = S3syncObject::NotVersioning(
                Object::builder().set_key(Some(key.to_string())).build(),
            );

            if self.base.send(object).await? == SendResult::Closed {
                return Ok(());
            }
        }
        debug!("diff list has been completed.");

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
