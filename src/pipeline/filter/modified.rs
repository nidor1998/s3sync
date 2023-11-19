use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::primitives::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::debug;

use crate::config::FilterConfig;
use crate::pipeline::filter::{ObjectFilter, ObjectFilterBase};
use crate::pipeline::stage::Stage;
use crate::types::{sha1_digest_from_key, ObjectKey, ObjectKeyMap, S3syncObject};

pub struct TargetModifiedFilter<'a> {
    base: ObjectFilterBase<'a>,
}

const FILTER_NAME: &str = "TargetModifiedFilter";

impl TargetModifiedFilter<'_> {
    pub fn new(base: Stage, target_key_map: Option<ObjectKeyMap>) -> Self {
        Self {
            base: ObjectFilterBase {
                base,
                target_key_map,
                name: FILTER_NAME,
            },
        }
    }
}

#[async_trait]
impl ObjectFilter for TargetModifiedFilter<'_> {
    async fn filter(&self) -> Result<()> {
        if self.base.base.config.filter_config.check_size {
            self.base.filter(is_modified_from_size).await
        } else {
            self.base.filter(is_modified_from_timestamp).await
        }
    }
}

fn is_modified_from_timestamp(
    object: &S3syncObject,
    _: &FilterConfig,
    target_key_map: &ObjectKeyMap,
) -> bool {
    let target_key_map_map = target_key_map.lock().unwrap();
    let key = object.key();
    let source_last_modified_date = object.last_modified();

    let result = target_key_map_map.get(&ObjectKey::KeySHA1Digest(sha1_digest_from_key(key)));
    if let Some(entry) = result {
        return filter_last_modified(key, source_last_modified_date, &entry.last_modified);
    }

    let result = target_key_map_map.get(&ObjectKey::KeyString(key.to_string()));
    if let Some(entry) = result {
        return filter_last_modified(key, source_last_modified_date, &entry.last_modified);
    }

    true
}

fn filter_last_modified(
    key: &str,
    source_last_modified_date: &DateTime,
    target_last_modified_date: &DateTime,
) -> bool {
    let modified =
        is_source_last_modified_date_newer(source_last_modified_date, target_last_modified_date);
    if !modified {
        let source_last_modified =
            aws_smithy_types::DateTime::from_millis(source_last_modified_date.to_millis().unwrap())
                .to_chrono_utc()
                .unwrap()
                .to_rfc3339();
        let target_last_modified =
            aws_smithy_types::DateTime::from_millis(target_last_modified_date.to_millis().unwrap())
                .to_chrono_utc()
                .unwrap()
                .to_rfc3339();

        debug!(
            name = FILTER_NAME,
            source_last_modified = source_last_modified,
            target_last_modified = target_last_modified,
            key = key,
            "object filtered."
        );
    }

    modified
}

fn is_source_last_modified_date_newer(
    source_last_modified_date: &DateTime,
    target_last_modified_date: &DateTime,
) -> bool {
    // GetObjectOutput doesn't have nanos
    target_last_modified_date.secs() < source_last_modified_date.secs()
}

fn is_modified_from_size(
    object: &S3syncObject,
    _: &FilterConfig,
    target_key_map: &ObjectKeyMap,
) -> bool {
    let target_key_map_map = target_key_map.lock().unwrap();
    let key = object.key();

    let result = target_key_map_map.get(&ObjectKey::KeySHA1Digest(sha1_digest_from_key(key)));
    if let Some(entry) = result {
        let modified = entry.content_length != object.size();
        if !modified {
            debug!(
                name = FILTER_NAME,
                content_length = entry.content_length,
                key = key,
                "object filtered."
            );
        }
        return modified;
    }

    let result = target_key_map_map.get(&ObjectKey::KeyString(key.to_string()));
    if let Some(entry) = result {
        let modified = entry.content_length != object.size();
        if !modified {
            debug!(
                name = FILTER_NAME,
                content_length = entry.content_length,
                key = key,
                "object filtered."
            );
        }
        return modified;
    }

    true
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;

    use crate::config::FilterConfig;
    use crate::types::{ObjectEntry, S3syncObject};

    use super::*;

    #[tokio::test]
    async fn not_modified_sha1() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .last_modified(DateTime::from_secs(1))
                .build(),
        );

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        assert!(is_modified_from_timestamp(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
        ));
    }

    #[tokio::test]
    async fn modified_sha1() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .last_modified(DateTime::from_secs(1))
                .build(),
        );

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        let mut key_map = HashMap::new();
        key_map.insert(
            ObjectKey::KeySHA1Digest(sha1_digest_from_key("test")),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
            },
        );

        assert!(!is_modified_from_timestamp(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(key_map))
        ));
    }

    #[tokio::test]
    async fn is_modified_true() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .last_modified(DateTime::from_secs(1))
                .build(),
        );

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        assert!(is_modified_from_timestamp(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
        ));
    }

    #[tokio::test]
    async fn is_modified_false() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .last_modified(DateTime::from_secs(1))
                .build(),
        );

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        let mut key_map = HashMap::new();
        key_map.insert(
            ObjectKey::KeyString("test".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(1),
                content_length: 1,
            },
        );

        assert!(!is_modified_from_timestamp(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(key_map))
        ));
    }

    #[test]
    fn filter_modified_false() {
        init_dummy_tracing_subscriber();

        assert!(!filter_last_modified(
            "key",
            &DateTime::from_secs(0),
            &DateTime::from_secs(1)
        ));
        assert!(!filter_last_modified(
            "key",
            &DateTime::from_secs(1),
            &DateTime::from_secs(1)
        ));
    }

    #[test]
    fn filter_modified_true() {
        init_dummy_tracing_subscriber();

        assert!(filter_last_modified(
            "key",
            &DateTime::from_secs(1),
            &DateTime::from_secs(0)
        ));
    }

    #[tokio::test]
    async fn size_modified_sha1_empty() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(Object::builder().key("test").size(1).build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: true,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        assert!(is_modified_from_size(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
        ));
    }

    #[tokio::test]
    async fn size_not_modified_sha1() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(Object::builder().key("test").size(1).build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: true,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        let mut key_map = HashMap::new();
        key_map.insert(
            ObjectKey::KeySHA1Digest(sha1_digest_from_key("test")),
            ObjectEntry {
                last_modified: DateTime::from_secs(99),
                content_length: 1,
            },
        );

        assert!(!is_modified_from_size(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(key_map))
        ));
    }

    #[tokio::test]
    async fn size_modified_sha1() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(Object::builder().key("test").size(1).build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: true,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        let mut key_map = HashMap::new();
        key_map.insert(
            ObjectKey::KeySHA1Digest(sha1_digest_from_key("test")),
            ObjectEntry {
                last_modified: DateTime::from_secs(99),
                content_length: 2,
            },
        );

        assert!(is_modified_from_size(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(key_map))
        ));
    }

    #[tokio::test]
    async fn size_not_modified_key() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(Object::builder().key("test").size(1).build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: true,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        let mut key_map = HashMap::new();
        key_map.insert(
            ObjectKey::KeyString("test".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(99),
                content_length: 1,
            },
        );

        assert!(!is_modified_from_size(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(key_map))
        ));
    }

    #[tokio::test]
    async fn size_modified_key() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(Object::builder().key("test").size(1).build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: true,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        let mut key_map = HashMap::new();
        key_map.insert(
            ObjectKey::KeyString("test".to_string()),
            ObjectEntry {
                last_modified: DateTime::from_secs(99),
                content_length: 2,
            },
        );

        assert!(is_modified_from_size(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(key_map))
        ));
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
