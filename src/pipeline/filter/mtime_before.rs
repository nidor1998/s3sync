use anyhow::Result;
use async_trait::async_trait;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::debug;

use crate::config::FilterConfig;
use crate::pipeline::filter::{ObjectFilter, ObjectFilterBase};
use crate::pipeline::stage::Stage;
use crate::types::{ObjectKeyMap, S3syncObject};

pub struct MtimeBeforeFilter<'a> {
    base: ObjectFilterBase<'a>,
}
const FILTER_NAME: &str = "MtimeBeforeFilter";

impl MtimeBeforeFilter<'_> {
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
impl ObjectFilter for MtimeBeforeFilter<'_> {
    async fn filter(&self) -> Result<()> {
        self.base.filter(is_before).await
    }
}

fn is_before(object: &S3syncObject, config: &FilterConfig, _: &ObjectKeyMap) -> bool {
    let last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
        object.last_modified().to_millis().unwrap(),
    ))
    .unwrap();

    if config.before_time.unwrap() <= last_modified {
        let key = object.key();
        let delete_marker = object.is_delete_marker();
        let version_id = object.version_id();
        let last_modified = last_modified.to_rfc3339();
        let config_time = config.before_time.unwrap().to_rfc3339();

        debug!(
            name = FILTER_NAME,
            key = key,
            delete_marker = delete_marker,
            version_id = version_id,
            last_modified = last_modified,
            config_time = config_time,
            "object filtered."
        );

        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Mutex;

    use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
    use aws_sdk_s3::types::Object;

    use super::*;

    #[test]
    fn before() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .last_modified(
                    DateTime::from_str("2023-01-20T00:00:00.000Z", DateTimeFormat::DateTime)
                        .unwrap(),
                )
                .build(),
        );

        let config = FilterConfig {
            before_time: Some(chrono::DateTime::from_str("2023-01-20T00:00:00.001Z").unwrap()),
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_checksum_algorithm: None,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        assert!(is_before(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
        ));
    }

    #[test]
    fn after() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .last_modified(
                    DateTime::from_str("2023-01-20T00:00:00.002Z", DateTimeFormat::DateTime)
                        .unwrap(),
                )
                .build(),
        );

        let config = FilterConfig {
            before_time: Some(chrono::DateTime::from_str("2023-01-20T00:00:00.001Z").unwrap()),
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_checksum_algorithm: None,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        assert!(!is_before(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
        ));
    }

    #[test]
    fn equal() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(
            Object::builder()
                .key("test")
                .last_modified(
                    DateTime::from_str("2023-01-20T00:00:00.001Z", DateTimeFormat::DateTime)
                        .unwrap(),
                )
                .build(),
        );

        let config = FilterConfig {
            before_time: Some(chrono::DateTime::from_str("2023-01-20T00:00:00.001Z").unwrap()),
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_checksum_algorithm: None,
            include_regex: None,
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        assert!(!is_before(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
        ));
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
