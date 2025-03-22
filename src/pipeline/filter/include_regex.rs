use anyhow::Result;
use async_trait::async_trait;
use tracing::debug;

use crate::config::FilterConfig;
use crate::pipeline::filter::{ObjectFilter, ObjectFilterBase};
use crate::pipeline::stage::Stage;
use crate::types::{ObjectKeyMap, S3syncObject};

pub struct IncludeRegexFilter<'a> {
    base: ObjectFilterBase<'a>,
}

const FILTER_NAME: &str = "IncludeRegexFilter";

impl IncludeRegexFilter<'_> {
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
impl ObjectFilter for IncludeRegexFilter<'_> {
    async fn filter(&self) -> Result<()> {
        self.base.filter(is_match).await
    }
}

fn is_match(object: &S3syncObject, config: &FilterConfig, _: &ObjectKeyMap) -> bool {
    let match_result = config
        .include_regex
        .as_ref()
        .unwrap()
        .is_match(object.key());

    if !match_result {
        let key = object.key();
        let delete_marker = object.is_delete_marker();
        let version_id = object.version_id();
        let include_regex = config.include_regex.as_ref().unwrap().as_str();

        debug!(
            name = FILTER_NAME,
            key = key,
            delete_marker = delete_marker,
            version_id = version_id,
            include_regex = include_regex,
            "object filtered."
        );
    }

    match_result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use crate::types::{ObjectEntry, ObjectKey};
    use aws_sdk_s3::types::Object;
    use regex::Regex;
    use tracing_subscriber::EnvFilter;

    use super::*;

    #[tokio::test]
    async fn is_match_true() {
        init_dummy_tracing_subscriber();

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_checksum_algorithm: None,
            include_regex: Some(Regex::new(r".+\.(csv|pdf)$").unwrap()),
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        let object = S3syncObject::NotVersioning(Object::builder().key("dir1/aaa.csv").build());
        assert!(is_match(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));

        let object = S3syncObject::NotVersioning(Object::builder().key("abcdefg.pdf").build());
        assert!(is_match(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));
    }

    #[tokio::test]
    async fn is_match_false() {
        init_dummy_tracing_subscriber();

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_checksum_algorithm: None,
            include_regex: Some(Regex::new(r".+\.(csv|pdf)$").unwrap()),
            exclude_regex: None,
            larger_size: None,
            smaller_size: None,
        };

        let object = S3syncObject::NotVersioning(Object::builder().key("aaa.txt").build());
        assert!(!is_match(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));

        let object = S3syncObject::NotVersioning(Object::builder().key("abcdefg").build());
        assert!(!is_match(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));
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
