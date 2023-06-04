use anyhow::Result;
use async_trait::async_trait;
use tracing::debug;

use crate::config::FilterConfig;
use crate::pipeline::filter::{ObjectFilter, ObjectFilterBase};
use crate::pipeline::stage::Stage;
use crate::types::{ObjectKeyMap, S3syncObject};

pub struct ExcludeRegexFilter<'a> {
    base: ObjectFilterBase<'a>,
}

const FILTER_NAME: &str = "ExcludeRegexFilter";

impl ExcludeRegexFilter<'_> {
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
impl ObjectFilter for ExcludeRegexFilter<'_> {
    async fn filter(&self) -> Result<()> {
        self.base.filter(is_not_match).await
    }
}

fn is_not_match(object: &S3syncObject, config: &FilterConfig, _: &ObjectKeyMap) -> bool {
    let match_result = config
        .exclude_regex
        .as_ref()
        .unwrap()
        .is_match(object.key());

    if match_result {
        let key = object.key();
        let delete_marker = object.is_delete_marker();
        let version_id = object.version_id();
        let exclude_regex = config.exclude_regex.as_ref().unwrap().as_str();

        debug!(
            name = FILTER_NAME,
            key = key,
            delete_marker = delete_marker,
            version_id = version_id,
            exclude_regex = exclude_regex,
            "object filtered."
        );
    }

    !match_result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use aws_sdk_s3::types::Object;
    use regex::Regex;

    use crate::types::{ObjectEntry, ObjectKey};

    use super::*;

    #[tokio::test]
    async fn is_not_match_true() {
        init_dummy_tracing_subscriber();

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            include_regex: None,
            exclude_regex: Some(Regex::new(r".+\.(csv|pdf)$").unwrap()),
            larger_size: None,
            smaller_size: None,
        };

        let object = S3syncObject::NotVersioning(Object::builder().key("dir1/aaa.txt").build());
        assert!(is_not_match(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));

        let object = S3syncObject::NotVersioning(Object::builder().key("abcdefg.docx").build());
        assert!(is_not_match(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));
    }

    #[tokio::test]
    async fn is_not_match_false() {
        init_dummy_tracing_subscriber();

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            include_regex: None,
            exclude_regex: Some(Regex::new(r".+\.(csv|pdf)$").unwrap()),
            larger_size: None,
            smaller_size: None,
        };

        let object = S3syncObject::NotVersioning(Object::builder().key("aaa.csv").build());
        assert!(!is_not_match(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));

        let object = S3syncObject::NotVersioning(Object::builder().key("abcdefg.pdf").build());
        assert!(!is_not_match(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
