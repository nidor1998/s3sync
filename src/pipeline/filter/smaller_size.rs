use anyhow::Result;
use async_trait::async_trait;
use tracing::debug;

use crate::config::FilterConfig;
use crate::pipeline::filter::{ObjectFilter, ObjectFilterBase};
use crate::pipeline::stage::Stage;
use crate::types::{ObjectKeyMap, S3syncObject};

pub struct SmallerSizeFilter<'a> {
    base: ObjectFilterBase<'a>,
}

const FILTER_NAME: &str = "SmallerSizeFilter";

impl SmallerSizeFilter<'_> {
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
impl ObjectFilter for SmallerSizeFilter<'_> {
    async fn filter(&self) -> Result<()> {
        self.base.filter(is_smaller).await
    }
}

fn is_smaller(object: &S3syncObject, config: &FilterConfig, _: &ObjectKeyMap) -> bool {
    if object.is_delete_marker() {
        return true;
    }

    if object.size() >= config.smaller_size.unwrap() as i64 {
        let key = object.key();
        let content_length = object.size();
        let delete_marker = object.is_delete_marker();
        let version_id = object.version_id();
        let config_size = config.smaller_size.unwrap();

        debug!(
            name = FILTER_NAME,
            key = key,
            content_length = content_length,
            delete_marker = delete_marker,
            version_id = version_id,
            config_size = config_size,
            "object filtered."
        );
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::callback::filter_manager::FilterManager;
    use crate::types::{ObjectEntry, ObjectKey};
    use aws_sdk_s3::types::{DeleteMarkerEntry, Object};
    use tracing_subscriber::EnvFilter;

    #[tokio::test]
    async fn larger() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(Object::builder().key("test").size(6).build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_mtime_and_etag: false,
            check_checksum_algorithm: None,
            check_mtime_and_additional_checksum: None,
            include_regex: None,
            exclude_regex: None,
            include_content_type_regex: None,
            exclude_content_type_regex: None,
            include_metadata_regex: None,
            exclude_metadata_regex: None,
            include_tag_regex: None,
            exclude_tag_regex: None,
            larger_size: None,
            smaller_size: Some(5),
            filter_manager: FilterManager::new(),
        };

        assert!(!is_smaller(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::<ObjectKey, ObjectEntry>::new()))
        ));
    }

    #[tokio::test]
    async fn smaller() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(Object::builder().size(4).build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_mtime_and_etag: false,
            check_checksum_algorithm: None,
            check_mtime_and_additional_checksum: None,
            include_regex: None,
            exclude_regex: None,
            include_content_type_regex: None,
            exclude_content_type_regex: None,
            include_metadata_regex: None,
            exclude_metadata_regex: None,
            include_tag_regex: None,
            exclude_tag_regex: None,
            larger_size: None,
            smaller_size: Some(5),
            filter_manager: FilterManager::new(),
        };

        assert!(is_smaller(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
        ));
    }

    #[tokio::test]
    async fn equal() {
        init_dummy_tracing_subscriber();

        let object = S3syncObject::NotVersioning(Object::builder().key("test").size(5).build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_mtime_and_etag: false,
            check_checksum_algorithm: None,
            check_mtime_and_additional_checksum: None,
            include_regex: None,
            exclude_regex: None,
            include_content_type_regex: None,
            exclude_content_type_regex: None,
            include_metadata_regex: None,
            exclude_metadata_regex: None,
            include_tag_regex: None,
            exclude_tag_regex: None,
            larger_size: None,
            smaller_size: Some(5),
            filter_manager: FilterManager::new(),
        };

        assert!(!is_smaller(
            &object,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
        ));
    }

    #[test]
    fn delete_marker() {
        init_dummy_tracing_subscriber();

        let delete_marker =
            S3syncObject::DeleteMarker(DeleteMarkerEntry::builder().key("test").build());

        let config = FilterConfig {
            before_time: None,
            after_time: None,
            remove_modified_filter: false,
            check_size: false,
            check_etag: false,
            check_mtime_and_etag: false,
            check_checksum_algorithm: None,
            check_mtime_and_additional_checksum: None,
            include_regex: None,
            exclude_regex: None,
            include_content_type_regex: None,
            exclude_content_type_regex: None,
            include_metadata_regex: None,
            exclude_metadata_regex: None,
            include_tag_regex: None,
            exclude_tag_regex: None,
            larger_size: None,
            smaller_size: Some(5),
            filter_manager: FilterManager::new(),
        };

        assert!(is_smaller(
            &delete_marker,
            &config,
            &ObjectKeyMap::new(Mutex::new(HashMap::new()))
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
