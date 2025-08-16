use std::collections::HashMap;

use anyhow::{Context, Result};
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::ObjectVersion;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::debug;

use crate::storage::Storage;
use crate::types::SyncStatistics::SyncSkip;
use crate::types::event_callback::{EventData, EventType};
use crate::types::{ObjectVersions, S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY, S3syncObject};
use crate::{Config, types};

pub struct VersioningInfoCollector {
    config: Config,
    worker_index: u16,
    target: Storage,
}

type HeadObjectOutputMap = HashMap<String, HeadObjectOutput>;

impl VersioningInfoCollector {
    pub fn new(config: Config, target: Storage, worker_index: u16) -> Self {
        Self {
            config,
            target,
            worker_index,
        }
    }

    pub async fn collect_object_versions_to_sync(
        &self,
        source_packed_object_versions: &S3syncObject,
    ) -> Result<ObjectVersions> {
        let source_object_versions = types::unpack_object_versions(source_packed_object_versions);

        // If point-in-time is set, we do not need to check the target storage.
        if self.config.point_in_time.is_some() {
            let mut object_versions_to_sync = ObjectVersions::new();
            for source_object in source_object_versions {
                object_versions_to_sync.push(source_object);
            }

            return Ok(object_versions_to_sync);
        }

        let key = source_packed_object_versions.key();
        let target_object_versions = self
            .target
            .get_object_versions(key, self.config.max_keys)
            .await?;

        let target_latest_version_deleted = is_latest_version_deleted(&target_object_versions);

        let target_head_object_output_map = self
            .build_head_object_output_map(&target_object_versions)
            .await?;

        let mut object_versions_to_sync = ObjectVersions::new();
        for source_object in source_object_versions {
            if let S3syncObject::DeleteMarker(marker) = &source_object {
                // delete marker is always at the end of the array
                if !target_latest_version_deleted || !object_versions_to_sync.is_empty() {
                    object_versions_to_sync.push(source_object);
                } else {
                    let source_version_id = marker.version_id().unwrap();
                    let source_last_modified =
                        DateTime::from_millis(source_object.last_modified().to_millis()?)
                            .to_chrono_utc()?
                            .to_rfc3339();
                    debug!(
                        worker_index = self.worker_index,
                        key = key,
                        source_version_id = source_version_id,
                        source_last_modified = source_last_modified,
                        "latest version in the target storage has already been deleted."
                    );
                }
                continue;
            }

            let source_version_id = source_object.version_id().unwrap();
            if does_not_contain_version_id(&target_head_object_output_map, source_version_id) {
                object_versions_to_sync.push(source_object);
            } else {
                let source_last_modified =
                    DateTime::from_millis(source_object.last_modified().to_millis()?)
                        .to_chrono_utc()?
                        .to_rfc3339();

                debug!(
                    worker_index = self.worker_index,
                    key = key,
                    source_version_id = source_version_id,
                    source_last_modified = source_last_modified,
                    "already synced."
                );

                let mut event_data = EventData::new(EventType::SYNC_FILTERED);
                event_data.key = Some(key.to_string());
                // skipcq: RS-W1070
                event_data.source_version_id = source_object.version_id().map(|v| v.to_string());
                event_data.source_last_modified = Some(*source_object.last_modified());
                event_data.source_size = Some(source_object.size() as u64);
                event_data.message =
                    Some("Object filtered. This version already synced.".to_string());
                self.config.event_manager.trigger_event(event_data).await;

                let _ = self
                    .target
                    .get_stats_sender()
                    .send(SyncSkip {
                        key: key.to_string(),
                    })
                    .await;
            }
        }

        Ok(object_versions_to_sync)
    }

    async fn build_head_object_output_map(
        &self,
        target_object_versions: &Vec<ObjectVersion>,
    ) -> Result<HeadObjectOutputMap> {
        let mut head_object_output_map = HashMap::new();
        for object in target_object_versions {
            let target_version_id = object.version_id().unwrap().to_string();

            let head_object_output = self
                .target
                .head_object(
                    object.key().unwrap(),
                    Some(target_version_id),
                    None,
                    None,
                    self.config.target_sse_c.clone(),
                    self.config.target_sse_c_key.clone(),
                    self.config.target_sse_c_key_md5.clone(),
                )
                .await
                .context("head_object() failed.")?;
            if let Some(metadata) = head_object_output.metadata() {
                if let Some(source_version_id) = metadata.get(S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY)
                {
                    head_object_output_map
                        .insert(source_version_id.to_string(), head_object_output);
                }
            }
        }

        Ok(head_object_output_map)
    }
}

fn is_latest_version_deleted(object_versions: &Vec<ObjectVersion>) -> bool {
    for object in object_versions {
        if object.is_latest().unwrap() {
            return false;
        }
    }
    true
}

fn does_not_contain_version_id(
    head_object_output_map: &HeadObjectOutputMap,
    version_id: &str,
) -> bool {
    head_object_output_map.get(version_id).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::primitives::DateTime;
    use tracing_subscriber::EnvFilter;

    #[test]
    fn is_latest_version_deleted_true() {
        init_dummy_tracing_subscriber();

        let object_versions = vec![
            ObjectVersion::builder().is_latest(false).build(),
            ObjectVersion::builder().is_latest(false).build(),
            ObjectVersion::builder().is_latest(false).build(),
            ObjectVersion::builder().is_latest(false).build(),
        ];

        assert!(is_latest_version_deleted(&object_versions));
    }

    #[test]
    fn is_latest_version_deleted_false() {
        init_dummy_tracing_subscriber();

        let object_versions = vec![
            ObjectVersion::builder().is_latest(true).build(),
            ObjectVersion::builder().is_latest(false).build(),
            ObjectVersion::builder().is_latest(false).build(),
            ObjectVersion::builder().is_latest(false).build(),
        ];

        assert!(!is_latest_version_deleted(&object_versions));
    }

    #[test]
    fn does_not_contain_version_id_true() {
        init_dummy_tracing_subscriber();

        let mut object_versions_map = HeadObjectOutputMap::new();
        object_versions_map.insert(
            "version1".to_string(),
            HeadObjectOutput::builder()
                .version_id("version1")
                .last_modified(DateTime::from_secs(10))
                .build(),
        );
        object_versions_map.insert(
            "version2".to_string(),
            HeadObjectOutput::builder()
                .version_id("version2")
                .last_modified(DateTime::from_secs(11))
                .build(),
        );
        object_versions_map.insert(
            "version3".to_string(),
            HeadObjectOutput::builder()
                .version_id("version3")
                .last_modified(DateTime::from_secs(11))
                .build(),
        );

        assert!(does_not_contain_version_id(
            &object_versions_map,
            "version4"
        ));
        assert!(does_not_contain_version_id(
            &object_versions_map,
            "version5"
        ));
        assert!(does_not_contain_version_id(
            &object_versions_map,
            "version6"
        ));
    }

    #[test]
    fn does_not_contain_version_id_false() {
        init_dummy_tracing_subscriber();

        let mut object_versions_map = HeadObjectOutputMap::new();
        object_versions_map.insert(
            "version1".to_string(),
            HeadObjectOutput::builder()
                .version_id("version1")
                .last_modified(DateTime::from_secs(10))
                .build(),
        );
        object_versions_map.insert(
            "version2".to_string(),
            HeadObjectOutput::builder()
                .version_id("version2")
                .last_modified(DateTime::from_secs(11))
                .build(),
        );
        object_versions_map.insert(
            "version3".to_string(),
            HeadObjectOutput::builder()
                .version_id("version3")
                .last_modified(DateTime::from_secs(11))
                .build(),
        );

        assert!(!does_not_contain_version_id(
            &object_versions_map,
            "version1"
        ));
        assert!(!does_not_contain_version_id(
            &object_versions_map,
            "version2"
        ));
        assert!(!does_not_contain_version_id(
            &object_versions_map,
            "version3"
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
