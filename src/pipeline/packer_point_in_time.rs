use anyhow::Result;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::trace;

use crate::types;
use crate::types::ObjectVersions;

use super::stage::Stage;

pub struct ObjectPointInTimePacker {
    base: Stage,
}

const FILTER_NAME: &str = "ObjectPointInTimePacker";

impl ObjectPointInTimePacker {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn pack(&self) -> Result<()> {
        trace!("object point-in-time packer has started.");

        let mut object_versions = ObjectVersions::new();
        let mut previous_key = "".to_string();

        loop {
            tokio::select! {
                result = self.base.receiver.as_ref().unwrap().recv() => {
                    match result {
                        Ok(object) => {
                            let key = object.key().to_string();

                            if !previous_key.is_empty() && previous_key != key {
                                if !object_versions.is_empty() {
                                    self.send_object_versions(&previous_key, &object_versions).await?;
                                }
                                object_versions.clear()
                            }

                            let last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
                                object.last_modified().to_millis()?,
                            ))?;

                            let version_id = if !object.is_delete_marker() {
                                object.version_id().unwrap().to_string()
                            } else {
                                "".to_string()
                            };

                            if self.base.config.point_in_time.unwrap() < last_modified {
                                trace!(
                                    name = FILTER_NAME,
                                    key = key,
                                    version_id = version_id,
                                    delete_marker = object.is_delete_marker(),
                                    last_modified = last_modified.to_rfc3339(),
                                    point_in_time = self.base.config.point_in_time.unwrap().to_rfc3339(),
                                    "object is filtered out by point-in-time."
                                );
                                continue;
                            }

                            object_versions.clear();
                            if object.is_delete_marker() {
                                trace!(
                                    name = FILTER_NAME,
                                    key = key,
                                    version_id = version_id,
                                    delete_marker = object.is_delete_marker(),
                                    last_modified = last_modified.to_rfc3339(),
                                    point_in_time = self.base.config.point_in_time.unwrap().to_rfc3339(),
                                    "object is a delete marker and will not be included in the versions."
                                );
                            } else {
                                object_versions.push(object);
                            }

                            previous_key = key;
                        },
                        Err(_) => {
                            if !object_versions.is_empty() {
                                let key = object_versions[0].key().to_string();
                                self.send_object_versions(&key, &object_versions).await?;
                            }

                            trace!("object point-in-time packer has been completed.");
                            return Ok(());
                        }
                    }
                },
                _ = self.base.cancellation_token.cancelled() => {
                    trace!("object point-in-time packer has been cancelled.");
                    return Ok(());
                }
            }
        }
    }

    async fn send_object_versions(
        &self,
        key: &str,
        object_versions: &ObjectVersions,
    ) -> Result<()> {
        let packed_versions = types::pack_object_versions(key, object_versions);

        self.base.send(packed_versions).await?;
        Ok(())
    }
}
