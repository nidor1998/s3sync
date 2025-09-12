use anyhow::Result;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::{error, trace};

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
        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(feature = "e2e_test_dangerous_simulations") {
            panic_simulation(
                &self.base.config,
                "ObjectPointInTimePacker::receive_and_filter",
            );

            if is_error_simulation_point(
                &self.base.config,
                "ObjectPointInTimePacker::receive_and_filter",
            ) {
                error!("error simulation point has been triggered.");
                return Err(anyhow::anyhow!(
                    "error simulation point has been triggered."
                ));
            }
        }

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

#[cfg_attr(coverage_nightly, coverage(off))]
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

#[cfg_attr(coverage_nightly, coverage(off))]
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
