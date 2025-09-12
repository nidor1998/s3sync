use anyhow::Result;
use tracing::{error, trace};

use crate::types;
use crate::types::ObjectVersions;

use super::stage::Stage;

pub struct ObjectVersionsPacker {
    base: Stage,
}

impl ObjectVersionsPacker {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn pack(&self) -> Result<()> {
        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(feature = "e2e_test_dangerous_simulations") {
            panic_simulation(
                &self.base.config,
                "ObjectVersionsPacker::receive_and_filter",
            );

            if is_error_simulation_point(
                &self.base.config,
                "ObjectVersionsPacker::receive_and_filter",
            ) {
                error!("error simulation point has been triggered.");
                return Err(anyhow::anyhow!(
                    "error simulation point has been triggered."
                ));
            }
        }

        trace!("object versions packer has started.");

        let mut object_versions = ObjectVersions::new();
        let mut previous_key = "".to_string();

        loop {
            tokio::select! {
                result = self.base.receiver.as_ref().unwrap().recv() => {
                    match result {
                        Ok(object) => {
                            let key = object.key().to_string();
                            if !previous_key.is_empty() && previous_key != key {
                                self.send_object_versions(&previous_key, &object_versions).await?;

                                object_versions.clear()
                            }

                            object_versions.push(object);
                            previous_key = key;
                        },
                        Err(_) => {
                            if !object_versions.is_empty() {
                                let key = object_versions[0].key().to_string();
                                self.send_object_versions(&key, &object_versions).await?;
                            }

                            trace!("object versions packer has been completed.");
                            return Ok(());
                        }
                    }
                },
                _ = self.base.cancellation_token.cancelled() => {
                    trace!("object versions packer has been cancelled.");
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
