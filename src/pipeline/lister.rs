use anyhow::Result;
use tracing::{debug, error};

use super::stage::Stage;

pub struct ObjectLister {
    base: Stage,
}

impl ObjectLister {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn list_source(&self, max_keys: i32) -> Result<()> {
        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(e2e_test_dangerous_simulations) {
            panic_simulation(&self.base.config, "ObjectLister::list_source");

            if is_error_simulation_point(&self.base.config, "ObjectLister::list_source") {
                error!("error simulation point has been triggered.");
                return Err(anyhow::anyhow!(
                    "error simulation point has been triggered."
                ));
            }
        }

        debug!("list source objects has started.");

        if self.base.config.enable_versioning || self.base.config.point_in_time.is_some() {
            self.base
                .source
                .as_ref()
                .unwrap()
                .list_object_versions(
                    self.base.sender.as_ref().unwrap(),
                    max_keys,
                    self.base.config.warn_as_error,
                )
                .await?;
        } else {
            self.base
                .source
                .as_ref()
                .unwrap()
                .list_objects(
                    self.base.sender.as_ref().unwrap(),
                    max_keys,
                    self.base.config.warn_as_error,
                )
                .await?;
        }

        debug!("list source objects has been completed.");
        Ok(())
    }

    pub async fn list_target(&self, max_keys: i32) -> Result<()> {
        // This is special for test emulation.
        #[allow(clippy::collapsible_if)]
        if cfg!(e2e_test_dangerous_simulations) {
            panic_simulation(&self.base.config, "ObjectLister::list_target");

            if is_error_simulation_point(&self.base.config, "ObjectLister::list_target") {
                error!("error simulation point has been triggered.");
                return Err(anyhow::anyhow!(
                    "error simulation point has been triggered."
                ));
            }
        }

        debug!("list target objects has started.");
        self.base
            .target
            .as_ref()
            .unwrap()
            .list_objects(
                self.base.sender.as_ref().unwrap(),
                max_keys,
                self.base.config.warn_as_error,
            )
            .await?;
        debug!("list target objects has been completed.");
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
