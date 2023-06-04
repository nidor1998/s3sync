use anyhow::Result;
use tracing::trace;

use super::stage::Stage;

pub struct ObjectLister {
    base: Stage,
}

impl ObjectLister {
    pub fn new(base: Stage) -> Self {
        Self { base }
    }

    pub async fn list_source(&self, max_keys: i32) -> Result<()> {
        trace!("list source objects has started.");

        if self.base.config.enable_versioning {
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

        trace!("list source objects has been completed.");
        Ok(())
    }

    pub async fn list_target(&self, max_keys: i32) -> Result<()> {
        trace!("list target objects has started.");
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
        trace!("list target objects has been completed.");
        Ok(())
    }
}
