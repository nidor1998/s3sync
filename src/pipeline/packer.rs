use anyhow::Result;
use tracing::trace;

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
