use super::stage::{SendResult, Stage};
use crate::types::SyncStatistics::{SyncDelete, SyncWarning};
use crate::types::event_callback::{EventData, EventType};
use crate::types::{ObjectKey, ObjectKeyMap};
use anyhow::{Error, Result, anyhow};
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, error, info, warn};

pub struct ObjectDeleter {
    worker_index: u16,
    base: Stage,
    target_key_map: Option<ObjectKeyMap>,
    delete_counter: Arc<AtomicU64>,
}

impl ObjectDeleter {
    pub fn new(
        base: Stage,
        worker_index: u16,
        target_key_map: Option<ObjectKeyMap>,
        delete_counter: Arc<AtomicU64>,
    ) -> Self {
        Self {
            base,
            worker_index,
            target_key_map,
            delete_counter,
        }
    }

    pub async fn delete_target(&self) -> Result<()> {
        debug!(
            worker_index = self.worker_index,
            "delete target objects process started."
        );
        self.receive_and_delete().await
    }

    async fn receive_and_delete(&self) -> Result<()> {
        loop {
            // This is special for test emulation.
            #[allow(clippy::collapsible_if)]
            if cfg!(feature = "e2e_test_dangerous_simulations") {
                panic_simulation(&self.base.config, "ObjectDeleter::receive_and_filter");

                if is_error_simulation_point(&self.base.config, "ObjectDeleter::receive_and_filter")
                {
                    error!("error simulation point has been triggered.");
                    return Err(anyhow::anyhow!(
                        "error simulation point has been triggered."
                    ));
                }
            }

            tokio::select! {
                recv_result = self.base.receiver.as_ref().unwrap().recv() => {
                    match recv_result {
                        Ok(object) => {
                            // The delete operation is an async function, so we increase the counter before calling it.
                            self.delete_counter.fetch_add(1, Ordering::SeqCst);

                            let deleted_count = self.delete_counter.load(Ordering::SeqCst);
                            if self.base.config.max_delete.is_some() && deleted_count >= self.base.config.max_delete.unwrap() {
                                self.base.send_stats(SyncWarning {
                                    key: object.key().to_string().to_string()}).await;
                                self.base.set_warning();

                                let message = "--max_delete has been reached. delete operation has been cancelled.";
                                warn!(
                                    key = object.key().to_string(),
                                    message
                                );

                                let mut event_data = EventData::new(EventType::SYNC_WARNING);
                                    event_data.key = Some(object.key().to_string());
                                    event_data.message = Some(message.to_string());
                                    self.base
                                        .config
                                        .event_manager
                                        .trigger_event(event_data)
                                        .await;

                                self.base.cancellation_token.cancel();

                                break;
                            }

                            let target_etag = if self.base.config.if_match {
                                self.get_etag_from_target_key_map(object.key())
                            } else {
                                None
                            };

                            let result = self.delete(object.key(), None, target_etag.clone()).await;
                            if result.is_err() {
                                if is_precondition_failed_error(result.as_ref().err().unwrap()) {
                                    let error = result.err().unwrap();
                                    self.base
                                        .send_stats(SyncWarning {
                                            key: object.key().to_string().to_string(),
                                        })
                                        .await;
                                    self.base.set_warning();

                                    let message = "delete precondition(if-match/copy-source-if-match) failed. skipping.";
                                    warn!(
                                        key = object.key().to_string(),
                                        if_match = target_etag,
                                        error = error.to_string(),
                                        source = &error.source(),
                                        message
                                    );

                                    let mut event_data = EventData::new(EventType::SYNC_WARNING);
                                    event_data.key = Some(object.key().to_string());
                                    event_data.source_version_id =
                                        object.version_id().map(|version_id| version_id.to_string());
                                    event_data.message = Some(message.to_string());
                                    self.base
                                        .config
                                        .event_manager
                                        .trigger_event(event_data)
                                        .await;

                                    if self.base.config.warn_as_error {
                                        self.base.cancellation_token.cancel();
                                        error!(worker_index = self.worker_index, "delete worker has been cancelled with error.");
                                        return Err(error);
                                    }

                                    continue;
                                }

                                self.base.cancellation_token.cancel();

                                let error = result.err().unwrap();
                                error!(worker_index = self.worker_index,
                                    error = error.to_string(),
                                    source = &error.source(),
                                    "delete worker has been cancelled with error.");
                                return Err(anyhow!("delete worker has been cancelled with error."));
                            }

                            let mut event_data = EventData::new(EventType::SYNC_DELETE);
                            event_data.key = Some(object.key().to_string());
                            self.base.config
                                .event_manager
                                .trigger_event(event_data.clone())
                                .await;

                            if self.base.send(object).await? == SendResult::Closed {
                                return Ok(());
                            }
                        },
                        Err(_) => {
                            // normal shutdown
                            debug!(worker_index = self.worker_index, "delete worker has been completed.");
                            break;
                        }
                    }
                },
                _ = self.base.cancellation_token.cancelled() => {
                    info!(worker_index = self.worker_index, "delete worker has been cancelled.");
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn delete(
        &self,
        key: &str,
        version_id: Option<String>,
        if_match: Option<String>,
    ) -> Result<()> {
        #[cfg(feature = "e2e_test_dangerous_simulations")]
        {
            self.do_precondition_error_simulation("ObjectDeleter::delete-precondition")?;
        }

        self.base
            .target
            .as_ref()
            .unwrap()
            .delete_object(key, version_id, if_match)
            .await?;

        self.base
            .send_stats(SyncDelete {
                key: key.to_string(),
            })
            .await;

        Ok(())
    }

    pub fn get_etag_from_target_key_map(&self, key: &str) -> Option<String> {
        if self.base.config.enable_versioning {
            panic!("get_etag_from_target_key_map is not supported when versioning is enabled");
        }

        if let Some(target_key_map) = self.target_key_map.as_ref() {
            let target_key_map_map = target_key_map.lock().unwrap();
            let result = target_key_map_map.get(&ObjectKey::KeyString(key.to_string()));
            if let Some(entry) = result {
                return entry.e_tag.clone();
            }
        }

        None
    }

    #[allow(dead_code)]
    fn do_precondition_error_simulation(&self, error_simulation_point: &str) -> Result<()> {
        const ERROR_DANGEROUS_SIMULATION_ENV: &str = "S3SYNC_ERROR_DANGEROUS_SIMULATION";
        const ERROR_DANGEROUS_SIMULATION_ENV_ALLOW: &str = "ALLOW";

        if std::env::var(ERROR_DANGEROUS_SIMULATION_ENV)
            .is_ok_and(|v| v == ERROR_DANGEROUS_SIMULATION_ENV_ALLOW)
            && self
                .base
                .config
                .error_simulation_point
                .as_ref()
                .is_some_and(|point| point == error_simulation_point)
        {
            error!(
                "precondition error simulation has been triggered. This message should not be shown in the production.",
            );

            let unhandled_error = DeleteObjectError::generic(
                aws_sdk_s3::error::ErrorMetadata::builder()
                    .code("PreconditionFailed")
                    .build(),
            );

            let response = Response::new(StatusCode::try_from(412).unwrap(), SdkBody::from(r#""#));

            return Err(anyhow!(SdkError::service_error(unhandled_error, response)));
        }

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

fn is_precondition_failed_error(result: &Error) -> bool {
    if let Some(SdkError::ServiceError(e)) =
        result.downcast_ref::<SdkError<DeleteObjectError, Response<SdkBody>>>()
    {
        if let Some(code) = e.err().meta().code() {
            return code == "PreconditionFailed";
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_runtime_api::http::StatusCode;

    fn build_delete_object_precondition_failed_error()
    -> SdkError<DeleteObjectError, Response<SdkBody>> {
        let unhandled_error = DeleteObjectError::generic(
            aws_sdk_s3::error::ErrorMetadata::builder()
                .code("PreconditionFailed")
                .build(),
        );

        let response = Response::new(StatusCode::try_from(412).unwrap(), SdkBody::from(r#""#));

        SdkError::service_error(unhandled_error, response)
    }

    #[test]
    fn is_precondition_failed_error_test() {
        assert!(is_precondition_failed_error(&anyhow!(
            build_delete_object_precondition_failed_error()
        )));
    }
}
