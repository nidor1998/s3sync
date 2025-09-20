use anyhow::{Context, Result, anyhow};
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::types::builders::ObjectPartBuilder;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumMode, DeleteMarkerEntry, ObjectAttributes, ObjectPart,
    ObjectVersion, RequestPayer, Tagging,
};
use aws_smithy_types_convert::date_time::DateTimeExt;
use leaky_bucket::RateLimiter;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, error, info};

use crate::Config;
use crate::config::ClientConfig;
use crate::storage::checksum::AdditionalChecksum;
use crate::storage::s3::upload_manager::UploadManager;
use crate::storage::{
    Storage, StorageFactory, StorageTrait, convert_head_to_get_object_output,
    convert_to_buf_byte_stream_with_callback,
};
use crate::types::SyncStatistics::{SyncBytes, SyncSkip};
use crate::types::event_callback::{EventData, EventType};
use crate::types::token::PipelineCancellationToken;
use crate::types::{
    ObjectChecksum, ObjectVersions, S3syncObject, SseCustomerKey, StoragePath, SyncStatistics,
    clone_object_version_with_key, get_additional_checksum, is_full_object_checksum,
};

const EXPRESS_ONEZONE_STORAGE_SUFFIX: &str = "--x-s3";

mod client_builder;
mod upload_manager;

pub struct S3StorageFactory {}

#[async_trait]
impl StorageFactory for S3StorageFactory {
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        client_config: Option<ClientConfig>,
        request_payer: Option<RequestPayer>,
        rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
        has_warning: Arc<AtomicBool>,
    ) -> Storage {
        S3Storage::boxed_new(
            config,
            path,
            cancellation_token,
            stats_sender,
            Some(Arc::new(
                client_config.as_ref().unwrap().create_client().await,
            )),
            request_payer,
            rate_limit_objects_per_sec,
            rate_limit_bandwidth,
            has_warning,
        )
        .await
    }
}

#[derive(Clone)]
struct S3Storage {
    config: Config,
    bucket: String,
    prefix: String,
    cancellation_token: PipelineCancellationToken,
    client: Option<Arc<Client>>,
    request_payer: Option<RequestPayer>,
    stats_sender: Sender<SyncStatistics>,
    rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    has_warning: Arc<AtomicBool>,
    listing_worker_semaphore: Arc<Semaphore>,
}

impl S3Storage {
    #[allow(clippy::too_many_arguments)]
    async fn boxed_new(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        client: Option<Arc<Client>>,
        request_payer: Option<RequestPayer>,
        rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
        has_warning: Arc<AtomicBool>,
    ) -> Storage {
        let (bucket, prefix) = if let StoragePath::S3 { bucket, prefix } = path {
            (bucket, prefix)
        } else {
            panic!("s3 path not found")
        };

        let max_parallel_listings: usize = config.max_parallel_listings as usize;
        let storage = S3Storage {
            config,
            bucket,
            prefix,
            cancellation_token,
            client,
            request_payer,
            stats_sender,
            rate_limit_objects_per_sec,
            rate_limit_bandwidth,
            has_warning,
            listing_worker_semaphore: Arc::new(Semaphore::new(max_parallel_listings)),
        };

        Box::new(storage)
    }

    async fn aggregate_delete_markers(
        &self,
        delete_marker_entries: &[DeleteMarkerEntry],
        s3sync_object_map: &mut HashMap<String, Vec<S3syncObject>>,
    ) {
        for delete_marker in delete_marker_entries {
            // If point-in-time is set, Intermediate delete markers will be needed.
            if self.config.point_in_time.is_none() && !delete_marker.is_latest().unwrap() {
                continue;
            }

            let key_without_prefix = remove_s3_prefix(delete_marker.key().unwrap(), &self.prefix);
            if key_without_prefix.is_empty() {
                let mut event_data = EventData::new(EventType::SYNC_FILTERED);
                event_data.key = Some(delete_marker.key().unwrap().to_string());
                // skipcq: RS-W1070
                event_data.source_version_id = delete_marker.version_id.clone();
                event_data.message = Some("Key that is same as prefix is skipped.".to_string());
                self.config.event_manager.trigger_event(event_data).await;

                self.send_stats(SyncSkip {
                    key: delete_marker.key().unwrap().to_string(),
                })
                .await;

                let key = delete_marker.key().unwrap();
                debug!(key = key, "Key that is same as prefix is skipped.");

                continue;
            }

            let delete_marker_object =
                S3syncObject::clone_delete_marker_with_key(delete_marker, &key_without_prefix);

            if s3sync_object_map.get_mut(&key_without_prefix).is_none() {
                s3sync_object_map.insert(key_without_prefix.to_string(), ObjectVersions::new());
            }
            s3sync_object_map
                .get_mut(&key_without_prefix)
                .unwrap()
                .push(delete_marker_object);
        }
    }

    async fn aggregate_object_versions_and_send(
        &self,
        sender: &Sender<S3syncObject>,
        object_versions: &[ObjectVersion],
        s3sync_object_map: &mut HashMap<String, ObjectVersions>,
    ) -> Result<()> {
        let mut previous_key = "".to_string();
        for object in object_versions {
            let key_without_prefix = remove_s3_prefix(object.key().unwrap(), &self.prefix);
            if key_without_prefix.is_empty() {
                let mut event_data = EventData::new(EventType::SYNC_FILTERED);
                event_data.key = object.key().map(|k| k.to_string());
                // skipcq: RS-W1070
                event_data.source_version_id = object.version_id.clone();
                event_data.message = Some("Key that is same as prefix is skipped.".to_string());
                self.config.event_manager.trigger_event(event_data).await;

                self.send_stats(SyncSkip {
                    key: object.key().unwrap().to_string(),
                })
                .await;

                let key = object.key().unwrap();
                debug!(key = key, "Key that is same as prefix is skipped.");

                continue;
            }

            if !previous_key.is_empty() && previous_key != key_without_prefix {
                Self::send_object_versions_with_sort(
                    sender,
                    &mut s3sync_object_map.remove(&previous_key).unwrap(),
                )
                .await?;
            }

            let versioning_object =
                S3syncObject::clone_versioning_object_with_key(object, &key_without_prefix);

            if s3sync_object_map.get(&key_without_prefix).is_none() {
                s3sync_object_map.insert(key_without_prefix.to_string(), ObjectVersions::new());
            }
            s3sync_object_map
                .get_mut(&key_without_prefix)
                .unwrap()
                .push(versioning_object);

            previous_key = key_without_prefix;
        }

        Ok(())
    }

    async fn send_object_versions_with_sort(
        sender: &Sender<S3syncObject>,
        object_versions: &mut ObjectVersions,
    ) -> Result<()> {
        object_versions.sort_by(|a, b| {
            if a.is_latest() {
                return Ordering::Greater;
            }
            if b.is_latest() {
                return Ordering::Less;
            }

            a.last_modified()
                .as_nanos()
                .cmp(&b.last_modified().as_nanos())
        });

        for object in object_versions {
            debug!(
                key = object.key(),
                "list_object_versions(): sending remote object."
            );
            if let Err(e) = sender
                .send(object.clone())
                .await
                .context("async_channel::Sender::send() failed.")
            {
                return if !sender.is_closed() { Err(e) } else { Ok(()) };
            }
        }

        Ok(())
    }

    async fn exec_rate_limit_objects_per_sec(&self) {
        if self.rate_limit_objects_per_sec.is_some() {
            self.rate_limit_objects_per_sec
                .as_ref()
                .unwrap()
                .acquire(1)
                .await;
        }
    }

    fn list_objects_with_parallel<'a>(
        &'a self,
        prefix: &'a str,
        sender: &'a Sender<S3syncObject>,
        max_keys: i32,
        current_depth: usize,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let is_root_prefix = prefix.is_empty();
            let prefix = if is_root_prefix {
                self.prefix.clone()
            } else {
                prefix.to_string()
            };

            // Until max_parallel_listing_max_depth, we need to set a delimiter to "/" for parallel listing of sub-prefixes.
            let delimiter = if current_depth <= self.config.max_parallel_listing_max_depth as usize
            {
                Some("/".to_string())
            } else {
                None
            };

            let mut current_permit = Some(permit);

            debug!(
                root_prefix = self.prefix,
                prefix = prefix.as_str(),
                "Start listing objects."
            );

            let mut continuation_token = None;
            loop {
                // Listing objects is rate-limited.
                self.exec_rate_limit_objects_per_sec().await;

                let list_object_v2 = self
                    .client
                    .as_ref()
                    .unwrap()
                    .list_objects_v2()
                    .set_request_payer(self.request_payer.clone())
                    .bucket(&self.bucket)
                    .prefix(&prefix)
                    .set_delimiter(delimiter.clone())
                    .set_continuation_token(continuation_token)
                    .max_keys(max_keys);

                if self.cancellation_token.is_cancelled() {
                    debug!("list_objects() canceled.");
                    break;
                }

                let list_objects_output = list_object_v2.send().await?;

                for object in list_objects_output.contents() {
                    let key_without_prefix = remove_s3_prefix(object.key().unwrap(), &self.prefix);
                    if key_without_prefix.is_empty() {
                        let mut event_data = EventData::new(EventType::SYNC_FILTERED);
                        event_data.key = object.key().map(|k| k.to_string());
                        // skipcq: RS-W1070
                        event_data.message =
                            Some("Key that is same as prefix is skipped.".to_string());
                        self.config.event_manager.trigger_event(event_data).await;

                        self.send_stats(SyncSkip {
                            key: object.key().unwrap().to_string(),
                        })
                        .await;

                        let key = object.key().unwrap();
                        debug!(key = key, "Key that is same as prefix is skipped.");

                        continue;
                    }

                    let non_versioning_object = S3syncObject::clone_non_versioning_object_with_key(
                        object,
                        &key_without_prefix,
                    );

                    debug!(key = object.key(), "list_objects(): sending remote object.");
                    if let Err(e) = sender
                        .send(non_versioning_object.clone())
                        .await
                        .context("async_channel::Sender::send() failed.")
                    {
                        error!("Failed to send object: {}", e);
                        return if !sender.is_closed() { Err(e) } else { Ok(()) };
                    }
                }

                if let Some(common_prefixes) = list_objects_output.common_prefixes.clone() {
                    let mut join_set = JoinSet::new();
                    for common_prefix in common_prefixes {
                        if self.cancellation_token.is_cancelled() {
                            debug!("list_objects() canceled.");
                            break;
                        }

                        if let Some(sub_prefix) = common_prefix.prefix() {
                            let storage = self.clone();
                            let sub_prefix = sub_prefix.to_string();
                            let sender = sender.clone();

                            debug!(
                                root_prefix = self.prefix,
                                prefix = prefix.as_str(),
                                sub_prefix = sub_prefix.as_str(),
                                "Start listing objects in sub-prefix."
                            );

                            if let Some(permit) = current_permit {
                                drop(permit);
                                current_permit = None;
                            }

                            let new_permit = self
                                .listing_worker_semaphore
                                .clone()
                                .acquire_owned()
                                .await
                                .unwrap();

                            join_set.spawn(async move {
                                storage
                                    .list_objects_with_parallel(
                                        &sub_prefix,
                                        &sender,
                                        max_keys,
                                        current_depth + 1,
                                        new_permit,
                                    )
                                    .await
                                    .context("Failed to list objects in sub-prefix.")
                            });
                        }
                    }

                    while let Some(join_result) = join_set.join_next().await {
                        if let Err(join_error) = join_result {
                            error!("Failed to join in sub-prefix: {}", join_error);
                            self.cancellation_token.cancel();
                            return Err(anyhow!(join_error));
                        }

                        if let Err(task_error) = join_result.unwrap() {
                            error!("Failed to list objects in sub-prefix: {}", task_error);
                            self.cancellation_token.cancel();
                            return Err(anyhow!(task_error));
                        }
                    }
                }

                if !list_objects_output.is_truncated().unwrap() {
                    break;
                }

                continuation_token = list_objects_output
                    .next_continuation_token()
                    .map(|token| token.to_string());
            }

            if let Some(permit) = current_permit {
                drop(permit);
            }

            Ok(())
        })
    }
}

#[async_trait]
impl StorageTrait for S3Storage {
    fn is_local_storage(&self) -> bool {
        false
    }

    fn is_express_onezone_storage(&self) -> bool {
        is_express_onezone_storage(&self.bucket)
    }

    async fn list_objects(
        &self,
        sender: &Sender<S3syncObject>,
        max_keys: i32,
        _warn_as_error: bool,
    ) -> Result<()> {
        if self.config.max_parallel_listings > 1 {
            if !self.is_express_onezone_storage() {
                debug!(
                    "Using parallel listing with {} workers.",
                    self.config.max_parallel_listings
                );
                let permit = self
                    .listing_worker_semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap();

                return self
                    .list_objects_with_parallel("", sender, max_keys, 1, permit)
                    .await
                    .context("Failed to parallel object listing.");
            } else if self.config.allow_parallel_listings_in_express_one_zone {
                debug!(
                    "Using parallel listing with {} workers(express one zone).",
                    self.config.max_parallel_listings
                );

                let permit = self
                    .listing_worker_semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap();

                return self
                    .list_objects_with_parallel("", sender, max_keys, 1, permit)
                    .await
                    .context("Failed to parallel object listing.");
            }
        }

        debug!("Disabled parallel listing.");

        let mut continuation_token = None;
        loop {
            let list_object_v2 = self
                .client
                .as_ref()
                .unwrap()
                .list_objects_v2()
                .set_request_payer(self.request_payer.clone())
                .bucket(&self.bucket)
                .prefix(&self.prefix)
                .set_continuation_token(continuation_token)
                .max_keys(max_keys);

            if self.cancellation_token.is_cancelled() {
                debug!("list_objects() canceled.");
                break;
            }

            let list_objects_output = list_object_v2
                .send()
                .await
                .context("aws_sdk_s3::client::list_objects_v2() failed.")?;

            for object in list_objects_output.contents() {
                let key_without_prefix = remove_s3_prefix(object.key().unwrap(), &self.prefix);
                if key_without_prefix.is_empty() {
                    let mut event_data = EventData::new(EventType::SYNC_FILTERED);
                    event_data.key = object.key().map(|k| k.to_string());
                    // skipcq: RS-W1070
                    event_data.message = Some("Key that is same as prefix is skipped.".to_string());
                    self.config.event_manager.trigger_event(event_data).await;

                    self.send_stats(SyncSkip {
                        key: object.key().unwrap().to_string(),
                    })
                    .await;

                    let key = object.key().unwrap();
                    debug!(key = key, "Key that is same as prefix is skipped.");

                    continue;
                }

                let non_versioning_object =
                    S3syncObject::clone_non_versioning_object_with_key(object, &key_without_prefix);

                debug!(key = object.key(), "list_objects(): sending remote object.");
                if let Err(e) = sender
                    .send(non_versioning_object.clone())
                    .await
                    .context("async_channel::Sender::send() failed.")
                {
                    return if !sender.is_closed() { Err(e) } else { Ok(()) };
                }
            }

            if !list_objects_output.is_truncated().unwrap() {
                break;
            }

            continuation_token = list_objects_output
                .next_continuation_token()
                .map(|token| token.to_string());
        }

        Ok(())
    }

    async fn list_object_versions(
        &self,
        sender: &Sender<S3syncObject>,
        max_keys: i32,
        _warn_as_error: bool,
    ) -> Result<()> {
        let mut key_marker = None;
        let mut version_id_marker = None;

        let mut s3sync_versioning_map = HashMap::new();

        loop {
            let list_object_versions = self
                .client
                .as_ref()
                .unwrap()
                .list_object_versions()
                .set_request_payer(self.request_payer.clone())
                .bucket(&self.bucket)
                .prefix(&self.prefix)
                .set_key_marker(key_marker)
                .set_version_id_marker(version_id_marker)
                .max_keys(max_keys);

            if self.cancellation_token.is_cancelled() {
                debug!("list_object_versions() canceled.");
                break;
            }

            let list_object_versions_output = list_object_versions
                .send()
                .await
                .context("aws_sdk_s3::client::list_object_versions() failed.")?;

            self.aggregate_delete_markers(
                list_object_versions_output.delete_markers(),
                &mut s3sync_versioning_map,
            )
            .await;

            self.aggregate_object_versions_and_send(
                sender,
                list_object_versions_output.versions(),
                &mut s3sync_versioning_map,
            )
            .await?;

            if !list_object_versions_output.is_truncated().unwrap() {
                break;
            }

            key_marker = list_object_versions_output
                .next_key_marker()
                .map(|marker| marker.to_string());
            version_id_marker = list_object_versions_output
                .next_version_id_marker()
                .map(|marker| marker.to_string());
        }

        // send remaining versioning objects
        for versioning_objects in s3sync_versioning_map.values_mut() {
            Self::send_object_versions_with_sort(sender, versioning_objects).await?;
        }

        Ok(())
    }

    async fn get_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<GetObjectOutput> {
        if self.config.dry_run {
            let head_object_result = self
                .client
                .as_ref()
                .unwrap()
                .head_object()
                .set_request_payer(self.request_payer.clone())
                .bucket(&self.bucket)
                .key(generate_full_key(&self.prefix, key))
                .set_version_id(version_id)
                .set_checksum_mode(checksum_mode)
                .set_range(range)
                .set_sse_customer_algorithm(sse_c)
                .set_sse_customer_key(sse_c_key.key.clone())
                .set_sse_customer_key_md5(sse_c_key_md5)
                .send()
                .await
                .context("aws_sdk_s3::client::head_object() failed.")?;

            return Ok(convert_head_to_get_object_output(head_object_result));
        }

        let result = self
            .client
            .as_ref()
            .unwrap()
            .get_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(generate_full_key(&self.prefix, key))
            .set_version_id(version_id)
            .set_checksum_mode(checksum_mode)
            .set_range(range)
            .set_sse_customer_algorithm(sse_c)
            .set_sse_customer_key(sse_c_key.key.clone())
            .set_sse_customer_key_md5(sse_c_key_md5)
            .send()
            .await
            .context("aws_sdk_s3::client::get_object() failed.")?;

        Ok(result)
    }

    async fn get_object_versions(&self, key: &str, max_keys: i32) -> Result<Vec<ObjectVersion>> {
        let mut key_marker = None;
        let mut version_id_marker = None;

        let mut object_versions = Vec::new();

        let key = generate_full_key(&self.prefix, key);
        let key_without_prefix = remove_s3_prefix(&key, &self.prefix);

        loop {
            let list_object_versions = self
                .client
                .as_ref()
                .unwrap()
                .list_object_versions()
                .set_request_payer(self.request_payer.clone())
                .bucket(&self.bucket)
                .prefix(&key)
                .set_key_marker(key_marker)
                .set_version_id_marker(version_id_marker)
                .max_keys(max_keys);

            if self.cancellation_token.is_cancelled() {
                debug!("list_object_versions() canceled.");
                break;
            }

            let list_object_versions_output = list_object_versions
                .send()
                .await
                .context("aws_sdk_s3::client::list_object_versions() failed.")?;

            object_versions.append(
                &mut list_object_versions_output
                    .versions()
                    .iter()
                    .filter(|&object| object.key().unwrap() == key)
                    .cloned()
                    .map(|object| clone_object_version_with_key(&object, &key_without_prefix))
                    .collect(),
            );

            if !list_object_versions_output.is_truncated().unwrap() {
                break;
            }

            key_marker = list_object_versions_output
                .next_key_marker()
                .map(|marker| marker.to_string());
            version_id_marker = list_object_versions_output
                .next_version_id_marker()
                .map(|marker| marker.to_string());
        }

        Ok(object_versions)
    }

    async fn get_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
    ) -> Result<GetObjectTaggingOutput> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .get_object_tagging()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(generate_full_key(&self.prefix, key))
            .set_version_id(version_id)
            .send()
            .await
            .context("aws_sdk_s3::client::get_object_tagging() failed.")?;

        Ok(result)
    }

    async fn head_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .head_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(generate_full_key(&self.prefix, key))
            .set_range(range)
            .set_version_id(version_id)
            .set_checksum_mode(checksum_mode)
            .set_sse_customer_algorithm(sse_c)
            .set_sse_customer_key(sse_c_key.key.clone())
            .set_sse_customer_key_md5(sse_c_key_md5)
            .send()
            .await
            .context("aws_sdk_s3::client::head_object() failed.")?;

        Ok(result)
    }

    async fn head_object_first_part(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .head_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(generate_full_key(&self.prefix, key))
            .set_version_id(version_id)
            .part_number(1)
            .set_checksum_mode(checksum_mode)
            .set_sse_customer_algorithm(sse_c)
            .set_sse_customer_key(sse_c_key.key.clone())
            .set_sse_customer_key_md5(sse_c_key_md5)
            .send()
            .await
            .context("aws_sdk_s3::client::head_object() failed.")?;

        Ok(result)
    }

    async fn get_object_parts(
        &self,
        key: &str,
        version_id: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        let object = self
            .client
            .as_ref()
            .unwrap()
            .head_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(generate_full_key(&self.prefix, key))
            .set_version_id(version_id.clone())
            .part_number(1)
            .set_sse_customer_algorithm(sse_c.clone())
            .set_sse_customer_key(sse_c_key.key.clone())
            .set_sse_customer_key_md5(sse_c_key_md5.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::head_object() failed.")?;

        let mut object_parts = vec![];

        let parts_count = object.parts_count().unwrap_or_default();
        if parts_count == 0 {
            return Ok(vec![]);
        }

        object_parts.push(
            ObjectPartBuilder::default()
                .size(object.content_length().unwrap())
                .build(),
        );

        for part_number in 2..=parts_count {
            let object = self
                .client
                .as_ref()
                .unwrap()
                .head_object()
                .set_request_payer(self.request_payer.clone())
                .bucket(&self.bucket)
                .key(generate_full_key(&self.prefix, key))
                .set_version_id(version_id.clone())
                .part_number(part_number)
                .set_sse_customer_algorithm(sse_c.clone())
                .set_sse_customer_key(sse_c_key.key.clone())
                .set_sse_customer_key_md5(sse_c_key_md5.clone())
                .send()
                .await
                .context("aws_sdk_s3::client::head_object() failed.")?;

            object_parts.push(
                ObjectPartBuilder::default()
                    .size(object.content_length().unwrap())
                    .build(),
            );
        }

        Ok(object_parts)
    }

    async fn get_object_parts_attributes(
        &self,
        key: &str,
        version_id: Option<String>,
        max_parts: i32,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        let mut object_parts = vec![];
        let mut part_number_marker = None;
        loop {
            let object = self
                .client
                .as_ref()
                .unwrap()
                .get_object_attributes()
                .set_request_payer(self.request_payer.clone())
                .bucket(&self.bucket)
                .key(generate_full_key(&self.prefix, key))
                .set_version_id(version_id.clone())
                .object_attributes(ObjectAttributes::ObjectParts)
                .set_part_number_marker(part_number_marker)
                .set_sse_customer_algorithm(sse_c.clone())
                .set_sse_customer_key(sse_c_key.key.clone())
                .set_sse_customer_key_md5(sse_c_key_md5.clone())
                .max_parts(max_parts)
                .send()
                .await
                .context("aws_sdk_s3::client::get_object_attributes() failed.")?;

            // A full object checksum has empty object parts.
            if object.object_parts().is_none() || object.object_parts().unwrap().parts().is_empty()
            {
                return Ok(vec![]);
            }

            for part in object.object_parts().unwrap().parts() {
                object_parts.push(part.clone());
            }

            if !object.object_parts().unwrap().is_truncated().unwrap() {
                break;
            }

            part_number_marker = object
                .object_parts()
                .unwrap()
                .next_part_number_marker()
                .map(|marker| marker.to_string());
        }

        Ok(object_parts)
    }

    async fn put_object(
        &self,
        key: &str,
        source: Storage,
        source_size: u64,
        source_additional_checksum: Option<String>,
        mut get_object_output_first_chunk: GetObjectOutput,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
        if_match: Option<String>,
        copy_source_if_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        let mut version_id = "".to_string();
        if let Some(source_version_id) = get_object_output_first_chunk.version_id().as_ref() {
            version_id = source_version_id.to_string();
        }
        let target_key = generate_full_key(&self.prefix, key);
        let source_key = key;
        let source_last_modified = aws_smithy_types::DateTime::from_millis(
            get_object_output_first_chunk
                .last_modified()
                .unwrap()
                .to_millis()?,
        )
        .to_chrono_utc()?
        .to_rfc3339();

        if self.config.dry_run {
            self.send_stats(SyncBytes(source_size)).await;

            info!(
                key = key,
                source_version_id = version_id,
                source_last_modified = source_last_modified,
                target_key = target_key,
                size = source_size.to_string(),
                if_match = if_match.clone(),
                copy_source_if_match = copy_source_if_match.clone(),
                "[dry-run] sync completed.",
            );

            return Ok(PutObjectOutput::builder().build());
        }

        // In the case of full object checksum, we don't need to calculate checksum for each part and
        // don't need to pass it to the upload manager.
        let additional_checksum_value = get_additional_checksum(
            &get_object_output_first_chunk,
            object_checksum.as_ref().unwrap().checksum_algorithm.clone(),
        );
        let full_object_checksum = is_full_object_checksum(&additional_checksum_value);
        #[allow(clippy::unnecessary_unwrap)]
        let checksum = if object_checksum.is_some()
            && object_checksum
                .as_ref()
                .unwrap()
                .checksum_algorithm
                .is_some()
            && !self.config.full_object_checksum
            && !full_object_checksum
        {
            Some(Arc::new(AdditionalChecksum::new(
                object_checksum
                    .as_ref()
                    .unwrap()
                    .checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .clone(),
                self.config.full_object_checksum,
            )))
        } else {
            None
        };

        get_object_output_first_chunk.body = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            self.get_stats_sender(),
            self.rate_limit_bandwidth.clone(),
            checksum,
            object_checksum.clone(),
        );

        let mut upload_manager = UploadManager::new(
            self.client.clone().unwrap(),
            self.config.clone(),
            self.request_payer.clone(),
            self.cancellation_token.clone(),
            self.get_stats_sender(),
            tagging,
            object_checksum.unwrap_or_default().object_parts,
            self.is_express_onezone_storage(),
            source,
            source_key.to_string(),
            source_size,
            source_additional_checksum,
            if_match,
            copy_source_if_match,
            self.has_warning.clone(),
        );

        self.exec_rate_limit_objects_per_sec().await;

        let put_object_output = upload_manager
            .upload(&self.bucket, &target_key, get_object_output_first_chunk)
            .await?;

        // If preprocess_callback is registered and preprocess was cancelled, e_tag will be None.
        if put_object_output.e_tag.is_some() {
            info!(
                key = key,
                source_version_id = version_id,
                source_last_modified = source_last_modified,
                target_key = target_key,
                size = source_size,
                "sync completed.",
            );
        }

        Ok(put_object_output)
    }

    async fn put_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
        tagging: Tagging,
    ) -> Result<PutObjectTaggingOutput> {
        let target_key = generate_full_key(&self.prefix, key);
        let version_id_str = version_id.clone().unwrap_or_default();

        if self.config.dry_run {
            info!(
                key = key,
                target_version_id = version_id_str,
                target_key = target_key,
                "[dry-run] sync(tagging only) completed.",
            );

            return Ok(PutObjectTaggingOutput::builder().build());
        }

        self.exec_rate_limit_objects_per_sec().await;

        let result = self
            .client
            .as_ref()
            .unwrap()
            .put_object_tagging()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(&target_key)
            .set_version_id(version_id.clone())
            .tagging(tagging)
            .send()
            .await
            .context("aws_sdk_s3::client::put_object_tagging() failed.")?;

        info!(
            key = key,
            target_version_id = version_id_str,
            target_key = target_key,
            "sync(tagging only) completed.",
        );

        Ok(result)
    }

    async fn delete_object(
        &self,
        key: &str,
        version_id: Option<String>,
        if_match: Option<String>,
    ) -> Result<DeleteObjectOutput> {
        let target_key = generate_full_key(&self.prefix, key);
        let version_id_str = version_id.clone().unwrap_or_default();

        if self.config.dry_run {
            info!(
                key = key,
                target_version_id = version_id_str,
                target_key = target_key,
                if_match = if_match.clone(),
                "[dry-run] delete completed.",
            );

            return Ok(DeleteObjectOutput::builder().build());
        }

        self.exec_rate_limit_objects_per_sec().await;

        let result = self
            .client
            .as_ref()
            .unwrap()
            .delete_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(&target_key)
            .set_version_id(version_id.clone())
            .set_if_match(if_match.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::delete_object() failed.")?;

        info!(
            key = key,
            target_version_id = version_id_str,
            target_key = target_key,
            if_match = if_match.clone(),
            "delete completed.",
        );

        Ok(result)
    }

    async fn delete_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
    ) -> Result<DeleteObjectTaggingOutput> {
        let target_key = generate_full_key(&self.prefix, key);
        let version_id_str = version_id.clone().unwrap_or_default();

        if self.config.dry_run {
            info!(
                key = key,
                target_version_id = version_id_str,
                target_key = target_key,
                "[dry-run] sync(delete tagging only) completed.",
            );

            return Ok(DeleteObjectTaggingOutput::builder().build());
        }

        self.exec_rate_limit_objects_per_sec().await;

        let result = self
            .client
            .as_ref()
            .unwrap()
            .delete_object_tagging()
            .bucket(&self.bucket)
            .key(&target_key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::delete_object_tagging() failed.")?;

        info!(
            key = key,
            target_version_id = version_id_str,
            target_key = target_key,
            "sync(delete tagging only) completed.",
        );

        Ok(result)
    }

    async fn is_versioning_enabled(&self) -> Result<bool> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .get_bucket_versioning()
            .bucket(&self.bucket)
            .send()
            .await
            .context("aws_sdk_s3::client::get_bucket_versioning() failed.")?;

        if result.status().is_none() {
            return Ok(false);
        }

        Ok(*result.status().unwrap() == BucketVersioningStatus::Enabled)
    }

    fn get_client(&self) -> Option<Arc<Client>> {
        self.client.clone()
    }

    fn get_stats_sender(&self) -> Sender<SyncStatistics> {
        self.stats_sender.clone()
    }

    async fn send_stats(&self, stats: SyncStatistics) {
        let _ = self.stats_sender.send(stats).await;
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    fn get_local_path(&self) -> PathBuf {
        // S3 storage does not have a local path.
        unimplemented!();
    }

    fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
        self.rate_limit_bandwidth.clone()
    }

    fn generate_copy_source_key(&self, key: &str, version_id: Option<String>) -> String {
        let full_key = generate_full_key(&self.prefix, key);
        let full_key = urlencoding::encode(&full_key);

        if version_id.is_some() {
            return format!(
                "{}/{}?versionId={}",
                &self.bucket,
                full_key,
                version_id.unwrap()
            );
        }
        format!("{}/{}", &self.bucket, full_key)
    }

    fn set_warning(&self) {
        self.has_warning
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

pub fn remove_s3_prefix(key: &str, prefix: &str) -> String {
    key.to_string().replacen(prefix, "", 1)
}

pub fn generate_full_key(prefix: &str, key: &str) -> String {
    format!("{prefix}{key}")
}

fn is_express_onezone_storage(bucket: &str) -> bool {
    bucket.ends_with(EXPRESS_ONEZONE_STORAGE_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::parse_from_args;
    use crate::types::token::create_pipeline_cancellation_token;
    use tracing_subscriber::EnvFilter;

    #[test]
    fn remove_s3_prefix_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(remove_s3_prefix("dir1/data1", "dir1/data1"), "");

        assert_eq!(remove_s3_prefix("dir1/data1", "dir1"), "/data1");
        assert_eq!(remove_s3_prefix("dir1/data1", "dir1/"), "data1");
        assert_eq!(remove_s3_prefix("/dir1/data1", "/dir1"), "/data1");
        assert_eq!(remove_s3_prefix("/dir1/data1", "/dir1/"), "data1");
    }

    #[test]
    fn is_express_onezone_storage_test() {
        init_dummy_tracing_subscriber();

        assert!(is_express_onezone_storage("bucket--x-s3"));

        assert!(!is_express_onezone_storage("bucket-x-s3"));
        assert!(!is_express_onezone_storage("bucket--x-s3s"));
        assert!(!is_express_onezone_storage("bucket"));
    }

    #[tokio::test]
    async fn create_storage() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "s3://source-bucket",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = S3StorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        assert!(storage.get_client().is_some());
    }

    #[tokio::test]
    async fn get_object_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--dry-run",
            "--target-access-key",
            "dummy_access_key",
            "--target-secret-access-key",
            "dummy_secret_access_key",
            "--aws-max-attempts",
            "1",
            "--target-endpoint-url",
            "https://invalid-s3-endpoint-url.6329313.local:65535",
            "--force-retry-count",
            "1",
            "--force-retry-interval-milliseconds",
            "1",
            "./test_data/",
            "s3://dummy-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = S3StorageFactory::create(
            config.clone(),
            config.target.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.target_client_config.clone(),
            None,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        assert!(
            storage
                .get_object(
                    "source/data1",
                    None,
                    None,
                    None,
                    None,
                    SseCustomerKey { key: None },
                    None,
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn is_local_storage() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "s3://source-bucket",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage = S3StorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        assert!(storage.get_client().is_some());
        assert!(!storage.is_local_storage());
    }

    #[tokio::test]
    #[should_panic]
    async fn create_storage_panic() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "/source-dir",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        S3StorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
    }

    #[tokio::test]
    async fn stats_channel_test() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "s3://source-bucket",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, stats_receiver) = async_channel::unbounded();

        let storage = S3StorageFactory::create(
            config.clone(),
            config.source.clone(),
            create_pipeline_cancellation_token(),
            stats_sender,
            config.source_client_config.clone(),
            None,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let stats_sender = storage.get_stats_sender();

        stats_sender.send(SyncBytes(0)).await.unwrap();
        assert_eq!(stats_receiver.recv().await.unwrap(), SyncBytes(0));
    }

    #[tokio::test]
    async fn generate_full_key_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(generate_full_key("dir1/", "data1"), "dir1/data1");
        assert_eq!(generate_full_key("dir1", "data1"), "dir1data1");

        assert_eq!(generate_full_key("data1", "data1"), "data1data1");
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
