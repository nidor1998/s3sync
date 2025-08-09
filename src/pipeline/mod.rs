use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Error, anyhow};
use async_channel::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{error, trace};

use crate::Config;
use crate::pipeline::deleter::ObjectDeleter;
use crate::pipeline::diff_lister::DiffLister;
use crate::pipeline::filter::{ExcludeRegexFilter, IncludeRegexFilter, ObjectFilter};
use crate::pipeline::key_aggregator::KeyAggregator;
use crate::pipeline::lister::ObjectLister;
use crate::pipeline::packer::ObjectVersionsPacker;
use crate::pipeline::packer_point_in_time::ObjectPointInTimePacker;
use crate::pipeline::stage::Stage;
use crate::pipeline::syncer::ObjectSyncer;
use crate::pipeline::terminator::Terminator;
use crate::pipeline::user_defined_filter::UserDefinedFilter;
use crate::storage::{Storage, StoragePair};
use crate::types::event_callback::{EventData, EventType};
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectKeyMap, S3syncObject, SyncStatistics, SyncStatsReport};

const CHANNEL_CAPACITY: usize = 20000;

mod deleter;
mod diff_detector;
mod diff_lister;
mod filter;
mod head_object_checker;
mod key_aggregator;
mod lister;
mod packer;
mod packer_point_in_time;
mod stage;
mod storage_factory;
mod syncer;
mod terminator;
mod user_defined_filter;
mod versioning_info_collector;

pub struct Pipeline {
    config: Config,
    source: Storage,
    target: Storage,
    source_key_map: Option<ObjectKeyMap>,
    target_key_map: Option<ObjectKeyMap>,
    cancellation_token: PipelineCancellationToken,
    stats_receiver: Receiver<SyncStatistics>,
    has_error: Arc<AtomicBool>,
    has_warning: Arc<AtomicBool>,
    errors: Arc<Mutex<VecDeque<Error>>>,
    ready: bool,
    sync_stats_report: Arc<Mutex<SyncStatsReport>>,
}

impl Pipeline {
    pub async fn new(config: Config, cancellation_token: PipelineCancellationToken) -> Self {
        let has_warning = Arc::new(AtomicBool::new(false));
        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let StoragePair { source, target } = storage_factory::create_storage_pair(
            config.clone(),
            cancellation_token.clone(),
            stats_sender,
            has_warning.clone(),
        )
        .await;

        let source_key_map = if is_listing_source_required(config.sync_with_delete) {
            Some(ObjectKeyMap::new(Mutex::new(HashMap::new())))
        } else {
            None
        };

        let target_key_map = if is_listing_target_required(
            config.enable_versioning,
            config.sync_with_delete,
            config.filter_config.remove_modified_filter,
            config.sync_latest_tagging,
        ) {
            Some(ObjectKeyMap::new(Mutex::new(HashMap::new())))
        } else {
            None
        };

        Self {
            config,
            source,
            target,
            source_key_map,
            target_key_map,
            cancellation_token,
            stats_receiver,
            has_error: Arc::new(AtomicBool::new(false)),
            has_warning,
            errors: Arc::new(Mutex::new(VecDeque::<Error>::new())),
            ready: true,
            sync_stats_report: Arc::new(Mutex::new(SyncStatsReport::default())),
        }
    }

    pub async fn run(&mut self) {
        if !self.ready {
            panic!("it can be executed only once.")
        }
        self.ready = false;

        let mut event_data = EventData::new(EventType::PIPELINE_START);
        self.config
            .event_manager
            .trigger_event(event_data.clone())
            .await;

        if !self.check_prerequisites().await {
            self.shutdown().await;

            event_data.event_type = EventType::PIPELINE_END;
            self.config
                .event_manager
                .trigger_event(event_data.clone())
                .await;

            return;
        }

        if self.is_listing_target_required() {
            self.aggregate_target_keys().await;
            if self.has_error() {
                self.shutdown().await;

                event_data.event_type = EventType::PIPELINE_ERROR;
                event_data.message = Some(
                    // skipcq: RS-W1031
                    self.get_errors_and_consume()
                        .unwrap_or_default()
                        .first()
                        .unwrap_or(&anyhow!("Unknown error"))
                        .to_string(),
                );
                self.config
                    .event_manager
                    .trigger_event(event_data.clone())
                    .await;

                event_data.event_type = EventType::PIPELINE_END;
                event_data.message = None;
                self.config
                    .event_manager
                    .trigger_event(event_data.clone())
                    .await;

                return;
            }
        }

        self.sync().await;

        self.shutdown().await;

        if self.has_error() {
            self.shutdown().await;

            event_data.event_type = EventType::PIPELINE_ERROR;
            event_data.message = Some(
                // skipcq: RS-W1031
                self.get_errors_and_consume()
                    .unwrap_or_default()
                    .first()
                    .unwrap_or(&anyhow!("Unknown error"))
                    .to_string(),
            );
            self.config
                .event_manager
                .trigger_event(event_data.clone())
                .await;
        }

        event_data.event_type = EventType::PIPELINE_END;
        self.config
            .event_manager
            .trigger_event(event_data.clone())
            .await;
    }

    async fn shutdown(&self) {
        self.close_stats_sender();
    }

    async fn check_prerequisites(&self) -> bool {
        if self.config.enable_versioning && !self.is_both_bucket_versioning_enabled().await {
            self.print_and_store_error(None, "Versioning must be enabled on both buckets.")
                .await;

            return false;
        }

        if self.config.point_in_time.is_some() && !self.is_source_bucket_versioning_enabled().await
        {
            self.print_and_store_error(
                None,
                "--point-in-time option requires versioning enabled on the source bucket.",
            )
            .await;

            return false;
        }

        true
    }

    async fn is_both_bucket_versioning_enabled(&self) -> bool {
        let source_versioning_enabled = self.source.is_versioning_enabled().await;
        if let Err(e) = source_versioning_enabled {
            self.print_and_store_error(
                Some(e),
                "failed to check versioning status of source bucket.",
            )
            .await;

            return false;
        }

        let target_versioning_enabled = self.target.is_versioning_enabled().await;
        if let Err(e) = target_versioning_enabled {
            self.print_and_store_error(
                Some(e),
                "failed to check versioning status of target bucket.",
            )
            .await;

            return false;
        }

        if source_versioning_enabled.unwrap() && target_versioning_enabled.unwrap() {
            return true;
        }

        false
    }

    async fn is_source_bucket_versioning_enabled(&self) -> bool {
        let source_versioning_enabled = self.source.is_versioning_enabled().await;
        if let Err(e) = source_versioning_enabled {
            self.print_and_store_error(
                Some(e),
                "failed to check versioning status of source bucket.",
            )
            .await;

            return false;
        }

        source_versioning_enabled.unwrap()
    }

    fn is_listing_target_required(&self) -> bool {
        is_listing_target_required(
            self.config.enable_versioning,
            self.config.sync_with_delete,
            self.config.filter_config.remove_modified_filter,
            self.config.sync_latest_tagging,
        )
    }

    async fn aggregate_target_keys(&self) {
        self.terminate(self.aggregate_keys_if_necessary(
            self.list_target(),
            Some(self.target_key_map.clone().unwrap()),
        ))
        .await
        .unwrap();
    }

    async fn sync(&mut self) {
        if self.config.enable_versioning {
            // To keep versioning order, objects must be packed after filtered
            self.terminate(
                self.sync_objects(
                    self.pack_object_versions(self.filter_objects(self.list_source())),
                ),
            )
            .await
            .unwrap();
        } else if self.config.point_in_time.is_some() {
            self.terminate(self.sync_objects(
                self.pack_objects_at_point_in_time(self.filter_objects(self.list_source())),
            ))
            .await
            .unwrap();
        } else {
            self.terminate(self.sync_objects(self.filter_objects(
                self.aggregate_keys_if_necessary(
                    self.list_source(),
                    self.source_key_map.as_ref().cloned(),
                ),
            )))
            .await
            .unwrap();
        }

        if self.has_error() {
            return;
        }

        if self.config.sync_with_delete {
            self.delete().await;
        }
    }

    fn list_source(&self) -> Receiver<S3syncObject> {
        let (stage, next_stage_receiver) = self.create_spsc_stage(None, self.has_warning.clone());
        let object_lister = ObjectLister::new(stage);
        let has_error = self.has_error.clone();
        let error_list = self.errors.clone();
        let max_keys = self.config.max_keys;

        tokio::spawn(async move {
            let result = object_lister.list_source(max_keys).await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    log_error(has_error, error_list, e, "list source objects failed.");
                }
            }
        });

        next_stage_receiver
    }

    fn list_target(&self) -> Receiver<S3syncObject> {
        let (stage, next_stage_receiver) = self.create_spsc_stage(None, self.has_warning.clone());
        let object_lister = ObjectLister::new(stage);
        let has_error = self.has_error.clone();
        let error_list = self.errors.clone();
        let max_keys = self.config.max_keys;

        tokio::spawn(async move {
            let result = object_lister.list_target(max_keys).await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    log_error(has_error, error_list, e, "list target objects failed.");
                }
            }
        });

        next_stage_receiver
    }

    fn aggregate_keys_if_necessary(
        &self,
        objects_list: Receiver<S3syncObject>,
        key_map: Option<ObjectKeyMap>,
    ) -> Receiver<S3syncObject> {
        if key_map.is_none() {
            return objects_list;
        }

        let (stage, next_stage_receiver) =
            self.create_spsc_stage(Some(objects_list), self.has_warning.clone());
        let key_aggregator = KeyAggregator::new(stage);

        let has_error = self.has_error.clone();
        let error_list = self.errors.clone();
        tokio::spawn(async move {
            let result = key_aggregator.aggregate(&key_map.unwrap()).await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    log_error(has_error, error_list, e, "keys aggregation failed.");
                }
            }
        });

        next_stage_receiver
    }

    fn filter_objects(&self, objects_list: Receiver<S3syncObject>) -> Receiver<S3syncObject> {
        let mut previous_stage_receiver = objects_list;

        if self.config.filter_config.before_time.is_some() {
            let (stage, new_receiver) =
                self.create_spsc_stage(Some(previous_stage_receiver), self.has_warning.clone());

            self.spawn_filter(Box::new(filter::MtimeBeforeFilter::new(stage, None)));
            trace!("MtimeBeforeFilter has been started.");

            previous_stage_receiver = new_receiver;
        }

        if self.config.filter_config.after_time.is_some() {
            let (stage, new_receiver) =
                self.create_spsc_stage(Some(previous_stage_receiver), self.has_warning.clone());

            self.spawn_filter(Box::new(filter::MtimeAfterFilter::new(stage, None)));
            trace!("MtimeAfterFilter has been started.");

            previous_stage_receiver = new_receiver;
        }

        if self.config.filter_config.smaller_size.is_some() {
            let (stage, new_receiver) =
                self.create_spsc_stage(Some(previous_stage_receiver), self.has_warning.clone());

            self.spawn_filter(Box::new(filter::SmallerSizeFilter::new(stage, None)));
            trace!("SmallerSizeFilter has been started.");

            previous_stage_receiver = new_receiver;
        }

        if self.config.filter_config.larger_size.is_some() {
            let (stage, new_receiver) =
                self.create_spsc_stage(Some(previous_stage_receiver), self.has_warning.clone());

            self.spawn_filter(Box::new(filter::LargerSizeFilter::new(stage, None)));
            trace!("LargerSizeFilter has been started.");

            previous_stage_receiver = new_receiver;
        }

        if self.config.filter_config.include_regex.is_some() {
            let (stage, new_receiver) =
                self.create_spsc_stage(Some(previous_stage_receiver), self.has_warning.clone());

            self.spawn_filter(Box::new(IncludeRegexFilter::new(stage, None)));
            trace!("IncludeRegexFilter has been started.");

            previous_stage_receiver = new_receiver;
        }

        if self.config.filter_config.exclude_regex.is_some() {
            let (stage, new_receiver) =
                self.create_spsc_stage(Some(previous_stage_receiver), self.has_warning.clone());

            self.spawn_filter(Box::new(ExcludeRegexFilter::new(stage, None)));
            trace!("ExcludeRegexFilter has been started.");

            previous_stage_receiver = new_receiver;
        }

        if self
            .config
            .filter_config
            .filter_manager
            .is_callback_registered()
        {
            let (stage, new_receiver) =
                self.create_spsc_stage(Some(previous_stage_receiver), self.has_warning.clone());

            self.spawn_user_defined_filter(stage);
            trace!("UserDefinedFilter has been started.");

            previous_stage_receiver = new_receiver;
        }

        if !self.config.enable_versioning && !self.config.filter_config.remove_modified_filter {
            let (stage, new_receiver) =
                self.create_spsc_stage(Some(previous_stage_receiver), self.has_warning.clone());
            self.spawn_filter(Box::new(filter::TargetModifiedFilter::new(
                stage,
                self.target_key_map.clone(),
            )));
            trace!("TargetModifiedFilter has started.");

            previous_stage_receiver = new_receiver;
        }

        previous_stage_receiver
    }

    fn spawn_filter(&self, filter: Box<dyn ObjectFilter + Send + Sync>) {
        let has_error = self.has_error.clone();
        let error_list = self.errors.clone();

        tokio::spawn(async move {
            let result = filter.filter().await;
            match result {
                Ok(_) => {}
                Err(e) => {
                    log_error(has_error, error_list, e, "filter objects failed.");
                }
            }
        });
    }

    fn spawn_user_defined_filter(&self, stage: Stage) {
        let has_error = self.has_error.clone();
        let error_list = self.errors.clone();

        let mut user_defined_filter = UserDefinedFilter::new(stage);

        tokio::spawn(async move {
            let result = user_defined_filter.filter().await;
            match result {
                Ok(_) => {}
                Err(e) => {
                    log_error(has_error, error_list, e, "filter objects failed.");
                }
            }
        });
    }

    fn sync_objects(&self, target_objects: Receiver<S3syncObject>) -> Receiver<S3syncObject> {
        let (sender, next_stage_receiver) =
            async_channel::bounded::<S3syncObject>(CHANNEL_CAPACITY);

        for worker_index in 0..(self.config.worker_size) {
            let stage = self.create_mpmc_stage(
                sender.clone(),
                target_objects.clone(),
                self.has_warning.clone(),
            );

            let object_syncer =
                ObjectSyncer::new(stage, worker_index, self.get_sync_stats_report());
            let has_error = self.has_error.clone();
            let error_list = self.errors.clone();

            tokio::spawn(async move {
                let result = object_syncer.sync().await;
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        log_error(has_error, error_list, e, "sync objects failed.");
                    }
                }
            });
        }

        next_stage_receiver
    }

    fn pack_object_versions(
        &self,
        target_objects: Receiver<S3syncObject>,
    ) -> Receiver<S3syncObject> {
        let (stage, new_receiver) =
            self.create_spsc_stage(Some(target_objects), self.has_warning.clone());
        let packer = ObjectVersionsPacker::new(stage);
        let has_error = self.has_error.clone();
        let error_list = self.errors.clone();
        tokio::spawn(async move {
            let result = packer.pack().await;
            match result {
                Ok(_) => {}
                Err(e) => {
                    log_error(has_error, error_list, e, "pack objects failed.");
                }
            }
        });

        new_receiver
    }

    fn pack_objects_at_point_in_time(
        &self,
        target_objects: Receiver<S3syncObject>,
    ) -> Receiver<S3syncObject> {
        let (stage, new_receiver) =
            self.create_spsc_stage(Some(target_objects), self.has_warning.clone());
        let packer = ObjectPointInTimePacker::new(stage);
        let has_error = self.has_error.clone();
        let error_list = self.errors.clone();
        tokio::spawn(async move {
            let result = packer.pack().await;
            match result {
                Ok(_) => {}
                Err(e) => {
                    log_error(
                        has_error,
                        error_list,
                        e,
                        "object point-in-time packer failed.",
                    );
                }
            }
        });

        new_receiver
    }

    fn terminate(&self, synced_objects: Receiver<S3syncObject>) -> JoinHandle<()> {
        let terminator = Terminator::new(synced_objects);

        tokio::spawn(async move {
            terminator.terminate().await;
        })
    }

    async fn delete(&mut self) {
        self.terminate(self.delete_target_objects(self.list_diff()))
            .await
            .unwrap();
    }

    fn list_diff(&self) -> Receiver<S3syncObject> {
        let (stage, next_stage_receiver) = self.create_spsc_stage(None, self.has_warning.clone());
        let diff_lister = DiffLister::new(stage);
        let has_error = self.has_error.clone();
        let error_list = self.errors.clone();

        let source_key_map = self.source_key_map.clone().unwrap();
        let target_key_map = self.target_key_map.clone().unwrap();
        tokio::spawn(async move {
            let result = diff_lister.list(&source_key_map, &target_key_map).await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    log_error(has_error, error_list, e, "difference detection failed.");
                }
            }
        });

        next_stage_receiver
    }

    fn delete_target_objects(
        &self,
        target_objects: Receiver<S3syncObject>,
    ) -> Receiver<S3syncObject> {
        let (sender, next_stage_receiver) =
            async_channel::bounded::<S3syncObject>(CHANNEL_CAPACITY);

        for worker_index in 0..(self.config.worker_size) {
            let stage = self.create_mpmc_stage(
                sender.clone(),
                target_objects.clone(),
                self.has_warning.clone(),
            );
            let object_deleter = ObjectDeleter::new(stage, worker_index);
            let has_error = self.has_error.clone();
            let error_list = self.errors.clone();

            tokio::spawn(async move {
                let result = object_deleter.delete_target().await;
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        log_error(has_error, error_list, e, "delete target objects failed.");
                    }
                }
            });
        }

        next_stage_receiver
    }

    fn create_spsc_stage(
        &self,
        previous_stage_receiver: Option<Receiver<S3syncObject>>,
        has_warning: Arc<AtomicBool>,
    ) -> (Stage, Receiver<S3syncObject>) {
        let (sender, next_stage_receiver) =
            async_channel::bounded::<S3syncObject>(CHANNEL_CAPACITY);
        let stage = Stage::new(
            self.config.clone(),
            Some(dyn_clone::clone_box(&*self.source)),
            Some(dyn_clone::clone_box(&*self.target)),
            previous_stage_receiver,
            Some(sender),
            self.cancellation_token.clone(),
            has_warning,
        );

        (stage, next_stage_receiver)
    }

    fn create_mpmc_stage(
        &self,
        sender: Sender<S3syncObject>,
        receiver: Receiver<S3syncObject>,
        has_warning: Arc<AtomicBool>,
    ) -> Stage {
        Stage::new(
            self.config.clone(),
            Some(dyn_clone::clone_box(&*self.source)),
            Some(dyn_clone::clone_box(&*self.target)),
            Some(receiver),
            Some(sender),
            self.cancellation_token.clone(),
            has_warning,
        )
    }

    async fn print_and_store_error(&self, e: Option<Error>, message: &str) {
        self.has_error.store(true, Ordering::SeqCst);

        let mut event_data = EventData::new(EventType::PIPELINE_ERROR);
        if let Some(e) = e {
            let error = e.to_string();
            let source = e.source();

            event_data.message = Some(format!("{error}: {message}"));
            self.config
                .event_manager
                .trigger_event(event_data.clone())
                .await;

            error!(error = error, source = source, message);
            self.errors.lock().unwrap().push_back(e);
        } else {
            event_data.message = Some(message.to_string());
            self.config
                .event_manager
                .trigger_event(event_data.clone())
                .await;

            error!("{}", message.to_string());
            self.errors
                .lock()
                .unwrap()
                .push_back(anyhow!(message.to_string()));
        }
    }

    pub fn get_stats_receiver(&self) -> Receiver<SyncStatistics> {
        self.stats_receiver.clone()
    }

    pub fn has_error(&self) -> bool {
        self.has_error.load(Ordering::SeqCst)
    }

    pub fn has_warning(&self) -> bool {
        self.has_warning.load(Ordering::SeqCst)
    }

    pub fn get_errors_and_consume(&self) -> Option<Vec<Error>> {
        if !self.has_error() {
            return None;
        }

        let error_list = self.errors.clone();
        let mut error_list = error_list.lock().unwrap();

        let mut errors_to_return = Vec::<Error>::new();
        for _ in 0..error_list.len() {
            errors_to_return.push(error_list.pop_front().unwrap());
        }

        Some(errors_to_return)
    }

    pub fn get_sync_stats_report(&self) -> Arc<Mutex<SyncStatsReport>> {
        self.sync_stats_report.clone()
    }

    pub fn close_stats_sender(&self) {
        self.source.get_stats_sender().close();
        self.target.get_stats_sender().close();
    }
}

fn log_error(
    has_error: Arc<AtomicBool>,
    errors: Arc<Mutex<VecDeque<Error>>>,
    e: Error,
    message: &str,
) {
    has_error.store(true, Ordering::SeqCst);

    let error = e.to_string();
    let source = e.source();

    error!(error = error, source = source, message);

    let mut error_list = errors.lock().unwrap();
    error_list.push_back(e);
}

fn is_listing_source_required(delete_target: bool) -> bool {
    delete_target
}

fn is_listing_target_required(
    enable_versioning: bool,
    delete_target: bool,
    remove_modified_filter: bool,
    sync_latest_tagging: bool,
) -> bool {
    if enable_versioning || sync_latest_tagging {
        return false;
    }

    delete_target || !remove_modified_filter
}

#[cfg(test)]
mod tests {
    use crate::config::args::parse_from_args;
    use crate::types::token::create_pipeline_cancellation_token;
    use std::path::PathBuf;
    use tracing_subscriber::EnvFilter;

    use super::*;

    const WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START: u64 = 500;
    const TEST_SOURCE_OBJECTS_COUNT: usize = 6;
    const TEST_SYNC_OBJECTS_COUNT: usize = 1;

    #[tokio::test]
    async fn new_pipeline() {
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

        Pipeline::new(config, create_pipeline_cancellation_token()).await;
    }

    #[tokio::test]
    async fn run_pipeline() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--head-each-target",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        assert!(!pipeline.get_stats_receiver().is_empty());
        assert!(!pipeline.has_error());
    }

    #[tokio::test]
    async fn run_pipeline_check_size() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--head-each-target",
            "--check-size",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        assert!(!pipeline.get_stats_receiver().is_empty());
        assert!(!pipeline.has_error());
    }

    #[tokio::test]
    #[should_panic]
    async fn run_pipeline_twice() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--head-each-target",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        pipeline.run().await;
    }

    #[tokio::test]
    async fn run_pipeline_with_skip() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--remove-modified-filter",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        assert!(!pipeline.get_stats_receiver().is_empty());
        assert!(!pipeline.has_error());
    }

    #[tokio::test]
    async fn run_pipeline_with_dry_run() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--remove-modified-filter",
            "--dry-run",
            "./test_data/source/dir1/",
            "./test_data/target/put_dry_run_test/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        assert!(!pipeline.get_stats_receiver().is_empty());
        assert!(!pipeline.has_error());

        assert!(
            !PathBuf::from("./test_data/target/put_dry_run_test/6byte.dat")
                .try_exists()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn run_pipeline_with_delete() {
        init_dummy_tracing_subscriber();

        {
            tokio::fs::File::create("./test_data/target/delete_test/data1")
                .await
                .unwrap();
        }

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--delete",
            "./test_data/source/dir2/",
            "./test_data/target/delete_test/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        assert!(!pipeline.get_stats_receiver().is_empty());
        assert!(!pipeline.has_error());

        assert!(
            !PathBuf::from("./test_data/target/delete_test/data1")
                .try_exists()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn run_pipeline_with_delete_cancel() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--delete",
            "./test_data/source/dir2/",
            "./test_data/target/delete_test/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config, cancellation_token.clone()).await;
        cancellation_token.cancel();

        pipeline.run().await;

        assert!(!pipeline.has_error());
    }

    #[tokio::test]
    async fn run_pipeline_with_delete_check_size() {
        init_dummy_tracing_subscriber();

        {
            tokio::fs::File::create("./test_data/target/delete_test2/data1")
                .await
                .unwrap();
        }

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--check-size",
            "--delete",
            "./test_data/source/dir2/",
            "./test_data/target/delete_test2/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        assert!(!pipeline.get_stats_receiver().is_empty());
        assert!(!pipeline.has_error());

        assert!(
            !PathBuf::from("./test_data/target/delete_test2/data1")
                .try_exists()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn run_pipeline_with_dry_run_delete() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--dry-run",
            "--delete",
            "./test_data/source/dir2/",
            "./test_data/target/delete_dry_run_test/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        assert!(!pipeline.get_stats_receiver().is_empty());
        assert!(!pipeline.has_error());

        assert!(
            PathBuf::from("./test_data/target/delete_dry_run_test/data1")
                .try_exists()
                .unwrap()
        )
    }

    #[tokio::test]
    async fn run_pipeline_sync_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--aws-max-attempts",
            "1",
            "--source-endpoint-url",
            "https://invalid-s3-endpoint-url.6329313.local:65535",
            "--force-retry-count",
            "1",
            "--force-retry-interval-milliseconds",
            "1",
            "s3://invalid_bucket?name!",
            "s3://invalid_bucket?name!",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let mut pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.run().await;

        assert!(pipeline.has_error());
    }

    #[tokio::test]
    async fn list_source() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "./test_data/source",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        let receiver = pipeline.list_source();

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;

        assert_eq!(receiver.len(), TEST_SOURCE_OBJECTS_COUNT);
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn list_source_error() {
        init_dummy_tracing_subscriber();

        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        assert!(
            !nix::unistd::geteuid().is_root(),
            "tests must not run as root"
        );

        let args = vec![
            "s3sync",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--warn-as-error",
            "./test_data/denied_dir2",
            "s3://target-bucket",
        ];
        let mut permissions = fs::metadata("./test_data/denied_dir2")
            .unwrap()
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions("./test_data/denied_dir2", permissions).unwrap();

        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        let receiver = pipeline.list_source();

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;

        permissions = fs::metadata("./test_data/denied_dir2")
            .unwrap()
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions("./test_data/denied_dir2", permissions).unwrap();

        assert_eq!(receiver.len(), 0);
        assert!(pipeline.has_error());
    }

    #[tokio::test]
    async fn list_target() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source",
            "./test_data/source",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        let receiver = pipeline.list_target();

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;

        assert_eq!(receiver.len(), TEST_SOURCE_OBJECTS_COUNT);
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn list_target_error() {
        init_dummy_tracing_subscriber();

        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        assert!(
            !nix::unistd::geteuid().is_root(),
            "tests must not run as root"
        );

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--warn-as-error",
            "./test_data/source",
            "./test_data/denied_dir3",
        ];
        let mut permissions = fs::metadata("./test_data/denied_dir3")
            .unwrap()
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions("./test_data/denied_dir3", permissions).unwrap();

        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        let receiver = pipeline.list_target();

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;

        permissions = fs::metadata("./test_data/denied_dir3")
            .unwrap()
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions("./test_data/denied_dir3", permissions).unwrap();

        assert_eq!(receiver.len(), 0);
        assert!(pipeline.has_error());
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn get_errors_and_consume_some() {
        init_dummy_tracing_subscriber();

        assert!(
            !nix::unistd::geteuid().is_root(),
            "tests must not run as root"
        );

        let args = vec![
            "s3sync",
            "--source-endpoint-url",
            "https://invalid-s3-endpoint-url.6329313.local:65535",
            "--target-endpoint-url",
            "https://invalid-s3-endpoint-url.6329313.local:65535",
            "--aws-max-attempts",
            "1",
            "--force-retry-count",
            "1",
            "--force-retry-interval-milliseconds",
            "1",
            "s3://source-bucket",
            "s3://target-bucket",
        ];

        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.list_target();
        tokio::time::sleep(std::time::Duration::from_millis(5000)).await;

        assert!(pipeline.has_error());
        assert_eq!(pipeline.get_errors_and_consume().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn get_errors_and_consume_none() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source",
            "./test_data/source",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.list_target();

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;

        assert!(!pipeline.has_error());
        assert!(pipeline.get_errors_and_consume().is_none());
    }

    #[tokio::test]
    async fn with_filters() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--filter-mtime-before",
            "2060-01-01T00:00:00.000Z",
            "--filter-mtime-after",
            "1971-01-01T00:00:00.000Z",
            "--filter-include-regex",
            r".+\.(csv|pdf)$",
            "--filter-exclude-regex",
            r".+\.(xlsx|docx)$",
            "--filter-smaller-size",
            "5MiB",
            "--filter-larger-size",
            "100000",
            "./test_data/source",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.filter_objects(pipeline.list_source());
    }

    #[tokio::test]
    async fn with_filters_all_through() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--remove-modified-filter",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--filter-mtime-before",
            "2060-01-01T00:00:00.000Z",
            "--filter-mtime-after",
            "1971-01-01T00:00:00.000Z",
            "--filter-smaller-size",
            "5MiB",
            "--filter-larger-size",
            "2",
            "--filter-include-regex",
            r".+\.(data|pdf)$",
            "--filter-exclude-regex",
            r".+\.(xlsx|docx)$",
            "./test_data/source",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        pipeline.filter_objects(pipeline.list_source());

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;
    }

    #[tokio::test]
    async fn with_filters_cancel() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--remove-modified-filter",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--filter-mtime-before",
            "2060-01-01T00:00:00.000Z",
            "--filter-mtime-after",
            "1971-01-01T00:00:00.000Z",
            "--filter-smaller-size",
            "5MiB",
            "--filter-larger-size",
            "2",
            "--filter-include-regex",
            r".+\.(data|pdf)$",
            "--filter-exclude-regex",
            r".+\.(xlsx|docx)$",
            "./test_data/source",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let cancellation_token = create_pipeline_cancellation_token();

        let pipeline = Pipeline::new(config, cancellation_token.clone()).await;
        cancellation_token.cancel();

        pipeline.filter_objects(pipeline.list_source());

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;
    }

    #[tokio::test]
    async fn no_filters() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "./test_data/source",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        let receiver = pipeline.filter_objects(pipeline.list_source());

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;

        assert_eq!(receiver.len(), TEST_SOURCE_OBJECTS_COUNT);
    }

    #[tokio::test]
    async fn sync_object() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--remove-modified-filter",
            "./test_data/source/dir1/",
            "./test_data/target/sync_test/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        let receiver = pipeline.sync_objects(pipeline.list_source());

        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_TASK_START,
        ))
        .await;

        assert_eq!(receiver.len(), TEST_SYNC_OBJECTS_COUNT);
    }

    #[tokio::test]
    async fn terminate() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--filter-mtime-before",
            "2060-01-01T00:00:00.000Z",
            "--filter-mtime-after",
            "1971-01-01T00:00:00.000Z",
            "./test_data/source",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let pipeline = Pipeline::new(config, create_pipeline_cancellation_token()).await;
        let join_handle = pipeline.terminate(pipeline.list_source());

        join_handle.await.unwrap();
    }

    #[test]
    fn is_listing_target_required_test() {
        init_dummy_tracing_subscriber();

        assert!(is_listing_target_required(false, true, false, false));
        assert!(!is_listing_target_required(false, false, true, false));
        assert!(is_listing_target_required(false, false, false, false));
        assert!(is_listing_target_required(false, true, true, false));

        assert!(!is_listing_target_required(true, true, false, false));
        assert!(!is_listing_target_required(true, false, true, false));
        assert!(!is_listing_target_required(true, false, false, false));
        assert!(!is_listing_target_required(true, true, true, false));

        assert!(!is_listing_target_required(false, true, false, true));
        assert!(!is_listing_target_required(false, false, true, true));
        assert!(!is_listing_target_required(false, false, false, true));
        assert!(!is_listing_target_required(true, true, true, true));
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
