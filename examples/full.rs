use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use std::collections::HashMap;

use s3sync::config::Config;
use s3sync::config::args::parse_from_args;
use s3sync::pipeline::Pipeline;
use s3sync::types::event_callback::{EventCallback, EventData, EventType};
use s3sync::types::filter_callback::FilterCallback;
use s3sync::types::preprocess_callback::{PreprocessCallback, PreprocessError, UploadMetadata};
use s3sync::types::token::create_pipeline_cancellation_token;
#[allow(unused_imports)]
use s3sync::types::{S3syncObject, SyncStatistics};

#[tokio::main]
async fn main() {
    // You can use all the arguments for s3sync CLI.
    let args = vec![
        "program_name",
        "--aws-max-attempts",
        "7",
        "./test_data/e2e_test/case1",
        "s3://XXXXXXX-TEST-BUCKET/",
    ];

    // s3sync library converts the arguments to Config.
    let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

    // This is an event manager that manages the event callbacks.(optional)
    // You can specify the events you want to capture. E.g. `EventType::SYNC_START | EventType::SYNC_COMPLETE`
    // If you want to capture all events, use `EventType::ALL_EVENTS`.
    config.event_manager.register_callback(
        EventType::SYNC_START
            | EventType::SYNC_COMPLETE
            | EventType::SYNC_CANCEL
            | EventType::PIPELINE_ERROR,
        DebugEventCallback {},
        config.dry_run,
    );

    // This is a filter manager that manages the filter callbacks.(optional)
    // You can register a filter callback to filter objects while listing them in the source.
    config
        .filter_config
        .filter_manager
        .register_callback(DebugFilterCallback {});

    // This is a preprocessed manager that manages the preprocess callbacks.(optional)
    // You can register a preprocessed callback to dynamically modify the upload metadata before uploading objects to S3.
    config
        .preprocess_manager
        .register_callback(DebugPreprocessCallback {});

    // Create a cancellation token for the pipeline.
    // You can use this token to cancel the pipeline.
    let cancellation_token = create_pipeline_cancellation_token();
    let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

    // You can get the statistics receiver to get the statistics of the pipeline in real time.
    // let stats_receiver = pipeline.get_stats_receiver();

    // You can close statistics sender to stop a statistics collection, if needed.
    // Statistics collection consumes some Memory, so it is recommended to close it if you don't need it.
    pipeline.close_stats_sender();

    pipeline.run().await;

    // You can get the sync statistics of the pipeline.
    let sync_stats = pipeline.get_sync_stats().await;
    println!(
        "stats_transferred_byte: {}",
        sync_stats.stats_transferred_byte
    );
    println!(
        "stats_transferred_byte_per_sec: {}",
        sync_stats.stats_transferred_byte_per_sec
    );
    println!(
        "stats_transferred_object: {}",
        sync_stats.stats_transferred_object
    );
    println!(
        "stats_transferred_object_per_sec: {}",
        sync_stats.stats_transferred_object_per_sec
    );
    println!("stats_etag_verified: {}", sync_stats.stats_etag_verified);
    println!("stats_etag_mismatch: {}", sync_stats.stats_etag_mismatch);
    println!(
        "stats_checksum_verified: {}",
        sync_stats.stats_checksum_verified
    );
    println!(
        "stats_checksum_mismatch: {}",
        sync_stats.stats_checksum_mismatch
    );
    println!("stats_deleted: {}", sync_stats.stats_deleted);
    println!("stats_skipped: {}", sync_stats.stats_skipped);
    println!("stats_error: {}", sync_stats.stats_error);
    println!("stats_warning: {}", sync_stats.stats_warning);
    println!("stats_duration_sec: {}", sync_stats.stats_duration_sec);

    // If there is an error in the pipeline, you can get the errors.
    if pipeline.has_error() {
        println!("An error has occurred.\n\n");
        println!("{:?}", pipeline.get_errors_and_consume().unwrap()[0]);
    }

    // If there is a warning in the pipeline, you can check it.
    if pipeline.has_warning() {
        println!("A warning has occurred.\n\n");
    }

    // If you use `--report-sync-stats`, `--report-metadata-sync-status` or
    // `--report-tagging-sync-status` options, you can get the sync statistics report.
    // These options are used to report the sync status of each object and not transfer the objects.
    // The above code does not use these options, so the sync statistics report is empty(all zero).
    let sync_stats_report_to_be_locked = pipeline.get_sync_stats_report();
    let sync_stats_report = sync_stats_report_to_be_locked.lock().unwrap();
    if sync_stats_report.number_of_objects != sync_stats_report.etag_matches {
        println!("Some objects could not be transferred correctly.");
    }
}

// This struct represents a user-defined event callback.
// You can use this callback to handle events, such as logging, monitoring, or custom actions.
pub struct DebugEventCallback;

#[async_trait]
impl EventCallback for DebugEventCallback {
    // A callback function is called when an event occurs in the pipeline.
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    async fn on_event(&mut self, event_data: EventData) {
        match event_data.event_type {
            EventType::SYNC_START => {
                println!("Sync started: {event_data:?}");
            }
            EventType::SYNC_COMPLETE => {
                println!("Sync complete: {event_data:?}");
            }
            EventType::SYNC_CANCEL => {
                println!("Sync cancelled: {event_data:?}");
            }
            EventType::PIPELINE_ERROR => {
                println!("Pipeline error: {event_data:?}");
            }
            _ => {
                println!("Other events: {event_data:?}");
            }
        }
    }
}

// This struct represents a user-defined filter callback.
// It can be used to implement custom filtering logic for objects while listing them in the source.
// While a preprocess callback is invoked after the source object data is fetched, this callback is invoked while listing objects.
// So this callback is preferred to be used for filtering objects based on basic properties like key, size, etc.
pub struct DebugFilterCallback;
#[async_trait]
impl FilterCallback for DebugFilterCallback {
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    // This callback is invoked while listing objects in the source.
    // This function should return false if the object should be filtered out (not uploaded)
    // and true if the object should be uploaded.
    // If an error occurs, it should be handled gracefully, and the function should return an error, and the pipeline will be cancelled.
    async fn filter(&mut self, source_object: &S3syncObject) -> anyhow::Result<bool> {
        Ok(!source_object.key().starts_with("should_be_skipped/"))
    }
}

// This struct represents a user-defined preprocessed callback.
// It can be used to implement custom preprocessing logic before uploading objects to S3.
pub struct DebugPreprocessCallback;

#[async_trait]
impl PreprocessCallback for DebugPreprocessCallback {
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    async fn preprocess_before_upload(
        &mut self,
        key: &str,                       // The key of the object being uploaded
        source_object: &GetObjectOutput, // The source object being uploaded(read only)
        metadata: &mut UploadMetadata,   // The metadata for the upload, which can be modified
    ) -> anyhow::Result<()> {
        // If we want to cancel the upload, return an error with PreprocessError::Cancelled
        if key == "callback_cancel_test" || key == "data1" {
            return Err(anyhow::Error::from(PreprocessError::Cancelled));
        }

        // The following code is an example of how to modify the metadata before uploading based on the source object.
        let content_length = source_object.content_length.unwrap().to_string();
        if let Some(user_defined_metadata) = metadata.metadata.as_mut() {
            user_defined_metadata.insert("mycontent-length".to_string(), content_length);
        } else {
            let mut user_defined_metadata = HashMap::new();
            user_defined_metadata.insert("mycontent-length".to_string(), content_length);
            metadata.metadata = Some(user_defined_metadata);
        }

        Ok(())
    }
}
