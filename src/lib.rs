/*!
# Overview
s3sync is a reliable, very fast, and powerful synchronization tool for S3.
It can be used to synchronize local directories with S3 bucket, and also to synchronize S3 to s3 bucket.
Supports multipart upload, versioning, metadata.

## Features
- Reliable: In-depth end-to-end object integrity check
  s3sync calculates ETag(MD5 or equivalent) for each object and compares them with the ETag in the target.
  An object that exists in the local disk is read from the disk and compared with the checksum in the source or target.
  Optionally, s3sync can also calculate and compare additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME) for each object.
  s3sync always shows the integrity check result, so you can verify that the synchronization was successful.

- Very fast
  s3sync implemented in Rust, using AWS SDK for Rust that uses multithreaded asynchronous I/O.
  In my environment(`c7a.large`, with 256 workers), Local to S3, about 3,900 objects/sec (small objects 10KiB) and Local to S3 16 objects(6GiB objects) 96.00 GiB, about 280 MiB/sec.
  Large objects are transferred in parallel using multipart upload (or get-range request for download), so it can transfer large objects very fast.
  Note: The default s3sync setting uses `--worker-size 16` and `--max-parallel-uploads 16`. This is a moderate setting for most cases. If you want to improve performance, you can increase `--worker-size` and `--max-parallel-uploads`. But it will increase CPU and memory usage.

- Multiple ways
  - Local to S3(S3-compatible storage)
  - S3(S3-compatible storage) to Local
  - S3 to S3(cross-region, same-region, same-account, cross-account, from-to S3/S3-compatible storage)

- Incremental transfer
  There are many ways to transfer objects:
    - Modified time based(default)
    - Size-based
    - ETag(MD5 or equivalent) based
    - Additional checksum(SHA256/SHA1/CRC32/CRC32C/CRC64NVME) based

- Flexible filtering
  - key, `ContentType`, user-defined metadata, tagging, by regular expression.
  - size, modified time

For more information, see [s3sync homepage](https://github.com/nidor1998/s3sync)

## As a library
s3sync can be used as a library.

s3sync CLI is a very thin wrapper of the s3sync library. You can use all features of s3sync CLI in the library.

s3sync library has many features that are not documented. You can refer to the s3sync CLI help(`s3sync -h`) for the features and pass the arguments to the library.

You can refer to the source code bin/cli to implement your own synchronization tool.

**NOTE: s3sync library is assumed to be used like a way that you use s3sync CLI.**

Example usage
=============

```Toml
[dependencies]
s3sync = "1"
tokio = { version = "1", features = ["full"] }

# If you want to use EventCallback, you need to add async-trait crate.
async-trait = "0.1"
```

```no_run
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
use s3sync::types::{S3syncObject, SyncStatistics};

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
            _ => {
                println!("Other events: {event_data:?}");
            }
        }
    }
}

// This struct represents a user-defined filter callback.
// It can be used to implement custom filtering logic for objects while listing them in the source.
// While preprocess callback is invoked after the source object data is fetched, this callback is invoked while listing objects.
// So this callback is preferred to be used for filtering objects based on basic properties like key, size, etc.
pub struct DebugFilterCallback;
#[async_trait]
#[cfg(not(tarpaulin_include))]
impl FilterCallback for DebugFilterCallback {
    // The callbacks are called serially, and the callback function MUST return immediately.
    // If a callback function takes a long time to execute, it may block a whole pipeline.
    // This callback is invoked while listing objects in the source.
    // This function should return false if the object should be filtered out (not uploaded)
    // and true if the object should be uploaded.
    // If an error occurs, it should be handled gracefully, and the function should return an error, and the pipeline will be cancelled.
    #[cfg(not(tarpaulin_include))]
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

#[tokio::main]
async fn main() {
    // You can use all the arguments for s3sync CLI.
    let args = vec![
        "program_name",
        "--aws-max-attempts",
        "7",
        "./src",
        "s3://test-bucket/src/",
    ];

    // s3sync library converts the arguments to Config.
    let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

    // This is an event manager that manages the event callbacks.(optional)
    // You can specify the events you want to capture. E.g. `EventType::SYNC_START | EventType::SYNC_COMPLETE`
    // If you want to capture all events, use `EventType::ALL_EVENTS`.
    config.event_manager.register_callback(
        EventType::SYNC_START | EventType::SYNC_COMPLETE | EventType::SYNC_CANCEL,
        DebugEventCallback {},
    );

    // The user-defined filter callback is disabled by default.
    // You can register a filter callback to filter objects while listing them in the source.
    config
        .filter_config
        .filter_manager
        .register_callback(DebugFilterCallback {});

    // This is a preprocess manager that manages the preprocess callbacks.(optional)
    // You can register a preprocess callback to dynamically modify the upload metadata before uploading objects to S3.
    config
        .preprocess_manager
        .register_callback(DebugPreprocessCallback {});

    // Create a cancellation token for the pipeline.
    // You can use this token to cancel the pipeline.
    let cancellation_token = create_pipeline_cancellation_token();
    let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
    let stats_receiver = pipeline.get_stats_receiver();

    // You can close statistics sender to stop statistics collection, if needed.
    // Statistics collection consumes some Memory, so it is recommended to close it if you don't need it.
    // pipeline.close_stats_sender();

    pipeline.run().await;

    // You can use the statistics receiver to get the statistics of the pipeline.
    // Or, you can get the live statistics, If you run async the pipeline.
    let mut total_sync_count = 0;
    while let Ok(sync_stats) = stats_receiver.try_recv() {
        if matches!(sync_stats, SyncStatistics::SyncComplete { .. }) {
            total_sync_count += 1;
        }
    }

    println!("Total sync count: {total_sync_count}");

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
    let sync_stats_to_be_locked = pipeline.get_sync_stats_report();
    let sync_stats = sync_stats_to_be_locked.lock().unwrap();
    if sync_stats.number_of_objects != sync_stats.etag_matches {
        println!("Some objects could not be transferred correctly.");
    }
}
```

For more examples,
=============
see [s3sync binary source code](https://github.com/nidor1998/s3sync/tree/main/src/bin/s3sync).

see [s3sync integration tests](https://github.com/nidor1998/s3sync/tree/main/tests).

For more information about s3sync binary,
=============
see [s3sync homepage](https://github.com/nidor1998/s3sync).
*/

pub use config::Config;
pub use config::args::CLIArgs;

pub mod callback;
pub mod config;
pub mod pipeline;
pub mod storage;
pub mod types;
