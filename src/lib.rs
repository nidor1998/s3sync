/*!
# Overview
s3sync is a reliable, very fast, and powerful synchronization tool for S3.
It can be used to synchronize local directories with S3 bucket, and also to synchronize S3 to s3 bucket.
Supports multipart upload, versioning, metadata.

AWS SDK for rust is not production ready yet and is not recommended for production use, so s3sync is also not recommended for production use. But any feedback is welcome. I will continue to improve s3sync.

This crate is a library for s3sync binary.

s3sync CLI is a very thin wrapper of the s3sync library. You can use every feature of s3sync CLI in the library.

s3sync library has many features that are not documented. You can refer to the s3sync CLI help(`s3sync -h`) for the features and pass the option strings to the library.

You can refer to the source code bin/cli to implement your own synchronization tool.

Example usage
=============

```Toml
[dependencies]
s3sync = "1.7.1"
tokio = { version = "1.42.0", features = ["full"] }
```

```no_run
use s3sync::config::args::parse_from_args;
use s3sync::config::Config;
use s3sync::pipeline::Pipeline;
use s3sync::types::token::create_pipeline_cancellation_token;
use s3sync::types::SyncStatistics;

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
    let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

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

    println!("Total sync count: {}", total_sync_count);

    // If there is an error in the pipeline, you can get the errors.
    if pipeline.has_error() {
        println!("An error has occurred.\n\n");
        println!("{:?}", pipeline.get_errors_and_consume().unwrap()[0]);
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

pub use config::args::CLIArgs;
pub use config::Config;

pub mod config;
pub mod pipeline;
pub mod storage;
pub mod types;
