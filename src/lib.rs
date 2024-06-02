/*!
# Overview
s3sync is a reliable, very fast, and powerful synchronization tool for S3.
It can be used to synchronize local directories with S3 bucket, and also to synchronize S3 to s3 bucket.
Supports multipart upload, versioning, metadata.

AWS SDK for rust is not production ready yet and is not recommended for production use, so s3sync is also not recommended for production use. But any feedback is welcome. I will continue to improve s3sync.

This crate is a library for s3sync binary. But you can use it to build your own s3sync-like tool.

Example usage
=============

```Toml
[dependencies]
s3sync = "1.4.0"
tokio = { version = "1.37.0", features = ["full"] }
```

```no_run
use s3sync::config::args::parse_from_args;
use s3sync::config::Config;
use s3sync::pipeline::Pipeline;
use s3sync::types::token::create_pipeline_cancellation_token;

#[tokio::main]
async fn main() {
    // You can use all the arguments for s3sync binary here.
    let args = vec!["program_name", "./src", "s3://test-bucket/src/"];

    // s3sync library converts the arguments to Config.
    let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

    // Create a cancellation token for the pipeline.
    // You can use this token to cancel the pipeline.
    let cancellation_token = create_pipeline_cancellation_token();
    let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

    // You can close statistics sender to stop statistics collection, if needed.
    // Statistics collection consumes some Memory, so it is recommended to close it if you don't need it.
    pipeline.close_stats_sender();

    // Run the pipeline. In this simple example, we run the pipeline synchronously.
    pipeline.run().await;

    assert!(!pipeline.has_error());
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
