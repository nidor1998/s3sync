/*!
# Overview
s3sync is a reliable, flexible, and fast synchronization tool for S3.
It can be used to synchronize local directories with an S3 bucket and synchronize between S3 buckets as well.
It supports multipart uploads, versioning, metadata, and tagging.

## Features
- Reliable:
  s3sync calculates ETag(MD5 or equivalent) for each object and compares them with the ETag in the target.
  An object on the local disk is read from the disk and compared with the checksum in the source or target.
  In the case of S3 to S3, s3sync simply compares ETags that are calculated by S3.
  Optionally, s3sync can also calculate and compare additional checksum (SHA256/SHA1/CRC32/CRC32C/CRC64NVME) for each object.
  s3sync always shows the integrity check result, so you can verify that the synchronization was successful.

- Fast:
  s3sync is implemented in Rust and uses the AWS SDK for Rust, which supports multithreaded asynchronous I/O.
  In my environment(`s3sync 1.45.0(glibc)/c7a.xlarge(4vCPU, 8GB)/200GB IOPS SSD(io 1)`, with 160 workers), uploading from local to S3 achieved about 4,300 objects/sec (small objects 10KiB) and Local to S3 16 objects(6GiB objects, `--max-parallel-uploads 48`) 96.00 GiB, about 256.72 MiB/sec.
  Large objects are transferred in parallel using multipart upload (or get-range request for download), so it can transfer large objects fast.
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
    - Key, `ContentType`, user-defined metadata, and tagging by regular expression.
    - Size and modified time
    - Custom filtering with a Lua script or User-defined callback function (Rust)

- Callbacks support
  - Event callback: You can register a callback to handle events, such as logging, monitoring, or custom actions.
  - Filter callback: You can register a callback to filter objects while listing them in the source.
  - Preprocess callback: You can register a callback to modify the upload metadata before uploading objects to S3.

  You can use these callbacks via Lua scripting or implement your own callback in Rust.

- Robust retry logic
  For long-time running operations, s3sync has a robust original retry logic in addition to AWS SDK's retry logic.

For more information, see [full README](https://github.com/nidor1998/s3sync/blob/main/FULL_README.md).

## As a library
s3sync can be used as a Rust library.
The s3sync CLI is a very thin wrapper over the s3sync library. All CLI features are available in the library.

s3sync library has many features that are not documented. You can refer to the s3sync CLI help(`s3sync -h`) for the features and pass the arguments to the library.

You can refer to the source code bin/cli to implement your own synchronization tool.

**IMPORTANT: This library has many dependencies, so it may be overkill if your sole purpose is to transfer objects. The generated binary becomes unnecessarily large and increases the attack surface.**

**NOTE: s3sync library is assumed to be used like a way that you use s3sync CLI. If you want to control more finely, instead of using s3sync library, we recommend using AWS SDK for Rust or aws-s3-transfer-manager-rs(developer preview) directly.**

**NOTE: Each type of callback is registered only once. Lua scripting support CLI arguments are disabled if you use custom callbacks.**

If you use Lua scripting and `Loading a C module fails with error undefined symbol: lua_xxx` error occurs,
You may need to consider specifying `rustflags = ["-C", "link-args=-rdynamic"]` in your `.cargo/config.toml` file.

Example usage
=============

```Toml
[dependencies]
s3sync = "1"
tokio = { version = "1", features = ["full"] }
```

```no_run
use s3sync::config::Config;
use s3sync::config::args::parse_from_args;
use s3sync::pipeline::Pipeline;
use s3sync::types::token::create_pipeline_cancellation_token;

#[tokio::main]
async fn main() {
    // You can use all the arguments for s3sync CLI.
    // Please refer to `s3sync --help` for more details.
    let args = vec![
        "program_name",
        "./test_data/e2e_test/case1",
        "s3://XXXXXXX-TEST-BUCKET/",
    ];

    // s3sync library converts the arguments to Config.
    // For simplicity, if invalid arguments are passed, this function will panic.
    let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

    // Create a cancellation token for the pipeline.
    // You can use this token to cancel the pipeline.
    let cancellation_token = create_pipeline_cancellation_token();
    let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

    // `stats_sender` is used to get the statistics of the pipeline in real time.
    // You can close `stats_sender` to stop a statistics collection, if needed.
    // Statistics collection consumes some memory, so it is recommended to close it if you don't need it.
    pipeline.close_stats_sender();

    // Run a synchronous pipeline. This function returns after the pipeline is completed.
    pipeline.run().await;

    // If there is an error in the pipeline, you can get the errors.
    if pipeline.has_error() {
        println!("An error has occurred.\n\n");
        println!("{:?}", pipeline.get_errors_and_consume().unwrap()[0]);
    }

    // If there is a warning in the pipeline, you can check it.
    if pipeline.has_warning() {
        println!("A warning has occurred.\n\n");
    }

    // You can get the sync statistics of the pipeline.
    let sync_stats = pipeline.get_sync_stats().await;
    println!(
        "Number of transferred objects: {}",
        sync_stats.stats_transferred_object
    );
}
```

For more examples,
=============
see [s3sync library examples](https://github.com/nidor1998/s3sync/tree/main/examples).

see [s3sync binary source code](https://github.com/nidor1998/s3sync/tree/main/src/bin/s3sync).

see [s3sync integration tests](https://github.com/nidor1998/s3sync/tree/main/tests).

For more information about s3sync CLI,
=============
see [s3sync homepage](https://github.com/nidor1998/s3sync).
*/

#![allow(clippy::collapsible_if)]

pub use config::Config;
pub use config::args::CLIArgs;

pub mod callback;
pub mod config;
pub mod lua;
pub mod pipeline;
pub mod storage;
pub mod types;
