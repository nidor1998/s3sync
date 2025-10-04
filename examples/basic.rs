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
