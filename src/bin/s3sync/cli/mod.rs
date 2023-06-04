use anyhow::{anyhow, Result};
use tokio::time::Instant;
use tracing::{error, trace};

use s3sync::pipeline::Pipeline;
use s3sync::types::token::create_pipeline_cancellation_token;
use s3sync::Config;

mod ctrl_c_handler;
mod indicator;
mod ui_config;

pub async fn run(config: Config) -> Result<()> {
    let cancellation_token = create_pipeline_cancellation_token();

    ctrl_c_handler::spawn_ctrl_c_handler(cancellation_token.clone());

    let start_time = Instant::now();
    trace!("sync pipeline start.");

    let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
    let indicator_join_handle = indicator::show_indicator(
        pipeline.get_stats_receiver(),
        ui_config::is_progress_indicator_needed(&config),
        ui_config::is_show_result_needed(&config),
        config.dry_run,
    );

    pipeline.run().await;
    indicator_join_handle.await.unwrap();

    let duration_sec = format!("{:.3}", start_time.elapsed().as_secs_f32());
    if pipeline.has_error() {
        error!(duration_sec = duration_sec, "s3sync failed.");

        return Err(anyhow!("s3sync failed."));
    }

    trace!(duration_sec = duration_sec, "s3sync has been completed.");

    Ok(())
}

#[cfg(test)]
mod tests {
    use s3sync::config::args::parse_from_args;

    use super::*;

    #[tokio::test]
    async fn run_pipeline() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        run(config).await.unwrap();
    }

    #[tokio::test]
    async fn run_pipeline_error() {
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
            "s3://invalid_bucket",
            "s3://invalid_bucket2",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        assert!(run(config).await.is_err());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
