use anyhow::{anyhow, Result};
use std::sync::MutexGuard;
use tokio::time::Instant;
use tracing::{error, info, trace};

use s3sync::pipeline::Pipeline;
use s3sync::types::token::create_pipeline_cancellation_token;
use s3sync::types::{SyncStatsReport, SYNC_REPORT_SUMMERY_NAME};
use s3sync::Config;

mod ctrl_c_handler;
mod indicator;
mod ui_config;

#[allow(dead_code)]
const EXIT_CODE_SUCCESS: i32 = 0;
#[allow(dead_code)]
const EXIT_CODE_ERROR: i32 = 1;
#[allow(dead_code)]
const EXIT_CODE_INVALID_ARGS: i32 = 2;
const EXIT_CODE_WARNING: i32 = 3;

pub async fn run(config: Config) -> Result<()> {
    #[allow(unused_assignments)]
    let mut has_warning = false;

    {
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
        indicator_join_handle.await?;

        let duration_sec = format!("{:.3}", start_time.elapsed().as_secs_f32());
        if pipeline.has_error() {
            error!(duration_sec = duration_sec, "s3sync failed.");

            return Err(anyhow!("s3sync failed."));
        }

        has_warning = pipeline.has_warning();

        if config.report_sync_status {
            show_sync_report_summary(pipeline.get_sync_stats_report().lock().unwrap());
        }

        trace!(duration_sec = duration_sec, "s3sync has been completed.");
    }

    if has_warning {
        std::process::exit(EXIT_CODE_WARNING);
    }

    Ok(())
}

fn show_sync_report_summary(sync_stats_report: MutexGuard<'_, SyncStatsReport>) {
    info!(
        name = SYNC_REPORT_SUMMERY_NAME,
        number_of_objects = sync_stats_report.number_of_objects,
        etag_matches = sync_stats_report.etag_matches,
        checksum_matches = sync_stats_report.checksum_matches,
        metadata_matches = sync_stats_report.metadata_matches,
        tagging_matches = sync_stats_report.tagging_matches,
        not_found = sync_stats_report.not_found,
        etag_mismatch = sync_stats_report.etag_mismatch,
        checksum_mismatch = sync_stats_report.checksum_mismatch,
        metadata_mismatch = sync_stats_report.metadata_mismatch,
        tagging_mismatch = sync_stats_report.tagging_mismatch,
        etag_unknown = sync_stats_report.etag_unknown,
        checksum_unknown = sync_stats_report.checksum_unknown,
    );
}

#[cfg(test)]
mod tests {
    use s3sync::config::args::parse_from_args;
    use std::time::Duration;

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

        // wait for signal test to finish
        tokio::time::sleep(Duration::from_millis(5000)).await;

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
