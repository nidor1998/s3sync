use anyhow::{Result, anyhow};
use s3sync::Config;
use s3sync::callback::user_defined_event_callback::UserDefinedEventCallback;
use s3sync::callback::user_defined_filter_callback::UserDefinedFilterCallback;
use s3sync::callback::user_defined_preprocess_callback::UserDefinedPreprocessCallback;
use s3sync::pipeline::Pipeline;
use s3sync::types::event_callback::EventType;
use s3sync::types::token::create_pipeline_cancellation_token;
use s3sync::types::{SYNC_REPORT_SUMMERY_NAME, SyncStatsReport};
use std::sync::MutexGuard;
use tokio::time::Instant;
use tracing::{debug, error, info};

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

pub async fn run(mut config: Config) -> Result<()> {
    #[allow(unused_assignments)]
    let mut has_warning = false;

    {
        let cancellation_token = create_pipeline_cancellation_token();

        // Note: Each type of callback is registered only once.
        // The user-defined event callback is disabled by default.
        let mut user_defined_event_callback = UserDefinedEventCallback::new();
        // This is for testing purpose only.
        if config.test_user_defined_callback {
            user_defined_event_callback.enable = true;
        }
        if user_defined_event_callback.is_enabled() {
            // By default, the user-defined event callback notifies all events.
            // You can modify EventType::ALL_EVENTS to filter specific events (e.g., EventType::SYNC_START | EventType::SYNC_COMPLETE)
            config.event_manager.register_callback(
                EventType::ALL_EVENTS,
                user_defined_event_callback,
                config.dry_run,
            );
        }

        // The user-defined filter callback is disabled by default.
        // But you can modify the `UserDefinedFilterCallback` to enable it.
        // User-defined filter callback allows us to filter objects while listing them in the source.
        let mut user_defined_filter_callback = UserDefinedFilterCallback::new();
        // This is for testing purpose only.
        if config.test_user_defined_callback {
            user_defined_filter_callback.enable = true;
        }
        if user_defined_filter_callback.is_enabled() {
            config
                .filter_config
                .filter_manager
                .register_callback(user_defined_filter_callback);
        }

        // The user-defined preprocess callback is disabled by default.
        // But you can modify the `UserDefinedPreprocessCallback` to enable it.
        // User-defined preprocess callback allows us to modify the object attributes dynamically before uploading to S3.
        let mut user_defined_preprocess_callback = UserDefinedPreprocessCallback::new();
        // This is for testing purpose only.
        if config.test_user_defined_callback {
            user_defined_preprocess_callback.enable = true;
        }
        if user_defined_preprocess_callback.is_enabled() {
            config
                .preprocess_manager
                .register_callback(user_defined_preprocess_callback);
        }

        ctrl_c_handler::spawn_ctrl_c_handler(cancellation_token.clone());

        let start_time = Instant::now();
        debug!("sync pipeline start.");

        // When reporting sync status, a sync summary log is not needed.
        let log_sync_summary = !config.report_sync_status;

        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
        let indicator_join_handle = indicator::show_indicator(
            pipeline.get_stats_receiver(),
            ui_config::is_progress_indicator_needed(&config),
            ui_config::is_show_result_needed(&config),
            log_sync_summary,
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

        debug!(duration_sec = duration_sec, "s3sync has been completed.");
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
    use super::*;
    use crate::cli::ui_config::{is_progress_indicator_needed, is_show_result_needed};
    use s3sync::config::args::parse_from_args;
    use std::time::Duration;

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
        assert!(is_show_result_needed(&config));
        assert!(is_progress_indicator_needed(&config));

        let _ = run(config).await;
    }

    #[tokio::test]
    #[cfg(feature = "lua_support")]
    async fn event_callback_lua_script_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
            "--event-callback-lua-script",
            "./test_data/script/invalid_script.lua",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap());
        assert!(config.is_err());
    }

    #[tokio::test]
    #[cfg(feature = "lua_support")]
    async fn filter_callback_lua_script_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
            "--filter-callback-lua-script",
            "./test_data/script/invalid_script.lua",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap());
        assert!(config.is_err());
    }

    #[tokio::test]
    #[cfg(feature = "lua_support")]
    async fn preprocess_callback_lua_script_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
            "--preprocess-callback-lua-script",
            "./test_data/script/invalid_script.lua",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap());

        assert!(config.is_err());
    }

    #[tokio::test]
    #[cfg(e2e_test_dangerous_simulations)]
    async fn test_user_defined_callback() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--test-user-defined-callback",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let _ = run(config).await;
    }

    #[tokio::test]
    async fn run_pipeline_with_report() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--report-sync-status",
            "-vvv",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let _ = run(config).await;
    }

    #[tokio::test]
    async fn run_pipeline_with_show_no_progress() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "--show-no-progress",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        assert!(!is_show_result_needed(&config));
        assert!(!is_progress_indicator_needed(&config));

        let _ = run(config).await;
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
            .with_env_filter("trace")
            .try_init();
    }
}
