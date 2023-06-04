#[cfg(test)]
mod tests {
    use crate::config::args::*;

    #[test]
    fn with_default_value() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(
            config.tracing_config.as_ref().unwrap().tracing_level,
            log::Level::Warn
        );
        assert!(!config.tracing_config.as_ref().unwrap().json_tracing);
        assert!(!config.tracing_config.as_ref().unwrap().aws_sdk_tracing);
        assert!(!config.tracing_config.as_ref().unwrap().span_events_tracing);
        assert!(
            !config
                .tracing_config
                .as_ref()
                .unwrap()
                .disable_color_tracing
        );
    }

    #[test]
    fn with_custom_value() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "-vvv",
            "--json-tracing",
            "--aws-sdk-tracing",
            "--span-events-tracing",
            "--disable-color-tracing",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(
            config.tracing_config.as_ref().unwrap().tracing_level,
            log::Level::Trace
        );
        assert!(config.tracing_config.as_ref().unwrap().json_tracing);
        assert!(config.tracing_config.as_ref().unwrap().aws_sdk_tracing);
        assert!(config.tracing_config.as_ref().unwrap().span_events_tracing);
        assert!(
            config
                .tracing_config
                .as_ref()
                .unwrap()
                .disable_color_tracing
        );
    }

    #[test]
    fn with_silent_option() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "-qqq",
            "--json-tracing",
            "--aws-sdk-tracing",
            "--span-events-tracing",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert!(config.tracing_config.is_none());
    }

    #[test]
    fn with_silent_option_dry_run() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--dry-run",
            "-qqq",
            "--json-tracing",
            "--aws-sdk-tracing",
            "--span-events-tracing",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert!(config.tracing_config.is_some());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
