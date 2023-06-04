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

        assert!(!config.dry_run);
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
            "--dry-run",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert!(config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Info
        );
        assert!(!config.tracing_config.unwrap().json_tracing);
        assert!(!config.tracing_config.unwrap().aws_sdk_tracing);
        assert!(!config.tracing_config.unwrap().span_events_tracing);
        assert!(!config.tracing_config.unwrap().disable_color_tracing);
    }

    #[test]
    fn with_custom_value_with_tracing_option() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--dry-run",
            "-vvv",
            "--json-tracing",
            "--aws-sdk-tracing",
            "--span-events-tracing",
            "--disable-color-tracing",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert!(config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Trace
        );
        assert!(config.tracing_config.unwrap().json_tracing);
        assert!(config.tracing_config.unwrap().aws_sdk_tracing);
        assert!(config.tracing_config.unwrap().span_events_tracing);
        assert!(config.tracing_config.unwrap().disable_color_tracing);
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
