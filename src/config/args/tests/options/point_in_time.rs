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

        assert!(config.point_in_time.is_none());
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
            "--point-in-time",
            "2023-10-01T12:00:00Z",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(
            config.point_in_time.unwrap().to_rfc3339(),
            "2023-10-01T12:00:00+00:00"
        );
    }

    #[test]
    fn with_custom_value_with_conflict() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "--point-in-time",
            "2023-10-01T12:00:00Z",
            "./test_data/source/dir1",
            "s3://target-bucket/",
        ];

        let config = build_config_from_args(args);

        assert!(config.is_err());
    }

    #[test]
    fn with_custom_value_conflict_express_onezone() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--point-in-time",
            "2023-10-01T12:00:00Z",
            "s3://bucket--x-s3/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args);

        assert!(config.is_err());
    }

    #[test]
    fn with_invalid_value() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--point-in-time",
            "2030-10-01T12:00:00Z",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args);

        assert!(config.is_err());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
