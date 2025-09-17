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

        assert!(!config.copy_source_if_match)
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
            "--server-side-copy",
            "--copy-source-if-match",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();
        assert!(config.copy_source_if_match);
    }

    #[test]
    fn with_custom_value_source_local_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "--copy-source-if-match",
            "./test_data/source/",
            "s3://source-bucket/source_key",
        ];

        let config = build_config_from_args(args);
        assert!(config.is_err());
    }

    #[test]
    fn with_custom_value_target_local_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--copy-source-if-match",
            "s3://source-bucket/source_key",
            "./test_data/target/",
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
