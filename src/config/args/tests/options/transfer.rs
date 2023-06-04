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

        assert_eq!(config.transfer_config.multipart_threshold, 8 * 1024 * 1024);
        assert_eq!(config.transfer_config.multipart_chunksize, 8 * 1024 * 1024);
        assert!(!config.transfer_config.auto_chunksize);
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
            "--multipart-threshold",
            "20MiB",
            "--multipart-chunksize",
            "50MiB",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(config.transfer_config.multipart_threshold, 20 * 1024 * 1024);
        assert_eq!(config.transfer_config.multipart_chunksize, 50 * 1024 * 1024);
    }

    #[test]
    fn with_auto_chunksize() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--auto-chunksize",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert!(config.transfer_config.auto_chunksize);
    }

    #[test]
    fn with_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--auto-chunksize",
            "--multipart-chunksize",
            "50MiB",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args);

        assert!(config.is_err());
    }

    #[test]
    fn with_conflict_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "--auto-chunksize",
            "./test_data/source/",
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
