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

        assert!(config.storage_class.is_none());
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
            "--storage-class",
            "STANDARD",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(config.storage_class.unwrap(), StorageClass::Standard);
    }

    #[test]
    fn with_custom_value_with_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--storage-class",
            "STANDARD2",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args);

        assert!(config.is_err());
    }

    #[test]
    fn with_custom_value_with_local_storage_error() {
        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--storage-class",
            "STANDARD",
            "s3://source-bucket/source_key",
            "/xyz/",
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
