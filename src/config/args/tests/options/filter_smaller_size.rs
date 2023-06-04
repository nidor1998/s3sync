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

        assert!(config.filter_config.smaller_size.is_none());
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
            "--filter-smaller-size",
            "65536",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(config.filter_config.smaller_size.unwrap(), 65536);
    }

    #[test]
    fn with_custom_value_human_byte() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--filter-smaller-size",
            "8MiB",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(config.filter_config.smaller_size.unwrap(), 8 * 1024 * 1024);
    }

    #[test]
    fn with_custom_value_over_5gib() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--filter-smaller-size",
            "10GiB",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(
            config.filter_config.smaller_size.unwrap(),
            10 * 1024 * 1024 * 1024
        );
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
            "--filter-smaller-size",
            "10x",
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
