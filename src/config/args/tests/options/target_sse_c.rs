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

        assert!(config.target_sse_c.is_none());
        assert!(config.target_sse_c_key.key.is_none());
        assert!(config.target_sse_c_key_md5.is_none());
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
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
            "--target-sse-c-key-md5",
            "zZ5FnqcIqUjVwvWmyog4zw==",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(config.target_sse_c.unwrap(), "AES256".to_string());
        assert_eq!(
            config.target_sse_c_key.key.clone().unwrap(),
            "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=".to_string()
        );
        assert_eq!(
            config.target_sse_c_key_md5.unwrap(),
            "zZ5FnqcIqUjVwvWmyog4zw==".to_string()
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
            "--target-sse-c",
            "AES128",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args);

        assert!(config.is_err());
    }

    #[test]
    fn with_custom_value_with_local_storage_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
            "--target-sse-c-key-md5",
            "zZ5FnqcIqUjVwvWmyog4zw==",
            "s3://source-bucket/target_key",
            "./test_data/target",
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
