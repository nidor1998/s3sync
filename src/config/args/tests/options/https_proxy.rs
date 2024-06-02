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

        assert!(config.source_client_config.unwrap().https_proxy.is_none());
        assert!(config.target_client_config.unwrap().https_proxy.is_none());
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
            "--https-proxy",
            "http://localhost:8080",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(
            config.source_client_config.unwrap().https_proxy.unwrap(),
            "http://localhost:8080".to_string()
        );
        assert_eq!(
            config.target_client_config.unwrap().https_proxy.unwrap(),
            "http://localhost:8080".to_string()
        );
    }

    #[test]
    fn with_custom_value_auth() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--https-proxy",
            "http://user:password@localhost:8080",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(
            config.source_client_config.unwrap().https_proxy.unwrap(),
            "http://user:password@localhost:8080".to_string()
        );
        assert_eq!(
            config.target_client_config.unwrap().https_proxy.unwrap(),
            "http://user:password@localhost:8080".to_string()
        );
    }

    #[test]
    fn with_custom_value_error() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--https-proxy",
            "ftp://localhost:8080",
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
