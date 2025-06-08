#[cfg(test)]
mod tests {
    use crate::config::args::*;

    #[test]
    fn validate_storage_config_source_s3() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "s3://source-bucket",
            "./test_data/target",
        ];

        if let Ok(config_args) = parse_from_args(args) {
            assert!(config_args.validate_storage_config().is_ok());
        } else {
            assert!(false, "error occurred.");
        }
    }

    #[test]
    fn validate_storage_config_target_s3() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "./test_data/source",
            "s3://target-bucket",
        ];

        if let Ok(config_args) = parse_from_args(args) {
            assert!(config_args.validate_storage_config().is_ok());
        } else {
            assert!(false, "error occurred.");
        }
    }

    #[test]
    fn validate_storage_config_both_s3() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "s3://source-bucket",
            "s3://target-bucket",
        ];

        if let Ok(config_args) = parse_from_args(args) {
            assert!(config_args.validate_storage_config().is_ok());
        } else {
            assert!(false, "error occurred.");
        }
    }

    #[test]
    fn validate_storage_config_both_storage_local() {
        init_dummy_tracing_subscriber();

        let args = vec!["s3sync", "/source-dir", "./test_data/target"];

        if let Ok(config_args) = parse_from_args(args) {
            assert!(config_args.validate_storage_config().is_err());
        } else {
            assert!(false, "error occurred.");
        }
    }

    #[test]
    fn validate_storage_config_source_credential_not_required() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "./test_data/source",
            "s3://target-bucket",
        ];

        if let Ok(config_args) = parse_from_args(args) {
            let result = config_args.validate_storage_config();
            assert!(result.is_err());
            if let Err(e) = result {
                assert_eq!(e, NO_SOURCE_CREDENTIAL_REQUIRED.to_string());
            }
        } else {
            assert!(false, "error occurred.");
        }
    }

    #[test]
    fn validate_storage_config_target_credential_not_required() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "s3://source-bucket",
            "./test_data/target",
        ];

        if let Ok(config_args) = parse_from_args(args) {
            let result = config_args.validate_storage_config();
            assert!(result.is_err());
            if let Err(e) = result {
                assert_eq!(e, NO_TARGET_CREDENTIAL_REQUIRED.to_string());
            }
        } else {
            assert!(false, "error occurred.");
        }
    }

    #[test]
    fn validate_storage_config_both_endpoint_url() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-endpoint-url",
            "https://localhost:9000",
            "--target-endpoint-url",
            "https://localhost:9000",
            "s3://source-bucket",
            "s3://target-bucket",
        ];

        if let Ok(config_args) = parse_from_args(args) {
            assert!(config_args.validate_storage_config().is_ok());
        } else {
            assert!(false, "error occurred.");
        }
    }

    #[test]
    fn validate_storage_config_source_local_endpoint_url() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-endpoint-url",
            "https://localhost:9000",
            "--target-endpoint-url",
            "https://localhost:9000",
            "./test_data/source",
            "s3://target-bucket",
        ];

        if let Ok(config_args) = parse_from_args(args) {
            let result = config_args.validate_storage_config();
            assert!(result.is_err());
            if let Err(e) = result {
                assert_eq!(
                    e,
                    SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL.to_string()
                );
            }
        } else {
            assert!(false, "error occurred.");
        }
    }

    #[test]
    fn validate_storage_config_target_local_endpoint_url() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-endpoint-url",
            "https://localhost:9000",
            "--target-endpoint-url",
            "https://localhost:9000",
            "s3://source-bucket",
            "./test_data/target",
        ];

        if let Ok(config_args) = parse_from_args(args) {
            let result = config_args.validate_storage_config();
            assert!(result.is_err());
            if let Err(e) = result {
                assert_eq!(
                    e,
                    TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL.to_string()
                );
            }
        } else {
            assert!(false, "error occurred.");
        }
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
