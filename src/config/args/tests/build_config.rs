#[cfg(test)]
mod tests {
    use crate::config::args::*;

    #[test]
    fn build_from_profile_with_default_value() {
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

        if let StoragePath::S3 { bucket, prefix } = config.source {
            assert_eq!(bucket, "source-bucket".to_string());
            assert_eq!(prefix, "source_key".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }

        if let StoragePath::S3 { bucket, prefix } = config.target {
            assert_eq!(bucket, "target-bucket".to_string());
            assert_eq!(prefix, "target_key".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }

        assert!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .client_config_location
                .aws_config_file
                .is_none()
        );
        assert!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .client_config_location
                .aws_shared_credentials_file
                .is_none()
        );

        if let S3Credentials::Profile(profile_name) =
            &config.source_client_config.as_ref().unwrap().credential
        {
            assert_eq!(profile_name.to_string(), "source_profile".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "source profile not found")
        }

        assert!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .region
                .is_none()
        );
        assert!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .endpoint_url
                .is_none()
        );
        assert!(
            !config
                .source_client_config
                .as_ref()
                .unwrap()
                .force_path_style
        );

        assert_eq!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .retry_config
                .aws_max_attempts,
            10
        );
        assert_eq!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .retry_config
                .initial_backoff_milliseconds,
            100
        );

        assert!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .client_config_location
                .aws_config_file
                .is_none()
        );
        assert!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .client_config_location
                .aws_shared_credentials_file
                .is_none()
        );

        if let S3Credentials::Profile(profile_name) =
            &config.target_client_config.as_ref().unwrap().credential
        {
            assert_eq!(profile_name.to_string(), "target_profile".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "target profile not found")
        }

        assert!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .region
                .is_none()
        );
        assert!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .endpoint_url
                .is_none()
        );
        assert!(
            !config
                .target_client_config
                .as_ref()
                .unwrap()
                .force_path_style
        );

        assert_eq!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .retry_config
                .aws_max_attempts,
            10
        );
        assert_eq!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .retry_config
                .initial_backoff_milliseconds,
            100
        );
    }

    #[test]
    fn build_from_credentials_with_custom_value() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--aws-config-file",
            "~/.aws/config",
            "--aws-shared-credentials-file",
            "~/.aws/credentials",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--source-session-token",
            "source_session_token",
            "--source-region",
            "source_region",
            "--source-endpoint-url",
            "https://source.endpoint.local",
            "--source-force-path-style",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--target-session-token",
            "target_session_token",
            "--target-region",
            "target_region",
            "--target-endpoint-url",
            "https://target.endpoint.local",
            "--target-force-path-style",
            "--aws-max-attempts",
            "30",
            "--initial-backoff-milliseconds",
            "3000",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert_eq!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .client_config_location
                .aws_config_file,
            Some(PathBuf::from("~/.aws/config"))
        );
        assert_eq!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .client_config_location
                .aws_shared_credentials_file,
            Some(PathBuf::from("~/.aws/credentials"))
        );

        if let S3Credentials::Credentials { access_keys } =
            &config.source_client_config.as_ref().unwrap().credential
        {
            assert_eq!(access_keys.access_key, "source_access_key".to_string());
            assert_eq!(
                access_keys.secret_access_key,
                "source_secret_access_key".to_string()
            );
            assert_eq!(
                access_keys.session_token,
                Some("source_session_token".to_string())
            );
        } else {
            // skipcq: RS-W1021
            assert!(false, "source access keys not found")
        }
        assert_eq!(
            config.source_client_config.as_ref().unwrap().region,
            Some("source_region".to_string())
        );
        assert_eq!(
            config.source_client_config.as_ref().unwrap().endpoint_url,
            Some("https://source.endpoint.local".to_string())
        );
        assert!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .force_path_style
        );
        assert_eq!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .retry_config
                .aws_max_attempts,
            30
        );
        assert_eq!(
            config
                .source_client_config
                .as_ref()
                .unwrap()
                .retry_config
                .initial_backoff_milliseconds,
            3000
        );

        if let S3Credentials::Credentials { access_keys } =
            &config.target_client_config.as_ref().unwrap().credential
        {
            assert_eq!(access_keys.access_key, "target_access_key".to_string());
            assert_eq!(
                access_keys.secret_access_key,
                "target_secret_access_key".to_string()
            );
            assert_eq!(
                access_keys.session_token,
                Some("target_session_token".to_string())
            );
        } else {
            // skipcq: RS-W1021
            assert!(false, "target access keys not found")
        }
        assert_eq!(
            config.target_client_config.as_ref().unwrap().region,
            Some("target_region".to_string())
        );
        assert_eq!(
            config.target_client_config.as_ref().unwrap().endpoint_url,
            Some("https://target.endpoint.local".to_string())
        );
        assert!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .force_path_style
        );
        assert_eq!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .retry_config
                .aws_max_attempts,
            30
        );
        assert_eq!(
            config
                .target_client_config
                .as_ref()
                .unwrap()
                .retry_config
                .initial_backoff_milliseconds,
            3000
        );
    }

    #[test]
    fn build_from_both_s3() {
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

        let config = build_config_from_args(args).unwrap();

        if let StoragePath::S3 { bucket, prefix } = config.source {
            assert_eq!(bucket, "source-bucket".to_string());
            assert_eq!(prefix, "".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }

        if let StoragePath::S3 { bucket, prefix } = config.target {
            assert_eq!(bucket, "target-bucket".to_string());
            assert_eq!(prefix, "".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn build_from_source_s3() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "s3://source-bucket",
            "target-dir",
        ];

        let config = build_config_from_args(args).unwrap();

        if let StoragePath::S3 { bucket, prefix } = config.source {
            assert_eq!(bucket, "source-bucket".to_string());
            assert_eq!(prefix, "".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }

        if let StoragePath::Local(path) = config.target {
            assert_eq!(path.to_str().unwrap(), "target-dir".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn build_from_target_s3() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "./test_data/source",
            "s3://target-bucket",
        ];

        let config = build_config_from_args(args).unwrap();

        if let StoragePath::Local(path) = config.source {
            assert_eq!(path.to_str().unwrap(), "./test_data/source".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }

        if let StoragePath::S3 { bucket, prefix } = config.target {
            assert_eq!(bucket, "target-bucket".to_string());
            assert_eq!(prefix, "".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn build_from_target_s3() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            ".\\test_data\\source",
            "s3://target-bucket",
        ];

        let config = build_config_from_args(args).unwrap();

        if let StoragePath::Local(path) = config.source {
            assert_eq!(path.to_str().unwrap(), ".\\test_data\\source\\".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }

        if let StoragePath::S3 { bucket, prefix } = config.target {
            assert_eq!(bucket, "target-bucket".to_string());
            assert_eq!(prefix, "".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn build_from_local_end_with_slash() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "./test_data/source/",
            "s3://target-bucket",
        ];

        let config = build_config_from_args(args).unwrap();

        if let StoragePath::Local(path) = config.source {
            assert_eq!(path.to_str().unwrap(), "./test_data/source/".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }

        if let StoragePath::S3 { bucket, prefix } = config.target {
            assert_eq!(bucket, "target-bucket".to_string());
            assert_eq!(prefix, "".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn build_from_crc64nvme() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "--additional-checksum-algorithm",
            "CRC64NVME",
            "./test_data/source/",
            "s3://target-bucket",
        ];

        let config = build_config_from_args(args).unwrap();
        assert!(config.full_object_checksum)
    }

    #[test]
    fn build_from_crc32() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "--additional-checksum-algorithm",
            "CRC32",
            "./test_data/source/",
            "s3://target-bucket",
        ];

        let config = build_config_from_args(args).unwrap();
        assert!(!config.full_object_checksum)
    }

    #[test]
    fn build_from_source_express_onezone() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "s3://bucket--x-s3",
            "./test_data/target",
        ];
        let _ = build_config_from_args(args).unwrap();
    }

    #[test]
    fn build_from_target_express_onezone() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            "./test_data/source",
            "s3://bucket--x-s3",
        ];
        let _ = build_config_from_args(args).unwrap();
    }

    #[test]
    fn build_from_both_express_onezone() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "s3://bucket1--x-s3",
            "s3://bucket2--x-s3",
        ];
        let _ = build_config_from_args(args).unwrap();
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn build_from_local_end_with_back_slash() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-profile",
            "target_profile",
            ".\\test_data\\source\\",
            "s3://target-bucket",
        ];

        let config = build_config_from_args(args).unwrap();

        if let StoragePath::Local(path) = config.source {
            assert_eq!(path.to_str().unwrap(), ".\\test_data\\source\\".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }

        if let StoragePath::S3 { bucket, prefix } = config.target {
            assert_eq!(bucket, "target-bucket".to_string());
            assert_eq!(prefix, "".to_string());
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn build_from_invalid_source() {
        init_dummy_tracing_subscriber();

        let args = vec!["s3sync", "./invalid_source", "s3://target-bucket"];

        if let Err(error_message) = build_config_from_args(args) {
            assert_eq!(
                error_message,
                SOURCE_LOCAL_STORAGE_PATH_NOT_FOUND.to_string()
            );
            return;
        }

        // skipcq: RS-W1021
        assert!(false, "no error occurred");
    }

    #[test]
    fn build_from_invalid_both_local() {
        init_dummy_tracing_subscriber();

        let args = vec!["s3sync", "./test_data/source", "./test_data/target"];

        if let Err(error_message) = build_config_from_args(args) {
            assert_eq!(error_message, NO_S3_STORAGE_SPECIFIED.to_string());
            return;
        }

        // skipcq: RS-W1021
        assert!(false, "no error occurred");
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
