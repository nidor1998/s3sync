#[cfg(test)]
mod tests {
    use crate::config::args::*;

    #[test]
    fn parse_from_args_both_profile() {
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

        match parse_from_args(args) {
            Ok(config_args) => {
                let (source_config_result, target_config_result) =
                    config_args.build_client_configs(RequestChecksumCalculation::WhenRequired);

                match source_config_result.unwrap().credential {
                    S3Credentials::Profile(profile_name) => {
                        assert_eq!(profile_name, "source_profile".to_string());
                    }
                    _ => {
                        // skipcq: RS-W1021
                        assert!(false, "no source client profile");
                    }
                }

                match target_config_result.unwrap().credential {
                    S3Credentials::Profile(profile_name) => {
                        assert_eq!(profile_name, "target_profile".to_string());
                    }
                    _ => {
                        // skipcq: RS-W1021
                        assert!(false, "no target client profile");
                    }
                }
            }
            _ => {
                // skipcq: RS-W1021
                assert!(false, "error occurred.");
            }
        }
    }

    #[test]
    fn parse_from_args_both_access_keys() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--source-session-token",
            "source_session_token",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "--target-session-token",
            "target_session_token",
            "s3://source-bucket",
            "s3://target-bucket",
        ];

        match parse_from_args(args) {
            Ok(config_args) => {
                let (source_config_result, target_config_result) =
                    config_args.build_client_configs(RequestChecksumCalculation::WhenRequired);

                match source_config_result.unwrap().credential {
                    S3Credentials::Credentials { access_keys } => {
                        assert_eq!(access_keys.access_key, "source_access_key".to_string());
                        assert_eq!(
                            access_keys.secret_access_key,
                            "source_secret_access_key".to_string()
                        );
                        assert_eq!(
                            access_keys.session_token,
                            Some("source_session_token".to_string())
                        );
                    }
                    _ => {
                        // skipcq: RS-W1021
                        assert!(false, "no source credential");
                    }
                }

                match target_config_result.unwrap().credential {
                    S3Credentials::Credentials { access_keys } => {
                        assert_eq!(access_keys.access_key, "target_access_key".to_string());
                        assert_eq!(
                            access_keys.secret_access_key,
                            "target_secret_access_key".to_string()
                        );
                        assert_eq!(
                            access_keys.session_token,
                            Some("target_session_token".to_string())
                        );
                    }
                    _ => {
                        // skipcq: RS-W1021
                        assert!(false, "no target credential");
                    }
                }
            }
            _ => {
                // skipcq: RS-W1021
                assert!(false, "error occurred.");
            }
        }
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
