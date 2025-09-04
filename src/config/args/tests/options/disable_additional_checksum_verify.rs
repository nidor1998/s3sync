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

        assert!(!config.disable_additional_checksum_verify);
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
            "--disable-additional-checksum-verify",
            "--additional-checksum-algorithm",
            "SHA256",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        let config = build_config_from_args(args).unwrap();

        assert!(config.disable_additional_checksum_verify);
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
            "--disable-additional-checksum-verify",
            "s3://source-bucket/source_key",
            "s3://target-bucket/target_key",
        ];

        assert!(build_config_from_args(args).is_err());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
