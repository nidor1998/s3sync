use s3sync::Config;

pub fn is_progress_indicator_needed(config: &Config) -> bool {
    if config.tracing_config.is_none() {
        return true;
    }

    if log::Level::Warn < config.tracing_config.as_ref().unwrap().tracing_level {
        return false;
    }

    !config.tracing_config.as_ref().unwrap().json_tracing
}

pub fn is_show_result_needed(config: &Config) -> bool {
    if config.tracing_config.is_none() {
        return true;
    }

    if config.report_sync_status {
        return false;
    }

    !config.tracing_config.as_ref().unwrap().json_tracing
}

#[cfg(test)]
mod tests {
    use s3sync::config::args::parse_from_args;

    use super::*;

    #[test]
    fn is_progress_indicator_needed_json_tracing() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "--json-tracing",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(!is_progress_indicator_needed(&config))
    }

    #[test]
    fn is_progress_indicator_needed_no_json_tracing() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(is_progress_indicator_needed(&config))
    }

    #[test]
    fn is_progress_indicator_needed_no_tracing_config() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "-qqq",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(is_progress_indicator_needed(&config))
    }

    #[test]
    fn is_progress_indicator_needed_default() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(is_progress_indicator_needed(&config))
    }

    #[test]
    fn is_progress_indicator_needed_info() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "-v",
            "--source-profile",
            "source_profile",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(!is_progress_indicator_needed(&config))
    }

    #[test]
    fn is_show_result_needed_default() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-profile",
            "source_profile",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(is_show_result_needed(&config))
    }

    #[test]
    fn is_show_result_needed_silent() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "-qqq",
            "--source-profile",
            "source_profile",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(is_show_result_needed(&config))
    }

    #[test]
    fn is_show_result_needed_json_tracing() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "-v",
            "--source-profile",
            "source_profile",
            "--json-tracing",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(!is_show_result_needed(&config))
    }

    #[test]
    fn is_show_result_needed_sync_report() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "-v",
            "--source-profile",
            "source_profile",
            "--report-sync-status",
            "s3://source-bucket",
            "/target-dir",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        assert!(!is_show_result_needed(&config))
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
