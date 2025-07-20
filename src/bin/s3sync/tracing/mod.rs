use std::env;

use rusty_fork::rusty_fork_test;
use tracing_subscriber::fmt::format::FmtSpan;

use s3sync::config::TracingConfig;

const EVENT_FILTER_ENV_VAR: &str = "RUST_LOG";

pub fn init_tracing(config: &TracingConfig) {
    let fmt_span = if config.span_events_tracing {
        FmtSpan::NEW | FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    let subscriber_builder = tracing_subscriber::fmt()
        .compact()
        .with_target(false)
        .with_ansi(!config.disable_color_tracing)
        .with_span_events(fmt_span);

    let mut show_target = true;
    let tracing_level = config.tracing_level;
    let event_filter = if config.aws_sdk_tracing {
        format!(
            "s3sync={tracing_level},aws_smithy_runtime={tracing_level},aws_config={tracing_level},aws_sigv4={tracing_level}"
        )
    } else if env::var(EVENT_FILTER_ENV_VAR).is_ok() {
        env::var(EVENT_FILTER_ENV_VAR).unwrap()
    } else {
        show_target = false;
        format!("s3sync={tracing_level}")
    };

    let subscriber_builder = subscriber_builder
        .with_env_filter(event_filter)
        .with_target(show_target);
    if config.json_tracing {
        subscriber_builder.json().init();
    } else {
        subscriber_builder.init();
    }
}

rusty_fork_test! {
    #[test]
    fn init_json_tracing() {
        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: true,
            aws_sdk_tracing: false,
            span_events_tracing: false,
            disable_color_tracing: false});
    }

    #[test]
    fn init_aws_sdk_tracing() {
        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: false,
            aws_sdk_tracing: true,
            span_events_tracing: false,
            disable_color_tracing: false,
        });
    }

    #[test]
    fn init_normal_tracing() {
        // This code is used to test purpose only and run separated processes.
        unsafe { env::remove_var(EVENT_FILTER_ENV_VAR) };

        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: false,
            aws_sdk_tracing: false,
            span_events_tracing: false,
            disable_color_tracing: false,
        });
    }

    #[test]
    fn init_span_events_tracing() {
        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: false,
            aws_sdk_tracing: true,
            span_events_tracing: true,
            disable_color_tracing: false,
        });
    }

    #[test]
    fn init_disable_color_tracing() {
        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: false,
            aws_sdk_tracing: false,
            span_events_tracing: false,
            disable_color_tracing: true,
        });
    }

    #[test]
    fn init_with_env() {
        // This code is used to test purpose only and run separated processes.
        unsafe { env::set_var(EVENT_FILTER_ENV_VAR, "trace") };

        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: false,
            aws_sdk_tracing: false,
            span_events_tracing: false,
            disable_color_tracing: true,
        });
    }
}
