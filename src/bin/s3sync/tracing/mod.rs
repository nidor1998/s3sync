use std::env;
use std::io::{IsTerminal, Write};

use rusty_fork::rusty_fork_test;
use tracing_subscriber::fmt::format::FmtSpan;

use s3sync::config::TracingConfig;

const EVENT_FILTER_ENV_VAR: &str = "RUST_LOG";

/// A writer that silently ignores `BrokenPipe` errors when forwarding to
/// the chosen standard stream, so piping to `head`/`wc`/etc. does not
/// produce noisy `tracing-subscriber` diagnostics or panics on broken
/// pipes (e.g. `s3sync ... | wc -l` followed by Ctrl-C).
enum PipeSafeWriter {
    Stdout,
    Stderr,
}

impl PipeSafeWriter {
    fn write_inner(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            PipeSafeWriter::Stdout => std::io::stdout().write(buf),
            PipeSafeWriter::Stderr => std::io::stderr().write(buf),
        }
    }

    fn flush_inner(&mut self) -> std::io::Result<()> {
        match self {
            PipeSafeWriter::Stdout => std::io::stdout().flush(),
            PipeSafeWriter::Stderr => std::io::stderr().flush(),
        }
    }
}

impl Write for PipeSafeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.write_inner(buf) {
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(buf.len()),
            other => other,
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self.flush_inner() {
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
            other => other,
        }
    }
}

pub fn init_tracing(config: &TracingConfig) {
    let fmt_span = if config.span_events_tracing {
        FmtSpan::NEW | FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    let stderr_tracing = config.stderr_tracing;
    let ansi_enabled = !config.disable_color_tracing
        && if stderr_tracing {
            std::io::stderr().is_terminal()
        } else {
            std::io::stdout().is_terminal()
        };

    let subscriber_builder = tracing_subscriber::fmt()
        .with_writer(move || {
            if stderr_tracing {
                PipeSafeWriter::Stderr
            } else {
                PipeSafeWriter::Stdout
            }
        })
        .compact()
        .with_target(false)
        .with_ansi(ansi_enabled)
        .with_span_events(fmt_span);

    let mut show_target = true;
    let tracing_level = config.tracing_level;
    let event_filter = if config.aws_sdk_tracing {
        format!(
            "s3sync={tracing_level},aws_smithy_runtime={tracing_level},aws_config={tracing_level},aws_sigv4={tracing_level}"
        )
    } else if let Ok(env_filter) = env::var(EVENT_FILTER_ENV_VAR) {
        env_filter
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
            disable_color_tracing: false,
            stderr_tracing: false,
        });
    }

    #[test]
    fn init_aws_sdk_tracing() {
        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: false,
            aws_sdk_tracing: true,
            span_events_tracing: false,
            disable_color_tracing: false,
            stderr_tracing: false,
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
            stderr_tracing: false,
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
            stderr_tracing: false,
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
            stderr_tracing: false,
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
            stderr_tracing: false,
        });
    }

    #[test]
    fn init_stderr_tracing() {
        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: false,
            aws_sdk_tracing: false,
            span_events_tracing: false,
            disable_color_tracing: false,
            stderr_tracing: true,
        });
    }

    #[test]
    fn init_stderr_json_tracing() {
        init_tracing(&TracingConfig {
            tracing_level: log::Level::Info,
            json_tracing: true,
            aws_sdk_tracing: false,
            span_events_tracing: false,
            disable_color_tracing: false,
            stderr_tracing: true,
        });
    }
}
