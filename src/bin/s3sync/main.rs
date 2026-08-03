use ::tracing::trace;
use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use clap_complete::generate;
use rusty_fork::rusty_fork_test;

use s3sync::CLIArgs;
use s3sync::Config;

mod cli;
mod pipe_safe;
mod tracing;

#[cfg_attr(coverage_nightly, coverage(off))]
#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config_exit_if_err();

    if let Some(shell) = config.auto_complete_shell {
        return print_completion_script(shell);
    }

    start_tracing_if_necessary(&config);

    trace!("config = {:?}", config);

    cli::run(config).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn load_config_exit_if_err() -> Config {
    let config = Config::try_from(CLIArgs::parse());
    if let Err(error_message) = config {
        clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message).exit();
    }

    // If report_sync_status is enabled, set dry_run to true to show report
    let mut config = config.unwrap();
    if config.report_sync_status {
        config.dry_run = true;
    }
    config
}

/// Render the shell-completion script for `shell` and print it pipe-safely.
///
/// clap_complete's generators panic on writer errors ("failed to write
/// completion file"), so they never touch stdout directly: the script is
/// rendered into an infallible in-memory buffer and written in one
/// pipe-safe pass. A reader that exits early
/// (`s3sync --auto-complete-shell bash | head 1`) yields exit 0; any other
/// stdout write error fails the command.
fn print_completion_script(shell: clap_complete::shells::Shell) -> Result<()> {
    let mut script = Vec::new();
    generate(shell, &mut CLIArgs::command(), "s3sync", &mut script);
    pipe_safe::write_all_pipe_safe(&script).context("failed to write completion script")
}

fn start_tracing_if_necessary(config: &Config) -> bool {
    if config.tracing_config.is_none() {
        return false;
    }

    tracing::init_tracing(config.tracing_config.as_ref().unwrap());
    true
}

rusty_fork_test! {
    // Runs in a forked child so the completion script written to the real
    // stdout does not pollute the test harness output. The closed-pipe
    // behavior itself is pinned process-level in tests/cli_broken_pipe.rs.
    #[test]
    fn print_completion_script_succeeds() {
        print_completion_script(clap_complete::shells::Shell::Bash).unwrap();
    }

    #[test]
    fn with_tracing() {
        let args = vec![
            "unittest",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "s3://source-bucket",
            "s3://target-bucket",
        ];

        let config = s3sync::Config::try_from(CLIArgs::try_parse_from(args).unwrap()).unwrap();
        assert!(start_tracing_if_necessary(&config));
    }

    #[test]
    fn without_tracing() {
        let args = vec![
            "unittest",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "-qq",
            "s3://source-bucket",
            "s3://target-bucket",
        ];

        let config = s3sync::Config::try_from(CLIArgs::try_parse_from(args).unwrap()).unwrap();
        assert!(!start_tracing_if_necessary(&config));
    }

    #[test]
    fn with_tracing_sync_report() {
        let args = vec![
            "unittest",
            "--source-profile",
            "source_profile",
            "--target-profile",
            "target_profile",
            "--report-sync-status",
            "s3://source-bucket",
            "s3://target-bucket",
        ];

        let config = s3sync::Config::try_from(CLIArgs::try_parse_from(args).unwrap()).unwrap();
        assert!(start_tracing_if_necessary(&config));
    }
}
