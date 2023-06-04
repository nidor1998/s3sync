use ::tracing::trace;
use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;
use rusty_fork::rusty_fork_test;

use s3sync::CLIArgs;
use s3sync::Config;

mod cli;
mod tracing;

#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config_exit_if_err();

    if let Some(shell) = config.auto_complete_shell {
        generate(
            shell,
            &mut CLIArgs::command(),
            "s3sync",
            &mut std::io::stdout(),
        );

        return Ok(());
    }

    start_tracing_if_necessary(&config);

    trace!("config = {:?}", config);

    cli::run(config).await?;

    Ok(())
}

#[cfg(not(tarpaulin_include))]
fn load_config_exit_if_err() -> Config {
    let config = Config::try_from(CLIArgs::parse());
    if let Err(error_message) = config {
        clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message).exit();
    }

    config.unwrap()
}

fn start_tracing_if_necessary(config: &Config) -> bool {
    if config.tracing_config.is_none() {
        return false;
    }

    tracing::init_tracing(config.tracing_config.as_ref().unwrap());
    true
}

rusty_fork_test! {
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
}
