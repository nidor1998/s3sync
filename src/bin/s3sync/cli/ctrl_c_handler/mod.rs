use tokio::task::JoinHandle;
use tokio::{select, signal};
use tracing::{debug, error, warn};

use s3sync::types::token::PipelineCancellationToken;

pub fn spawn_ctrl_c_handler(cancellation_token: PipelineCancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        select! {
            _ = cancellation_token.cancelled() => {
                debug!("cancellation_token canceled.")
            }
            result = signal::ctrl_c() => {
                match result {
                    Ok(()) => {
                        warn!("ctrl-c received, shutting down.");
                        cancellation_token.cancel();
                    }
                    Err(e) => {
                        error!("failed to listen for ctrl-c signal: {e}");
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use once_cell::sync::Lazy;
    use tokio::sync::Semaphore;

    use s3sync::types::token;

    use super::*;

    static SEMAPHORE: Lazy<Arc<Semaphore>> = Lazy::new(|| Arc::new(Semaphore::new(1)));

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn ctrl_c_handler_handles_sigint() {
        const WAITING_TIME_MILLIS_FOR_ASYNC_CTRL_C_HANDLER_START: u64 = 100;

        init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let cancellation_token = token::create_pipeline_cancellation_token();

        let join_handle = spawn_ctrl_c_handler(cancellation_token.clone());
        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_CTRL_C_HANDLER_START,
        ))
        .await;

        kill_sigint_to_self();

        join_handle.await.unwrap();

        assert!(cancellation_token.is_cancelled());
    }

    #[tokio::test]
    async fn ctrl_c_handler_handles_cancellation_token() {
        init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let cancellation_token = token::create_pipeline_cancellation_token();

        let join_handle = spawn_ctrl_c_handler(cancellation_token.clone());
        cancellation_token.cancel();

        join_handle.await.unwrap();

        assert!(cancellation_token.is_cancelled());
    }

    #[cfg(target_family = "unix")]
    fn kill_sigint_to_self() {
        nix::sys::signal::kill(nix::unistd::Pid::this(), nix::sys::signal::Signal::SIGINT).unwrap();
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
