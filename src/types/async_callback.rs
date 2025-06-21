use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_channel::Sender;
use leaky_bucket::RateLimiter;
use pin_project::pin_project;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::runtime::Handle;
use tokio::task;

use crate::storage::checksum::AdditionalChecksum;
use crate::types::{ObjectChecksum, SyncStatistics};

#[pin_project]
pub struct AsyncReadWithCallback<R: AsyncRead + Send + Sync> {
    #[pin]
    inner: R,
    stats_sender: Sender<SyncStatistics>,
    bandwidth_limiter: Option<Arc<RateLimiter>>,
    additional_checksum: Option<Arc<AdditionalChecksum>>,
    object_checksum: Option<ObjectChecksum>,
}

impl<R: AsyncRead + Send + Sync> AsyncReadWithCallback<R> {
    pub fn new(
        inner: R,
        stats_sender: Sender<SyncStatistics>,
        bandwidth_limiter: Option<Arc<RateLimiter>>,
        additional_checksum: Option<Arc<AdditionalChecksum>>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Self {
        Self {
            inner,
            stats_sender,
            bandwidth_limiter,
            additional_checksum,
            object_checksum,
        }
    }
}

impl<R: AsyncRead + Send + Sync> AsyncRead for AsyncReadWithCallback<R> {
    #[rustfmt::skip] // For coverage tool incorrectness
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let this = self.project();

        let before = buf.filled().len();

        let result = this.inner.poll_read(cx, buf);
        if !result.is_ready() {
            return result;
        }

        let after = buf.filled().len();

        let sync_bytes = after - before;

        if let Some(bandwidth_limiter) = this.bandwidth_limiter.clone() {
            if 0 < sync_bytes {
                task::block_in_place(move || {
                    Handle::current().block_on(async move {
                        bandwidth_limiter.acquire(sync_bytes).await;
                    });
                });
            }
        }

        if 0 < sync_bytes {
            let _ = this
                .stats_sender
                .send_blocking(SyncStatistics::SyncBytes(sync_bytes as u64));
        }

        result
    }
}

#[cfg(test)]
#[cfg(target_family = "unix")]
mod tests {
    use super::*;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;
    use tracing_subscriber::EnvFilter;

    const TEST_DATA_SIZE: usize = 5;

    #[tokio::test]
    async fn callback_test() {
        init_dummy_tracing_subscriber();

        let file = File::open("test_data/5byte.dat").await.unwrap();
        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let mut file_with_callback =
            AsyncReadWithCallback::new(file, stats_sender, None, None, None);

        let mut buffer = Vec::new();
        file_with_callback.read_to_end(&mut buffer).await.unwrap();

        assert!(!stats_receiver.is_empty());
        assert_eq!(buffer.len(), TEST_DATA_SIZE);
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
