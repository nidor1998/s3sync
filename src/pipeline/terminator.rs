use async_channel::Receiver;
use tracing::trace;

#[derive(Debug)]
pub struct Terminator<T> {
    receiver: Receiver<T>,
}

impl<T> Terminator<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        Self { receiver }
    }

    pub async fn terminate(&self) {
        trace!("terminator has started.");

        while self.receiver.recv().await.is_ok() {}

        trace!("terminator has been completed.");
    }
}
