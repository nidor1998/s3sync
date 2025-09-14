use async_channel::Receiver;
use tracing::debug;

#[derive(Debug)]
pub struct Terminator<T> {
    receiver: Receiver<T>,
}

impl<T> Terminator<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        Self { receiver }
    }

    pub async fn terminate(&self) {
        debug!("terminator has started.");

        while self.receiver.recv().await.is_ok() {}

        debug!("terminator has been completed.");
    }
}
