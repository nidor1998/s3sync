use anyhow::{anyhow, Context, Result};
use async_channel::{Receiver, Sender};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::storage::Storage;
use crate::types::token::PipelineCancellationToken;
use crate::types::{S3syncObject, SyncStatistics};
use crate::Config;

pub struct Stage {
    pub config: Config,
    pub source: Option<Storage>,
    pub target: Option<Storage>,
    pub receiver: Option<Receiver<S3syncObject>>,
    pub sender: Option<Sender<S3syncObject>>,
    pub cancellation_token: PipelineCancellationToken,
    pub has_warning: Arc<AtomicBool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SendResult {
    Success,
    Closed,
}

impl Stage {
    pub fn new(
        config: Config,
        source: Option<Storage>,
        target: Option<Storage>,
        receiver: Option<Receiver<S3syncObject>>,
        sender: Option<Sender<S3syncObject>>,
        cancellation_token: PipelineCancellationToken,
        has_warning: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            source,
            target,
            receiver,
            sender,
            cancellation_token,
            has_warning,
        }
    }

    pub async fn send(&self, object: S3syncObject) -> Result<SendResult> {
        let result = self
            .sender
            .as_ref()
            .unwrap()
            .send(object)
            .await
            .context("async_channel::Sender::send() failed.");

        if let Err(e) = result {
            return if !self.is_channel_closed() {
                Err(anyhow!(e))
            } else {
                Ok(SendResult::Closed)
            };
        }

        Ok(SendResult::Success)
    }

    pub fn is_channel_closed(&self) -> bool {
        self.sender.as_ref().unwrap().is_closed()
    }

    pub async fn send_stats(&self, stats: SyncStatistics) {
        let _ = self
            .target
            .as_ref()
            .unwrap()
            .get_stats_sender()
            .send(stats)
            .await;
    }

    pub fn set_warning(&self) {
        self.has_warning.store(true, Ordering::SeqCst);
    }
}
