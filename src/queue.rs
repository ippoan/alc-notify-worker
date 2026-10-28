//! crates/notify の [`JobQueue`] の Workers での実装: Cloudflare Queues の producer の binding `NOTIFY_DOCS_QUEUE`
//! (本番 `notify-docs`・staging `notify-docs-staging`。W8c-2、Refs ippoan/rust-alc-api#680)。
//! consumer は同じ worker の `#[event(queue)]` (src/lib.rs)。
//!
//! 値と future は JS の値で `Send` でないので、`worker::send` の `SendWrapper`・`SendFuture` で包む (Workers は単一 thread)。
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging の実物)。

use futures_util::future::BoxFuture;
use notify::jobs::{Job, JobQueue, QueueError};
use worker::send::{SendFuture, SendWrapper};
use worker::{MessageBuilder, Queue};

pub struct QueueBinding(SendWrapper<Queue>);

impl QueueBinding {
    pub fn new(queue: Queue) -> Self {
        Self(SendWrapper::new(queue))
    }
}

impl JobQueue for QueueBinding {
    fn send(&self, job: Job, delay_secs: u32) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(SendFuture::new(async move {
            let message = MessageBuilder::new(job).delay_seconds(delay_secs).build();
            self.0.send(message).await.map_err(|_| QueueError)
        }))
    }
}

/// binding が無い env (recompute は 500)
pub struct NoQueue;

impl JobQueue for NoQueue {
    fn send(&self, _: Job, _: u32) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async { Err(QueueError) })
    }
}
