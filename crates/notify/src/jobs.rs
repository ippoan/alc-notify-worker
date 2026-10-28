//! 抽出・黒塗りの背景処理のジョブ (Cloudflare Queues `notify-docs` / staging `notify-docs-staging` のメッセージ。W8c-2、
//! Refs ippoan/rust-alc-api#680)。rust は `tokio::spawn` で投げっ放しにしていた (Cloud Run では応答後に CPU が絞られる)。
//!
//! メッセージは `{kind, tenant_id, doc_id, attempt}` だけで、文書の中身は consumer が DB から引く ([`crate::background`])。
//! `attempt` は一時的な失敗で送り直した回数 (workers-rs 0.8 は Queues の試行回数を出さないので本文に持つ。初回は 0)。
//! 送るのは recompute の口 ([`crate::recompute`]) と、upload・ingest (PR-3b) と、一時的な失敗の送り直し (worker の consumer)。

use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    Extract,
    Redact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub kind: JobKind,
    pub tenant_id: Uuid,
    pub doc_id: Uuid,
    #[serde(default)]
    pub attempt: u32,
}

impl Job {
    pub fn new(kind: JobKind, tenant_id: Uuid, doc_id: Uuid) -> Self {
        Self {
            kind,
            tenant_id,
            doc_id,
            attempt: 0,
        }
    }
}

/// 送れなかった (binding が無い・Queues の失敗)。理由は持たない
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueError;

/// queue への送信 (Worker では binding `NOTIFY_DOCS_QUEUE`、テストではモック)
pub trait JobQueue: Send + Sync {
    /// `delay_secs` 秒後に配る (0 ならすぐ)
    fn send(&self, job: Job, delay_secs: u32) -> BoxFuture<'_, Result<(), QueueError>>;
}

pub type SharedQueue = Arc<dyn JobQueue>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_json_shape() {
        let t = Uuid::nil();
        let job = Job::new(JobKind::Redact, t, t);
        assert_eq!(
            serde_json::to_value(&job).unwrap(),
            serde_json::json!({"kind": "redact", "tenant_id": t, "doc_id": t, "attempt": 0})
        );
        // attempt を持たないメッセージも読める
        let back: Job = serde_json::from_str(&format!(
            r#"{{"kind":"extract","tenant_id":"{t}","doc_id":"{t}"}}"#
        ))
        .unwrap();
        assert_eq!(back, Job::new(JobKind::Extract, t, t));
    }
}
