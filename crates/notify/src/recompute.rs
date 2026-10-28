//! 抽出・黒塗りのやり直しの口 (W8c-2。rust-alc-api の `crates/alc-notify/src/documents.rs` の redact_recompute・
//! extract_recompute の写し。Refs ippoan/rust-alc-api#680)。
//!
//! - `POST /api/notify/documents/{id}/redact-recompute` — 黒塗りの結果を空にして `pending` に戻し、queue へ積む。202
//! - `POST /api/notify/documents/{id}/extract-recompute` — `extraction_status` を `pending` に戻し (失敗はログだけで続ける)、
//!   queue へ積む。202
//!
//! 文書が無ければ 404、DB の失敗は 500 (本文なし)。結果は状態の列を画面が polling する (黒塗りは WS の push も)。
//! rust との違い: queue に積めなければ 500 (rust の `tokio::spawn` は失敗しない)。

use alc_core_wasm::TenantId;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Router};
use uuid::Uuid;

use crate::jobs::{Job, JobKind};
use crate::pg;
use crate::NotifyState;

pub const REDACT_RECOMPUTE_PATH: &str = "/api/notify/documents/{id}/redact-recompute";
pub const EXTRACT_RECOMPUTE_PATH: &str = "/api/notify/documents/{id}/extract-recompute";

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(REDACT_RECOMPUTE_PATH, post(redact_recompute))
        .route(EXTRACT_RECOMPUTE_PATH, post(extract_recompute))
}

async fn exists(
    state: &NotifyState,
    what: &str,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<(), StatusCode> {
    let doc = {
        let mut pg = state.pg.lock().await;
        pg::get_document(&mut pg, tenant_id, id).await
    };
    match doc {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => Err(state.db_fail(what, &e)),
    }
}

async fn enqueue(state: &NotifyState, what: &str, job: Job) -> Result<StatusCode, StatusCode> {
    match state.jobs.send(job, 0).await {
        Ok(()) => Ok(StatusCode::ACCEPTED),
        Err(_) => {
            (state.log)(&format!("notify: {what} failed: queue"));
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn redact_recompute(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    exists(&state, "redact_recompute", tenant_id, id).await?;
    let r = {
        let mut pg = state.pg.lock().await;
        pg::reset_redaction(&mut pg, tenant_id, id).await
    };
    r.map_err(|e| state.db_fail("reset_redaction", &e))?;
    enqueue(
        &state,
        "redact_recompute",
        Job::new(JobKind::Redact, tenant_id, id),
    )
    .await
}

async fn extract_recompute(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    exists(&state, "extract_recompute", tenant_id, id).await?;
    // 戻せなくても続ける (rust と同じ。背景処理が終わりに上書きする)
    let r = {
        let mut pg = state.pg.lock().await;
        pg::reset_extraction(&mut pg, tenant_id, id).await
    };
    state.best_effort("reset_extraction", r);
    enqueue(
        &state,
        "extract_recompute",
        Job::new(JobKind::Extract, tenant_id, id),
    )
    .await
}
