//! LINE WORKS の Bot が招かれたトークルーム (lineworks_channels) の口。rust-alc-api の
//! `crates/alc-notify/src/lineworks_channels.rs` の tenant 側 3 本と同じ path・method・JSON・status
//! (internal 側の bot-secret / event は [`crate::lineworks_webhook`]、send は ippoan/alc-lineworks-worker)。
//!
//! | method | path | 成功 | 失敗 |
//! |---|---|---|---|
//! | GET | `/api/notify/lineworks/channels` | 200 `active` の行 (`joined_at` の新しい順) | 500 `list_failed` |
//! | DELETE | `/api/notify/lineworks/channels/{id}` | 204 (無くても 204) | 500 `delete_failed` |
//! | POST | `/api/notify/lineworks/channels/{id}/test-send` | 200 `{"ok": true}` (`{"text": "..."}` をそのトークルームへ) | 404 `channel_not_found` / 500 `get_failed` / lineworks-worker の断り ([`crate::lineworks::relay_error`]) |
//!
//! test-send は lineworks-worker の `POST /api/internal/lineworks/messages` に、行の bot (`bot_config_id`) と
//! LINE WORKS 側の `channel_id` を渡す。**channel 宛は行の bot**、個人宛 (配信) は tenant の既定の bot
//! (`bot_config_id` を渡さない。CLAUDE.md)。行を読む transaction を閉じてから呼ぶ。

use alc_core_wasm::api_error::{internal_error_msg, not_found, ApiError};
use alc_core_wasm::TenantId;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::lineworks::{LwRequest, LW_MESSAGES_PATH};
use crate::pg::{self, LineworksChannel};
use crate::NotifyState;

pub const CHANNELS_PATH: &str = "/api/notify/lineworks/channels";
pub const CHANNEL_PATH: &str = "/api/notify/lineworks/channels/{id}";
pub const CHANNEL_TEST_SEND_PATH: &str = "/api/notify/lineworks/channels/{id}/test-send";

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(CHANNELS_PATH, get(list))
        .route(CHANNEL_PATH, delete(delete_one))
        .route(CHANNEL_TEST_SEND_PATH, post(test_send))
}

#[derive(Debug, Deserialize)]
pub struct TestSendBody {
    pub text: String,
}

async fn list(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Vec<LineworksChannel>>, ApiError> {
    let mut pg = state.pg.lock().await;
    let rows = pg::list_active_channels(&mut pg, tenant.0)
        .await
        .map_err(|e| {
            state.db_fail("list lineworks channels", &e);
            internal_error_msg("list_failed")
        })?;
    Ok(Json(rows))
}

async fn delete_one(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let mut pg = state.pg.lock().await;
    pg::delete_channel(&mut pg, tenant.0, id)
        .await
        .map_err(|e| {
            state.db_fail("delete lineworks channel", &e);
            internal_error_msg("delete_failed")
        })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn test_send(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<TestSendBody>,
) -> Result<Json<Value>, ApiError> {
    let row = {
        let mut pg = state.pg.lock().await;
        pg::get_channel(&mut pg, tenant.0, id).await.map_err(|e| {
            state.db_fail("get lineworks channel", &e);
            internal_error_msg("get_failed")
        })?
    }
    .ok_or_else(|| not_found("channel_not_found"))?;
    let message = json!({
        "bot_config_id": row.bot_config_id,
        "to": {"channel_id": row.channel_id},
        "content": {"type": "text", "text": body.text},
    });
    let req = LwRequest {
        post: true,
        path: LW_MESSAGES_PATH.to_owned(),
        tenant_id: tenant.0,
        body: Some(message.to_string()),
    };
    state.lineworks_ok("lineworks test send", req).await?;
    Ok(Json(json!({"ok": true})))
}
