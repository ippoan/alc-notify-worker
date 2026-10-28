//! LINE WORKS の webhook の受け口 (W8b、Refs ippoan/rust-alc-api#680)。rust-alc-api の
//! `crates/alc-notify/src/lineworks_channels.rs` の internal の 2 本と同じ path・method・JSON・status。
//!
//! 呼び手は auth-worker の Durable Object `LineworksWebhookDO` (bot_id ごとに 1 つ)。DO が LINE WORKS の webhook を受けて
//! HMAC を検証し、イベントをキューに積んで再送する。この 2 本は tenant のヘッダーを持たない ([`crate::router`] の tenant の
//! layer の外)。bot_id から tenant を引くのは SECURITY DEFINER の関数 ([`pg::lookup_bot_for_webhook`]。テナントが決まる前なので
//! nil のテナントの transaction で呼ぶ)。
//!
//! | method | path | 成功 | 失敗 |
//! |---|---|---|---|
//! | GET | `/api/internal/lineworks/bot-secret/{bot_id}` | 200 `{"bot_secret_encrypted": "..."}` (復号は DO) | 404 `bot_not_found` / 404 `bot_secret_not_configured` / 500 `lookup_failed` |
//! | POST | `/api/internal/lineworks/event` | 200 `{"ok": true}` (`join`/`joined` → トークルームを足す、`leave`/`left` → 外す、ほかと `channel_id` なしは何もしない) | 404 `bot_not_found` / 500 `lookup_failed`・`upsert_failed`・`mark_left_failed` |
//!
//! rust との違い: 外す (`leave`) の UPDATE は `WHERE tenant_id` でも絞る (RLS の下では同じ行になる)。

use alc_core_wasm::api_error::{internal_error_msg, not_found, ApiError};
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::pg::{self, BotForWebhook};
use crate::NotifyState;

pub const BOT_SECRET_PATH: &str = "/api/internal/lineworks/bot-secret/{bot_id}";
pub const EVENT_PATH: &str = "/api/internal/lineworks/event";

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(BOT_SECRET_PATH, get(bot_secret))
        .route(EVENT_PATH, post(event))
}

#[derive(Debug, Serialize)]
pub struct BotSecretResponse {
    pub bot_secret_encrypted: String,
}

/// auth-worker の DO が送る、検証済みのイベント (rust の `InternalEventBody`)
#[derive(Debug, Deserialize)]
pub struct EventBody {
    pub bot_id: String,
    pub event_type: String,
    pub channel_id: Option<String>,
    pub channel_type: Option<String>,
    pub title: Option<String>,
}

async fn lookup(state: &NotifyState, bot_id: String) -> Result<BotForWebhook, ApiError> {
    let mut pg = state.pg.lock().await;
    pg::lookup_bot_for_webhook(&mut pg, bot_id)
        .await
        .map_err(|e| {
            state.db_fail("lookup lineworks bot", &e);
            internal_error_msg("lookup_failed")
        })?
        .ok_or_else(|| not_found("bot_not_found"))
}

async fn bot_secret(
    State(state): State<NotifyState>,
    Path(bot_id): Path<String>,
) -> Result<Json<BotSecretResponse>, ApiError> {
    let bot = lookup(&state, bot_id).await?;
    let bot_secret_encrypted = bot
        .bot_secret_encrypted
        .ok_or_else(|| not_found("bot_secret_not_configured"))?;
    Ok(Json(BotSecretResponse {
        bot_secret_encrypted,
    }))
}

async fn event(
    State(state): State<NotifyState>,
    Json(body): Json<EventBody>,
) -> Result<Json<Value>, ApiError> {
    let bot = lookup(&state, body.bot_id).await?;
    let Some(channel_id) = body.channel_id else {
        return Ok(Json(json!({"ok": true})));
    };
    let mut pg = state.pg.lock().await;
    match body.event_type.as_str() {
        "join" | "joined" => pg::upsert_joined_channel(
            &mut pg,
            bot.tenant_id,
            bot.id,
            channel_id,
            body.channel_type,
            body.title,
        )
        .await
        .map_err(|e| {
            state.db_fail("upsert lineworks channel", &e);
            internal_error_msg("upsert_failed")
        })?,
        "leave" | "left" => pg::mark_left_channel(&mut pg, bot.tenant_id, bot.id, channel_id)
            .await
            .map_err(|e| {
                state.db_fail("mark lineworks channel left", &e);
                internal_error_msg("mark_left_failed")
            })?,
        _ => {}
    }
    Ok(Json(json!({"ok": true})))
}
