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
//!
//! ## tenant 付きの 2 本 (rust に無い口)
//!
//! `GET /api/internal/lineworks/tenant/bot-secret/{bot_id}`・`POST /api/internal/lineworks/tenant/event`。応答・失敗の語は上の 2 本と同じ。
//! auth-worker が callback URL の識別子 (tenant と bot_id を暗号化したもの) を復号し、tenant を `X-Tenant-ID` で付けて呼ぶ。
//! tenant の layer の中に置き ([`tenant_router`]。無い・UUID でない → 401)、bot はその tenant の transaction の中で RLS のまま引く
//! ([`pg::lookup_tenant_bot_for_webhook`])。tenant をまたぐ SECURITY DEFINER の関数を通さないための経路で、上の 2 本は
//! callback URL が切り替わるまで残す。
//!
//! ## トークルームの名前 (rust に無い)
//!
//! LINE WORKS の招待のイベント (`join`/`joined`) はトークルームの名前を持たない。名前 (`title`) が無ければ、足す前に
//! lineworks-worker の `GET /api/internal/lineworks/channel-info` (そのトークルームに居る bot) で引いて入れる。
//! 取れなければ名前なしのまま足す (best-effort。ログは固定の語と status / 届かない理由の label だけ)。

use alc_core_wasm::api_error::{internal_error_msg, not_found, ApiError};
use alc_core_wasm::TenantId;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::line::form_urlencode;
use crate::lineworks::{LwRequest, LwUnreachable};
use crate::pg::{self, BotForWebhook};
use crate::NotifyState;

/// lineworks-worker のトークルームの名前の口 (path は lineworks-worker の `crates/lineworks/src/routes.rs` と同じ)
pub const LW_CHANNEL_INFO_PATH: &str = "/api/internal/lineworks/channel-info";

pub const BOT_SECRET_PATH: &str = "/api/internal/lineworks/bot-secret/{bot_id}";
pub const EVENT_PATH: &str = "/api/internal/lineworks/event";
pub const TENANT_BOT_SECRET_PATH: &str = "/api/internal/lineworks/tenant/bot-secret/{bot_id}";
pub const TENANT_EVENT_PATH: &str = "/api/internal/lineworks/tenant/event";

/// tenant のヘッダーを持たない 2 本 (tenant の layer の外)
pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(BOT_SECRET_PATH, get(bot_secret))
        .route(EVENT_PATH, post(event))
}

/// tenant 付きの 2 本 (tenant の layer の中に置く)
pub fn tenant_router() -> Router<NotifyState> {
    Router::new()
        .route(TENANT_BOT_SECRET_PATH, get(tenant_bot_secret))
        .route(TENANT_EVENT_PATH, post(tenant_event))
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

/// bot_id → 有効な bot。tenant が分かっていればその tenant の中で、無ければ tenant をまたぐ関数で引く
async fn lookup(
    state: &NotifyState,
    tenant: Option<Uuid>,
    bot_id: String,
) -> Result<BotForWebhook, ApiError> {
    let mut pg = state.pg.lock().await;
    let found = match tenant {
        Some(t) => pg::lookup_tenant_bot_for_webhook(&mut pg, t, bot_id).await,
        None => pg::lookup_bot_for_webhook(&mut pg, bot_id).await,
    };
    found
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
    secret_of(&state, None, bot_id).await
}

async fn tenant_bot_secret(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant)): Extension<TenantId>,
    Path(bot_id): Path<String>,
) -> Result<Json<BotSecretResponse>, ApiError> {
    secret_of(&state, Some(tenant), bot_id).await
}

async fn secret_of(
    state: &NotifyState,
    tenant: Option<Uuid>,
    bot_id: String,
) -> Result<Json<BotSecretResponse>, ApiError> {
    let bot = lookup(state, tenant, bot_id).await?;
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
    apply_event(state, None, body).await
}

async fn tenant_event(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant)): Extension<TenantId>,
    Json(body): Json<EventBody>,
) -> Result<Json<Value>, ApiError> {
    apply_event(state, Some(tenant), body).await
}

async fn apply_event(
    state: NotifyState,
    tenant: Option<Uuid>,
    body: EventBody,
) -> Result<Json<Value>, ApiError> {
    let bot = lookup(&state, tenant, body.bot_id).await?;
    let Some(channel_id) = body.channel_id else {
        return Ok(Json(json!({"ok": true})));
    };
    let joined = matches!(body.event_type.as_str(), "join" | "joined");
    let title = match body.title {
        None if joined => channel_title(&state, &bot, &channel_id).await,
        t => t,
    };
    let mut pg = state.pg.lock().await;
    match body.event_type.as_str() {
        "join" | "joined" => pg::upsert_joined_channel(
            &mut pg,
            bot.tenant_id,
            bot.id,
            channel_id,
            body.channel_type,
            title,
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

/// トークルームの名前を lineworks-worker で引く (そのトークルームに居る bot・その tenant)。取れなければ `None`
async fn channel_title(
    state: &NotifyState,
    bot: &BotForWebhook,
    channel_id: &str,
) -> Option<String> {
    let bot_config_id = bot.id.to_string();
    let query = form_urlencode(&[
        ("channel_id", channel_id),
        ("bot_config_id", &bot_config_id),
    ]);
    let req = LwRequest {
        post: false,
        path: format!("{LW_CHANNEL_INFO_PATH}?{query}"),
        tenant_id: bot.tenant_id,
        body: None,
    };
    match state.lineworks.call(req).await {
        Ok(r) if (200..300).contains(&r.status) => {
            let v: Value = serde_json::from_slice(&r.body).ok()?;
            v.get("title")?.as_str().map(str::to_owned)
        }
        Ok(r) => {
            (state.log)(&format!(
                "notify: lineworks channel title failed: status {}",
                r.status
            ));
            None
        }
        Err(LwUnreachable(label)) => {
            (state.log)(&format!("notify: lineworks channel title failed: {label}"));
            None
        }
    }
}
