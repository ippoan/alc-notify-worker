//! LINE (Messaging API) の設定 (notify_line_configs) の口。rust-alc-api の `crates/alc-notify/src/line_config.rs` と
//! 同じ path・method・JSON・status。
//!
//! | method | path | 成功 |
//! |---|---|---|
//! | GET | `/api/notify/line-config` | 200 設定 (暗号文の項目を持たない) / 無ければ `null` |
//! | POST | `/api/notify/line-config` | 200 保存した設定 (tenant に 1 行。在れば差し替える) |
//! | DELETE | `/api/notify/line-config` | 204 (無くても 204) |
//!
//! POST は `channel_secret` と `private_key` を `SSO_ENCRYPTION_KEY` (Secrets Store。worker はこの口でだけ引く) で
//! 暗号化してから保存する (`alc_worker_crypto::secret::encrypt_secret`。rust の `alc_core::auth_lineworks::encrypt_secret`
//! と同じ形式)。失敗は rust と同じく本文なしの 500。

use alc_core_wasm::TenantId;
use alc_worker_crypto::secret::encrypt_secret;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::pg::{self, UpsertLineConfigRow};
use crate::NotifyState;

pub const LINE_CONFIG_PATH: &str = "/api/notify/line-config";

pub fn router() -> Router<NotifyState> {
    Router::new().route(
        LINE_CONFIG_PATH,
        get(get_config).post(upsert).delete(delete),
    )
}

/// rust の `alc_core::repository::notify_line_config::UpsertLineConfig` と同じ入力
#[derive(Debug, Deserialize)]
pub struct UpsertLineConfig {
    pub name: String,
    pub channel_id: String,
    pub channel_secret: String,
    pub key_id: String,
    pub private_key: String,
    pub bot_basic_id: Option<String>,
    pub public_key_jwk: Option<String>,
}

async fn get_config(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let config = pg::get_line_config(&mut pg, tenant.0)
        .await
        .map_err(|e| state.db_fail("get line config", &e))?;
    Ok(Json(json!(config)))
}

async fn upsert(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Json(input): Json<UpsertLineConfig>,
) -> Result<Json<Value>, StatusCode> {
    // 鍵が無い (Secrets Store から取れない) か、暗号化の失敗 (乱数が取れないときだけ) → 500。文に秘密値を入れない
    let encrypted = state.sso_key.as_deref().and_then(|key| {
        let channel_secret = encrypt_secret(&input.channel_secret, key).ok()?;
        Some((
            channel_secret,
            encrypt_secret(&input.private_key, key).ok()?,
        ))
    });
    let Some((channel_secret_encrypted, private_key_encrypted)) = encrypted else {
        (state.log)("notify: upsert line config failed: encrypt");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    let row = UpsertLineConfigRow {
        name: input.name,
        channel_id: input.channel_id,
        channel_secret_encrypted,
        key_id: input.key_id,
        private_key_encrypted,
        bot_basic_id: input.bot_basic_id,
        public_key_jwk: input.public_key_jwk,
    };
    let mut pg = state.pg.lock().await;
    let config = pg::upsert_line_config(&mut pg, tenant.0, row)
        .await
        .map_err(|e| state.db_fail("upsert line config", &e))?;
    Ok(Json(json!(config)))
}

async fn delete(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    pg::delete_line_config(&mut pg, tenant.0)
        .await
        .map_err(|e| state.db_fail("delete line config", &e))?;
    Ok(StatusCode::NO_CONTENT)
}
