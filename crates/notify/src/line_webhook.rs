//! LINE (Messaging API) の webhook の受け口 `POST /api/internal/notify/line/webhook` (W8b、Refs ippoan/rust-alc-api#680)。
//! rust-alc-api の `crates/alc-notify/src/line_webhook.rs` の移植。
//!
//! LINE の Console に登録した URL は auth-worker の `POST /line/webhook` で、auth-worker が raw body と `x-line-signature` を
//! そのままここへ渡す (auth-worker は channel secret を持たない)。この口は tenant のヘッダーを持たない
//! ([`crate::router`] の tenant の layer の外)。
//!
//! 1. `x-line-signature` が無い → 400、本文が LINE の webhook の JSON でない → 400 (rust と同じ順)
//! 2. 全テナントの有効な LINE の設定 ([`pg::list_enabled_line_configs`]。テナントが決まる前なので nil のテナントの transaction で
//!    SECURITY DEFINER の関数を呼ぶ) の channel secret を `SSO_ENCRYPTION_KEY` で復号し、`base64(HMAC-SHA256(body))` を
//!    定数時間で照合する。復号できない設定は飛ばす。どれにも合わなければ 401
//! 3. 合った設定の Private Key を復号する (key_id・private_key が無い・復号できない → 500)
//! 4. `follow` の event ごとに、LINE の profile API で表示名を引き (失敗したら `LINE User <user id の先頭 8 文字>`)、
//!    合った tenant の宛先を作る・名前を更新する ([`pg::upsert_line_recipient`])。書けなくても 200 (rust と同じ。ログだけ)
//!
//! 応答は本文なしの status (rust と同じ)。ログは固定の語と DB の kind だけ (user id・tenant を出さない。rust は出していた)。

use alc_worker_crypto::secret::{decrypt_pem_secret, decrypt_secret};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use uuid::Uuid;

use crate::line::{LineClient, LineConfig};
use crate::pg;
use crate::NotifyState;

pub const LINE_WEBHOOK_PATH: &str = "/api/internal/notify/line/webhook";

pub fn router() -> Router<NotifyState> {
    Router::new().route(LINE_WEBHOOK_PATH, post(handle))
}

#[derive(Deserialize)]
struct WebhookBody {
    events: Vec<WebhookEvent>,
}

#[derive(Deserialize)]
struct WebhookEvent {
    #[serde(rename = "type")]
    event_type: String,
    source: Option<EventSource>,
}

#[derive(Deserialize)]
struct EventSource {
    #[serde(rename = "userId")]
    user_id: Option<String>,
}

/// 署名が合った設定 (復号済み)
struct Resolved {
    tenant_id: Uuid,
    config: LineConfig,
}

/// LINE の `x-line-signature` (`base64(HMAC-SHA256(channel secret, body))`) を定数時間で照合する
pub fn verify_signature(body: &[u8], channel_secret: &str, signature: &str) -> bool {
    let Ok(decoded) = STANDARD.decode(signature) else {
        return false;
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(channel_secret.as_bytes())
        .expect("HMAC は任意の長さの鍵を受ける");
    mac.update(body);
    mac.verify_slice(&decoded).is_ok()
}

async fn handle(State(state): State<NotifyState>, headers: HeaderMap, body: Bytes) -> StatusCode {
    match process(&state, &headers, &body).await {
        Ok(()) => StatusCode::OK,
        Err(status) => status,
    }
}

async fn process(state: &NotifyState, headers: &HeaderMap, body: &[u8]) -> Result<(), StatusCode> {
    let signature = headers
        .get("x-line-signature")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let webhook: WebhookBody = serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let resolved = resolve(state, body, signature).await?;

    let out = &state.outbound;
    let line = LineClient {
        http: out.http.as_ref(),
        endpoints: &out.line_endpoints,
        cache: &out.line_tokens,
        now: (out.now)(),
    };
    for event in &webhook.events {
        if event.event_type != "follow" {
            continue;
        }
        let Some(user_id) = event.source.as_ref().and_then(|s| s.user_id.clone()) else {
            continue;
        };
        let name = match line.display_name(&resolved.config, &user_id).await {
            Some(name) => name,
            None => {
                (state.log)("notify: line webhook profile failed");
                format!("LINE User {}", user_id.chars().take(8).collect::<String>())
            }
        };
        let mut pg = state.pg.lock().await;
        let r = pg::upsert_line_recipient(&mut pg, resolved.tenant_id, user_id, name).await;
        state.best_effort("upsert line recipient", r);
    }
    Ok(())
}

/// 署名で設定を特定する
async fn resolve(
    state: &NotifyState,
    body: &[u8],
    signature: &str,
) -> Result<Resolved, StatusCode> {
    let configs = {
        let mut pg = state.pg.lock().await;
        pg::list_enabled_line_configs(&mut pg)
            .await
            .map_err(|e| state.db_fail("list line configs", &e))?
    };
    let Some(key) = state.sso_key.as_deref() else {
        (state.log)("notify: line webhook: no SSO_ENCRYPTION_KEY");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    for cfg in configs {
        let Ok(channel_secret) = decrypt_secret(&cfg.channel_secret_encrypted, key) else {
            continue;
        };
        if !verify_signature(body, &channel_secret, signature) {
            continue;
        }
        let private_key = match cfg.private_key_encrypted.as_deref() {
            Some(enc) => Some(decrypt_pem_secret(enc, key).map_err(|_| {
                (state.log)("notify: line webhook: decrypt private_key failed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?),
            None => None,
        };
        let (Some(key_id), Some(private_key)) = (cfg.key_id, private_key) else {
            (state.log)("notify: line webhook: line config has no key_id / private_key");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        };
        return Ok(Resolved {
            tenant_id: cfg.tenant_id,
            config: LineConfig {
                channel_id: cfg.channel_id,
                key_id,
                private_key,
            },
        });
    }
    (state.log)("notify: line webhook: no matching line config");
    Err(StatusCode::UNAUTHORIZED)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        STANDARD.encode(mac.finalize().into_bytes())
    }

    #[test]
    fn signature_is_checked_against_the_body_and_secret() {
        let body = br#"{"events":[]}"#;
        let sig = sign("s3cret", body);
        assert!(verify_signature(body, "s3cret", &sig));
        assert!(!verify_signature(body, "other", &sig));
        assert!(!verify_signature(b"{}", "s3cret", &sig));
        assert!(!verify_signature(body, "s3cret", "not base64!"));
        assert!(!verify_signature(
            body,
            "s3cret",
            &STANDARD.encode(b"short")
        ));
    }
}
