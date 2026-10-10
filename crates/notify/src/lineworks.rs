//! LINE WORKS の口 (Refs ippoan/rust-alc-api#747)。LINE WORKS の API はこの worker では叩かず、すべて
//! ippoan/alc-lineworks-worker を Service Binding `ALC_LINEWORKS` で呼ぶ ([`LineworksApi`]。worker の実装は直下の
//! `src/lineworks.rs`、テストはモックに差し替える)。tenant は `X-Tenant-ID` で渡し、bot は lineworks-worker が決める。
//!
//! | method | path | 成功 | 中身 |
//! |---|---|---|---|
//! | GET | `/api/notify/lineworks/users` | 200 `[{user_id, user_name, email, already_registered}]` | lineworks-worker の `GET /api/internal/lineworks/users` (tenant の既定の bot) に、自 tenant の notify_recipients の `lineworks_user_id` に在るかを足す (rust の `lineworks_directory::list_users`) |
//! | GET | `/api/notify/lineworks/login-activity?days=` | 200 1 人 1 行の配列 | lineworks-worker の `GET /api/internal/lineworks/login-activity` の中継 (計算とキャッシュは lineworks-worker) |
//!
//! channel の 3 本 (一覧・削除・test-send) は [`crate::channels`]。
//!
//! lineworks-worker が断ったときは、その status と語 (`error`、`missing_scope` の `scope`、語の形の `message`) だけを写す
//! ([`relay_error`])。上流の本文の自由文は載せない (rust の 502 は文を載せていた)。lineworks-worker に届かない・本文が
//! 読めないときは 502 `upstream_error`。ログは段の名前と、届かない理由の label か status だけ。

use std::collections::HashSet;

use alc_core_wasm::api_error::{internal_error_msg, ApiError};
use alc_core_wasm::TenantId;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{pg, NotifyState};

pub const USERS_PATH: &str = "/api/notify/lineworks/users";
pub const LOGIN_ACTIVITY_PATH: &str = "/api/notify/lineworks/login-activity";

/// lineworks-worker の口 (path は lineworks-worker の `crates/lineworks/src/routes.rs` と同じ)
pub const LW_MESSAGES_PATH: &str = "/api/internal/lineworks/messages";
pub const LW_USERS_PATH: &str = "/api/internal/lineworks/users";
pub const LW_LOGIN_ACTIVITY_PATH: &str = "/api/internal/lineworks/login-activity";

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(USERS_PATH, get(list_users))
        .route(LOGIN_ACTIVITY_PATH, get(login_activity))
}

/// lineworks-worker への 1 リクエスト。`X-Tenant-ID` は `tenant_id` から実装が付ける
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LwRequest {
    /// `true` = POST (`body` を JSON で送る)、`false` = GET
    pub post: bool,
    /// path と query (`/api/internal/lineworks/...`)
    pub path: String,
    pub tenant_id: Uuid,
    pub body: Option<String>,
}

/// lineworks-worker の応答 (status と本文のバイト列)
#[derive(Debug, Clone)]
pub struct LwResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// lineworks-worker に届かなかった (binding が無い・fetch の失敗・本文が読めない)。中身はログに出す段の label
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LwUnreachable(pub &'static str);

/// lineworks-worker の Service Binding (`ALC_LINEWORKS`)。テストはモックに差し替える
pub trait LineworksApi: Send + Sync {
    fn call(&self, req: LwRequest) -> BoxFuture<'_, Result<LwResponse, LwUnreachable>>;
}

/// 上流の 502 (文を載せない)
fn upstream_error() -> ApiError {
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({"error": "upstream_error"})),
    )
}

/// 応答に写してよい語 (小文字・数字・`_`・`.` だけの短い文字列)。自由文は通さない
fn word(v: &Value, key: &str) -> Option<String> {
    let s = v.get(key)?.as_str()?;
    let ok = !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'.');
    ok.then(|| s.to_owned())
}

/// lineworks-worker が断った応答を写す: status (4xx / 5xx でなければ 502) と `error`
/// (語でなければ `upstream_error`)。`missing_scope` は `scope` と rust と同じ固定の `message` を付け、
/// ほかは `message` が語のときだけ写す (`internal_error` の `private_key_invalid` など)
pub fn relay_error(status: u16, body: &[u8]) -> ApiError {
    let status = StatusCode::from_u16(status)
        .ok()
        .filter(|s| s.is_client_error() || s.is_server_error())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let error = word(&v, "error").unwrap_or_else(|| "upstream_error".to_owned());
    let mut out = json!({"error": error});
    if error == "missing_scope" {
        if let Some(scope) = word(&v, "scope") {
            out["message"] = json!(format!(
                "LINE WORKS Developer Console で {scope} scope を追加してください"
            ));
            out["scope"] = json!(scope);
        }
    } else if let Some(message) = word(&v, "message") {
        out["message"] = json!(message);
    }
    (status, Json(out))
}

impl NotifyState {
    /// lineworks-worker を呼び、2xx の応答を返す。届かない → 502、断られた → [`relay_error`]
    pub(crate) async fn lineworks_ok(
        &self,
        what: &str,
        req: LwRequest,
    ) -> Result<LwResponse, ApiError> {
        let resp = self.lineworks.call(req).await.map_err(|LwUnreachable(l)| {
            (self.log)(&format!("notify: {what} failed: lineworks ({l})"));
            upstream_error()
        })?;
        if !(200..300).contains(&resp.status) {
            (self.log)(&format!(
                "notify: {what} failed: lineworks (status {})",
                resp.status
            ));
            return Err(relay_error(resp.status, &resp.body));
        }
        Ok(resp)
    }

    /// [`Self::lineworks_ok`] の本文を JSON で読む。読めない → 502
    async fn lineworks_json<T: serde::de::DeserializeOwned>(
        &self,
        what: &str,
        req: LwRequest,
    ) -> Result<T, ApiError> {
        let resp = self.lineworks_ok(what, req).await?;
        serde_json::from_slice(&resp.body).map_err(|_| {
            (self.log)(&format!("notify: {what} failed: lineworks (body)"));
            upstream_error()
        })
    }
}

/// lineworks-worker の users の 1 行 (rust の `LineworksMember`)
#[derive(Debug, Deserialize)]
struct LineworksMember {
    user_id: String,
    user_name: Option<String>,
    email: Option<String>,
}

/// rust の `lineworks_directory::DirectoryUser` と同じ JSON
#[derive(Debug, Serialize)]
pub struct DirectoryUser {
    pub user_id: String,
    pub user_name: Option<String>,
    pub email: Option<String>,
    pub already_registered: bool,
}

/// 組織のメンバー (tenant の既定の bot) に、自 tenant の宛先に在るかを足す。
/// lineworks-worker を先に呼び、DB はその後 (Fetcher の待ちを transaction の中に挟まない)
async fn list_users(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Vec<DirectoryUser>>, ApiError> {
    let req = LwRequest {
        post: false,
        path: LW_USERS_PATH.to_owned(),
        tenant_id: tenant.0,
        body: None,
    };
    let members: Vec<LineworksMember> = state.lineworks_json("list lineworks users", req).await?;
    let existing = {
        let mut pg = state.pg.lock().await;
        pg::list_recipients(&mut pg, tenant.0).await.map_err(|e| {
            state.db_fail("list recipients", &e);
            internal_error_msg("list_recipients_failed")
        })?
    };
    let registered: HashSet<String> = existing
        .into_iter()
        .filter_map(|r| r.lineworks_user_id)
        .collect();
    let users = members
        .into_iter()
        .map(|m| DirectoryUser {
            already_registered: registered.contains(&m.user_id),
            user_id: m.user_id,
            user_name: m.user_name,
            email: m.email,
        })
        .collect();
    Ok(Json(users))
}

#[derive(Debug, Deserialize)]
pub struct LoginActivityQuery {
    /// 既定 3 (rust と同じ)。負の数は lineworks-worker が 0 にする
    #[serde(default = "default_days")]
    pub days: i64,
}

fn default_days() -> i64 {
    3
}

/// lineworks-worker のログイン状況をそのまま返す (tenant の既定の bot。JSON は読めることだけ確かめる)
async fn login_activity(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Query(q): Query<LoginActivityQuery>,
) -> Result<Json<Value>, ApiError> {
    let req = LwRequest {
        post: false,
        path: format!("{LW_LOGIN_ACTIVITY_PATH}?days={}", q.days),
        tenant_id: tenant.0,
        body: None,
    };
    let entries: Value = state
        .lineworks_json("lineworks login activity", req)
        .await?;
    Ok(Json(entries))
}
