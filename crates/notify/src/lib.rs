//! notify の配信・宛先の口 (Refs ippoan/rust-alc-api#747)。rust-alc-api の `crates/alc-notify` から、Cloud Run に残さない口を移す。
//!
//! - [`recipients`] — 宛先の CRUD と一括登録 (`/api/notify/recipients`)
//! - [`groups`] — 宛先のグループの CRUD とメンバー (`/api/notify/groups`)
//! - [`line_config`] — LINE の設定 (`/api/notify/line-config`。秘密値は暗号化して保存)
//! - [`channels`] — LINE WORKS の Bot が招かれたトークルームの一覧・削除・試し送信 (`/api/notify/lineworks/channels`)
//! - [`lineworks`] — LINE WORKS の組織のメンバーとログイン状況 (`/api/notify/lineworks/users`・`/login-activity`)。
//!   LINE WORKS の API は ippoan/alc-lineworks-worker を Service Binding で呼ぶ ([`lineworks::LineworksApi`])
//! - [`distribute`] — 文書の配信と試し配信 (`/api/notify/documents/{id}/distribute`・`/api/notify/test-distribute`)。
//!   LINE は [`line`] (外への POST は [`http::Http`])、LINE WORKS は lineworks-worker、viewer の KV への登録は [`viewer_register`]
//! - [`pg`] — SQL (`PgClient::tenant_tx` + `query_typed` 系だけ)
//!
//! path・method・JSON の形・status は rust と同じ (nuxt-notify と auth-worker がそのまま使う)。path の頭の `/api` は、
//! auth-worker の Service Binding が渡す path (`/api/...` のまま) に合わせる。
//! tenant は auth-worker が付ける `X-Tenant-ID` (無い・UUID でない → 401。`alc_core_wasm::require_tenant_header`)。
//! role の検査は rust にも無いので持たない。
//!
//! 失敗の応答は rust と同じ (宛先・グループ・line-config は本文なしの status、LINE WORKS の口は `{"error": ...}`)。ログは固定の語と DB のエラーの kind だけ (識別子・入力の値を出さない)。

pub mod channels;
pub mod distribute;
pub mod groups;
pub mod http;
pub mod line;
pub mod line_config;
pub mod lineworks;
pub mod pg;
pub mod recipients;
pub mod viewer_register;

use std::sync::Arc;

use alc_core_wasm::require_tenant_header;
use alc_worker_db::{kind, PgClient};
use axum::http::StatusCode;
use axum::{middleware, Router};
use distribute::Outbound;
use futures_util::lock::Mutex;
use lineworks::LineworksApi;

pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct NotifyState {
    pub pg: Arc<Mutex<PgClient>>,
    /// ippoan/alc-lineworks-worker (Service Binding `ALC_LINEWORKS`)
    pub lineworks: Arc<dyn LineworksApi>,
    /// `SSO_ENCRYPTION_KEY` (Secrets Store)。worker は line-config の POST と配信の口でだけ引く (ほかは `None`)
    pub sso_key: Option<Arc<str>>,
    /// 配信の外への送信 (LINE・viewer の KV への登録) に要るもの
    pub outbound: Outbound,
    pub log: LogSink,
}

impl NotifyState {
    /// DB の失敗を 500 (本文なし) にし、固定の語と kind だけをログに出す
    pub(crate) fn db_fail(&self, what: &str, e: &tokio_postgres::Error) -> StatusCode {
        (self.log)(&format!("notify: {what} failed: db ({})", kind(e)));
        StatusCode::INTERNAL_SERVER_ERROR
    }

    /// 失敗しても止めない DB の書き込み (rust の `let _ = ...`)。失敗は [`Self::db_fail`] と同じログだけ
    pub(crate) fn best_effort(&self, what: &str, r: Result<(), tokio_postgres::Error>) {
        if let Err(e) = r {
            self.db_fail(what, &e);
        }
    }
}

/// どの口にも tenant の layer を掛ける
pub fn router() -> Router<NotifyState> {
    Router::new()
        .merge(recipients::router())
        .merge(groups::router())
        .merge(line_config::router())
        .merge(channels::router())
        .merge(lineworks::router())
        .merge(distribute::router())
        .route_layer(middleware::from_fn(require_tenant_header))
}
