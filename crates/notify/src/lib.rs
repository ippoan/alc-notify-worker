//! notify の配信・宛先の口 (Refs ippoan/rust-alc-api#747)。rust-alc-api の `crates/alc-notify` から、Cloud Run に残さない口を移す。
//!
//! - [`recipients`] — 宛先の CRUD と一括登録 (`/api/notify/recipients`)
//! - [`groups`] — 宛先のグループの CRUD とメンバー (`/api/notify/groups`)
//! - [`pg`] — SQL (`PgClient::tenant_tx` + `query_typed` 系だけ)
//!
//! path・method・JSON の形・status は rust と同じ (nuxt-notify と auth-worker がそのまま使う)。path の頭の `/api` は、
//! auth-worker の Service Binding が渡す path (`/api/...` のまま) に合わせる。
//! tenant は auth-worker が付ける `X-Tenant-ID` (無い・UUID でない → 401。`alc_core_wasm::require_tenant_header`)。
//! role の検査は rust にも無いので持たない。
//!
//! 失敗の応答は rust と同じく本文なしの status だけ。ログは固定の語と DB のエラーの kind だけ (識別子・入力の値を出さない)。

pub mod groups;
pub mod pg;
pub mod recipients;

use std::sync::Arc;

use alc_core_wasm::require_tenant_header;
use alc_worker_db::{kind, PgClient};
use axum::http::StatusCode;
use axum::{middleware, Router};
use futures_util::lock::Mutex;

pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct NotifyState {
    pub pg: Arc<Mutex<PgClient>>,
    pub log: LogSink,
}

impl NotifyState {
    /// DB の失敗を 500 (本文なし) にし、固定の語と kind だけをログに出す
    pub(crate) fn db_fail(&self, what: &str, e: &tokio_postgres::Error) -> StatusCode {
        (self.log)(&format!("notify: {what} failed: db ({})", kind(e)));
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// どの口にも tenant の layer を掛ける
pub fn router() -> Router<NotifyState> {
    Router::new()
        .merge(recipients::router())
        .merge(groups::router())
        .route_layer(middleware::from_fn(require_tenant_header))
}
