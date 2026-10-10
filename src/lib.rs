//! notify の配信・宛先の Worker `alc-notify` (Refs ippoan/rust-alc-api#747)。crates/notify の口 (`router`、axum) を
//! workers-rs (`http` / `axum` feature) に載せる。型は ippoan/alc-lineworks-worker と同じ。
//! DB への経路 (staging = Workers VPC の先の PgBouncer、本番 = Hyperdrive) は [`db::connect`] の 1 か所で出し分ける。
//!
//! **この Worker は呼び手を認証しない。** 本番・staging とも workers.dev を開けず (`workers_dev` / `preview_urls` = false、
//! `routes` を持たない)、入口は auth-worker からの Service Binding だけ (wrangler.toml と scripts/check-exposure.sh が保証する)。
//!
//! LINE WORKS の API は ippoan/alc-lineworks-worker を Service Binding `ALC_LINEWORKS` で呼ぶ ([`lineworks`])。
//! 秘密値 `SSO_ENCRYPTION_KEY` (Secrets Store) は line-config の POST と配信の口でだけ、`INTERNAL_SHARED_SECRET`
//! (Secrets Store、viewer の register-view の認証) は配信の口でだけ引く。配信の外への POST (LINE・viewer) は [`http`]。

// worker 本体は wasm32 専用。native の `cargo build` / `clippy` / `test` (workspace の root で打つもの) では空の crate にして、
// crates/notify だけを検査させる
#![cfg(target_arch = "wasm32")]

mod db;
mod http;
mod lineworks;
mod tcp;

use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::http::{HeaderValue, Method, Response, StatusCode};
use futures_util::lock::Mutex;
use notify::distribute::{is_distribute_request, Outbound};
use notify::line::{LineEndpoints, LineTokenCache};
use notify::line_config::LINE_CONFIG_PATH;
use notify::lineworks::LineworksApi;
use notify::{router, LogSink, NotifyState};
use tower_service::Service;
use worker::{
    console_error, event, Context, Date, Env, HttpRequest, Result, WorkerVersionMetadata,
};

use crate::http::WorkerHttp;
use crate::lineworks::{LineworksBinding, NoLineworks};

/// line-config の秘密値の暗号化の鍵 (Secrets Store。auth-worker・alc-lineworks-worker と同じ secret)
const SSO_ENCRYPTION_KEY_BINDING: &str = "SSO_ENCRYPTION_KEY";
/// ippoan/alc-lineworks-worker の Service Binding
const ALC_LINEWORKS_BINDING: &str = "ALC_LINEWORKS";
/// viewer の register-view の認証 (Secrets Store。nuxt-notify・auth-worker などと同じ secret)
const INTERNAL_SHARED_SECRET_BINDING: &str = "INTERNAL_SHARED_SECRET";
/// viewer の Worker (ippoan/nuxt-notify) の公開 URL (wrangler.toml の vars)
const NOTIFY_FRONTEND_URL_VAR: &str = "NOTIFY_FRONTEND_URL";

/// LINE の channel access token の置き場 (isolate の間ずっと持つ。キーは channel ごと)
fn line_tokens() -> Arc<LineTokenCache> {
    static CACHE: OnceLock<Arc<LineTokenCache>> = OnceLock::new();
    CACHE.get_or_init(Default::default).clone()
}

fn now_secs() -> u64 {
    Date::now().as_millis() / 1000
}

/// Secrets Store の値。取れなければ `None` (口が本文なしの 500 にする)
async fn secret(env: &Env, binding: &str) -> Option<Arc<str>> {
    let store = env.secret_store(binding).ok()?;
    match store.get().await {
        Ok(Some(v)) => Some(Arc::from(v)),
        _ => {
            console_error!("notify: no {binding} value");
            None
        }
    }
}

fn lineworks_api(env: &Env) -> Arc<dyn LineworksApi> {
    match env.service(ALC_LINEWORKS_BINDING) {
        Ok(fetcher) => Arc::new(LineworksBinding::new(fetcher)),
        Err(_) => Arc::new(NoLineworks),
    }
}

fn log_sink() -> LogSink {
    Arc::new(|message| console_error!("{message}"))
}

fn error_response(status: StatusCode, code: &str) -> Response<Body> {
    let mut resp = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *resp.status_mut() = status;
    resp
}

#[event(fetch)]
async fn fetch(req: HttpRequest, env: Env, _ctx: Context) -> Result<Response<Body>> {
    // 全リクエストで routing の前に繋ぐ (ippoan/alc-vein-worker と同じ)
    let pg = match db::connect(&env).await {
        Ok(c) => c,
        Err(db::ConnectError::NotConfigured) => {
            console_error!("notify: {}", db::ConnectError::NotConfigured);
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_not_configured",
            ));
        }
        Err(e) => {
            console_error!("notify: {e}");
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ));
        }
    };
    // 秘密値を引くのは line-config の POST と配信の口だけ (ほかの口では Secrets Store を読まない)
    let post = req.method() == Method::POST;
    let distributing = is_distribute_request(post, req.uri().path());
    let sso_key = if distributing || (post && req.uri().path() == LINE_CONFIG_PATH) {
        secret(&env, SSO_ENCRYPTION_KEY_BINDING).await
    } else {
        None
    };
    let viewer_secret = if distributing {
        secret(&env, INTERNAL_SHARED_SECRET_BINDING).await
    } else {
        None
    };
    let outbound = Outbound {
        http: Arc::new(WorkerHttp),
        line_endpoints: LineEndpoints::default(),
        line_tokens: line_tokens(),
        frontend_url: env
            .var(NOTIFY_FRONTEND_URL_VAR)
            .ok()
            .map(|v| Arc::from(v.to_string())),
        viewer_secret,
        now: Arc::new(now_secs),
    };
    let state = NotifyState {
        pg: Arc::new(Mutex::new(pg)),
        lineworks: lineworks_api(&env),
        sso_key,
        outbound,
        log: log_sink(),
    };
    let mut resp = match router().with_state(state).call(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    // どの版が応えたかを応答ヘッダーで分かるようにする。binding が取れなければ何も付けない
    if let Ok(meta) = env.get_binding::<WorkerVersionMetadata>("CF_VERSION_METADATA") {
        if let Ok(v) = HeaderValue::from_str(&meta.id()) {
            resp.headers_mut().insert("x-worker-version", v);
        }
        let tag = meta.tag();
        if !tag.is_empty() {
            if let Ok(v) = HeaderValue::from_str(&tag) {
                resp.headers_mut().insert("x-worker-tag", v);
            }
        }
    }
    Ok(resp)
}
