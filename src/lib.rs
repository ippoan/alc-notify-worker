//! notify の配信・宛先の Worker `alc-notify` (Refs ippoan/rust-alc-api#747)。crates/notify の口 (`router`、axum) を
//! workers-rs (`http` / `axum` feature) に載せる。型は ippoan/alc-lineworks-worker と同じ。
//! DB への経路 (staging = Workers VPC の先の PgBouncer、本番 = Hyperdrive) は [`db::connect`] の 1 か所で出し分ける。
//!
//! **この Worker は呼び手を認証しない。** 本番・staging とも workers.dev を開けず (`workers_dev` / `preview_urls` = false、
//! `routes` を持たない)、入口は auth-worker からの Service Binding だけ (wrangler.toml と scripts/check-exposure.sh が保証する)。
//!
//! LINE WORKS の API は ippoan/alc-lineworks-worker を Service Binding `ALC_LINEWORKS` で呼ぶ ([`lineworks`])。
//! 秘密値 `SSO_ENCRYPTION_KEY` (Secrets Store) は line-config の POST・LINE の webhook・配信の口でだけ、`INTERNAL_SHARED_SECRET`
//! (Secrets Store、viewer の register-view の認証) は配信の口でだけ引く。`CF_EMAIL_ROUTING_TOKEN` (Secrets Store) と
//! `CF_ACCOUNT_ID` (`[vars]`) (Cloudflare Email Routing の送り先の登録) は宛先の保存と email-status の口でだけ引く ([`email_routing`])。配信の外への POST (LINE・viewer) は [`http`]。
//! 文書のファイル (閲覧の preview・download と黒塗りの JPEG) は R2 の binding `NOTIFY_R2` ([`r2`])。
//!
//! 抽出・黒塗り (W8c-2) は Cloudflare Queues の binding `NOTIFY_DOCS_QUEUE` へ積み ([`queue`])、同じ worker の
//! `#[event(queue)]` が 1 メッセージずつ [`notify::background::Background::run`] に渡す。alc-redact は auth-worker の RPC
//! `AlcRedactEntrypoint` ([`redact`])、終わりの WS の push は notify-realtime-bus の Service Binding ([`bus`]。
//! `X-Broadcast-Secret` は Secrets Store の `NOTIFY_REDACT_BROADCAST_SECRET`。consumer でだけ引く)。

// worker 本体は wasm32 専用。native の `cargo build` / `clippy` / `test` (workspace の root で打つもの) では空の crate にして、
// crates/notify だけを検査させる
#![cfg(target_arch = "wasm32")]

mod bus;
mod db;
mod email_routing;
mod http;
mod lineworks;
mod queue;
mod r2;
mod redact;
mod tcp;

use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::http::{HeaderValue, Method, Response, StatusCode};
use futures_util::lock::Mutex;
use notify::background::{Background, Broadcaster, Outcome};
use notify::distribute::{is_distribute_request, Outbound};
use notify::email_routing::{needs_email_routing, EmailRoutingApi};
use notify::jobs::{Job, SharedQueue};
use notify::line::{LineEndpoints, LineTokenCache};
use notify::line_config::LINE_CONFIG_PATH;
use notify::line_webhook::LINE_WEBHOOK_PATH;
use notify::lineworks::LineworksApi;
use notify::store::SharedStore;
use notify::{router, LogSink, NotifyState};
use tower_service::Service;
use worker::{
    console_error, event, Context, Date, Env, HttpRequest, MessageBatch, MessageExt, Result,
    WorkerVersionMetadata,
};

use crate::bus::BusBinding;
use crate::email_routing::{CloudflareEmailRouting, NoEmailRouting};
use crate::http::WorkerHttp;
use crate::lineworks::{LineworksBinding, NoLineworks};
use crate::queue::{NoQueue, QueueBinding};
use crate::r2::R2Store;
use crate::redact::{NoRedact, RedactBinding};

/// line-config の秘密値の暗号化の鍵 (Secrets Store。auth-worker・alc-lineworks-worker と同じ secret)。LINE の webhook の
/// 署名の照合 (channel secret の復号) にも使う
const SSO_ENCRYPTION_KEY_BINDING: &str = "SSO_ENCRYPTION_KEY";
/// ippoan/alc-lineworks-worker の Service Binding
const ALC_LINEWORKS_BINDING: &str = "ALC_LINEWORKS";
/// viewer の register-view の認証 (Secrets Store。nuxt-notify・auth-worker などと同じ secret)
const INTERNAL_SHARED_SECRET_BINDING: &str = "INTERNAL_SHARED_SECRET";
/// viewer の Worker (ippoan/nuxt-notify) の公開 URL (wrangler.toml の vars)
const NOTIFY_FRONTEND_URL_VAR: &str = "NOTIFY_FRONTEND_URL";

/// Cloudflare Email Routing の API の token (Secrets Store。権限は Email Routing Addresses Edit)
const CF_EMAIL_ROUTING_TOKEN_BINDING: &str = "CF_EMAIL_ROUTING_TOKEN";
/// Cloudflare の account ID (wrangler.toml の vars。他の Worker と同じく平文)
const CF_ACCOUNT_ID_BINDING: &str = "CF_ACCOUNT_ID";

/// 文書のファイルの R2 の binding (wrangler.toml の `[[r2_buckets]]`。本番と staging で別の bucket)。
/// 無ければ preview は 503・download は 500 (rust の storage 無しと同じ)
const NOTIFY_R2_BINDING: &str = "NOTIFY_R2";

/// 抽出・黒塗りの queue の producer (wrangler.toml の `[[queues.producers]]`。本番 `notify-docs`・staging `notify-docs-staging`)
const NOTIFY_DOCS_QUEUE_BINDING: &str = "NOTIFY_DOCS_QUEUE";
/// auth-worker の named entrypoint `AlcRedactEntrypoint` (alc-redact の黒塗りと抽出)
const ALC_REDACT_BINDING: &str = "ALC_REDACT";
/// nuxt-notify の notify-realtime-bus (黒塗りの終わりの WS の push)
const NOTIFY_REALTIME_BUS_BINDING: &str = "NOTIFY_REALTIME_BUS";
/// realtime-bus の `/broadcast` の認証 (Secrets Store。realtime-bus と同じ secret)
const NOTIFY_REDACT_BROADCAST_SECRET_BINDING: &str = "NOTIFY_REDACT_BROADCAST_SECRET";

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

/// 宛先の保存と email-status の口でだけ token (Secrets Store) と account ID (vars) を引く。どちらかが無い・空・読めないときは
/// 未設定 (Cloudflare は呼ばない)
async fn email_routing_api(env: &Env, needed: bool) -> Arc<dyn EmailRoutingApi> {
    if !needed {
        return Arc::new(NoEmailRouting);
    }
    let account_id = env
        .var(CF_ACCOUNT_ID_BINDING)
        .ok()
        .map(|v| v.to_string())
        .filter(|v| !v.is_empty())
        .map(Arc::from);
    match (
        secret(env, CF_EMAIL_ROUTING_TOKEN_BINDING).await,
        account_id,
    ) {
        (Some(token), Some(account_id)) => Arc::new(CloudflareEmailRouting::new(token, account_id)),
        _ => Arc::new(NoEmailRouting),
    }
}

fn files(env: &Env) -> Option<SharedStore> {
    env.bucket(NOTIFY_R2_BINDING)
        .ok()
        .map(|b| Arc::new(R2Store::new(b)) as SharedStore)
}

fn jobs(env: &Env) -> SharedQueue {
    match env.queue(NOTIFY_DOCS_QUEUE_BINDING) {
        Ok(q) => Arc::new(QueueBinding::new(q)),
        Err(_) => Arc::new(NoQueue),
    }
}

/// realtime-bus の binding と secret が両方あるときだけ push する
async fn bus(env: &Env) -> Option<Arc<dyn Broadcaster>> {
    let fetcher = env.service(NOTIFY_REALTIME_BUS_BINDING).ok()?;
    let secret = secret(env, NOTIFY_REDACT_BROADCAST_SECRET_BINDING).await?;
    Some(Arc::new(BusBinding::new(fetcher, secret)))
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
    // 秘密値を引くのは line-config の POST・LINE の webhook・配信の口だけ (ほかの口では Secrets Store を読まない)
    let post = req.method() == Method::POST;
    let distributing = is_distribute_request(post, req.uri().path());
    let path = req.uri().path();
    let sso_key =
        if distributing || (post && (path == LINE_CONFIG_PATH || path == LINE_WEBHOOK_PATH)) {
            secret(&env, SSO_ENCRYPTION_KEY_BINDING).await
        } else {
            None
        };
    let viewer_secret = if distributing {
        secret(&env, INTERNAL_SHARED_SECRET_BINDING).await
    } else {
        None
    };
    let email_routing =
        email_routing_api(&env, needs_email_routing(req.method(), req.uri().path())).await;
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
        email_routing,
        sso_key,
        outbound,
        files: files(&env),
        jobs: jobs(&env),
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

/// 抽出・黒塗りの consumer (wrangler.toml の `[[queues.consumers]]`、max_batch_size = 1)。1 メッセージずつ処理し、
/// 一時的な失敗は attempt を進めて送り直してから ack する (送れなければ Queues の retry)。DB に繋げなければバッチごと retry
#[event(queue)]
async fn queue(batch: MessageBatch<Job>, env: Env, _ctx: Context) -> Result<()> {
    let pg = match db::connect(&env).await {
        Ok(c) => c,
        Err(e) => {
            console_error!("notify: queue: {e}");
            batch.retry_all();
            return Ok(());
        }
    };
    let background = Background {
        pg: Arc::new(Mutex::new(pg)),
        files: files(&env),
        redact: match env.service(ALC_REDACT_BINDING) {
            Ok(f) => Arc::new(RedactBinding::new(f)),
            Err(_) => Arc::new(NoRedact),
        },
        bus: bus(&env).await,
        log: log_sink(),
        now_ms: Arc::new(|| Date::now().as_millis()),
    };
    let requeue = jobs(&env);
    for message in batch.messages()? {
        match background.run(message.body().clone()).await {
            Outcome::Ack => message.ack(),
            Outcome::Requeue { job, delay_secs } => match requeue.send(job, delay_secs).await {
                Ok(()) => message.ack(),
                Err(_) => {
                    console_error!("notify: queue: requeue failed");
                    message.retry();
                }
            },
        }
    }
    Ok(())
}
