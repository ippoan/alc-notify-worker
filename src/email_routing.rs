//! crates/notify の [`EmailRoutingApi`] の Workers での実装: Cloudflare の API (`api.cloudflare.com`) を、Secrets Store の
//! token (`CF_EMAIL_ROUTING_TOKEN`、権限は Email Routing Addresses Edit) と `[vars]` の account ID (`CF_ACCOUNT_ID`) で呼ぶ。
//! 型は `src/http.rs` の `WorkerHttp` と同じ (`worker::Fetch`、redirect は辿らない、`SendFuture` で包む)。
//!
//! token・account ID・URL・ヘッダー・ランタイムの生のエラー文はログに出さない (エラーは段の label だけに落とす)。
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging / 本番の実物)。

use std::sync::Arc;

use futures_util::future::BoxFuture;
use notify::email_routing::{EmailRoutingApi, ErMethod, ErRequest, ErResponse, ErUnreachable};
use wasm_bindgen::JsValue;
use worker::send::SendFuture;
use worker::{Fetch, Headers, Method, Request, RequestInit, RequestRedirect};

const CLOUDFLARE_API: &str = "https://api.cloudflare.com/client/v4/accounts";
/// 応答の本文の上限 (送り先の一覧の 1 ページは小さい)
const BODY_LIMIT: usize = 4 * 1024 * 1024;

pub struct CloudflareEmailRouting {
    token: Arc<str>,
    account_id: Arc<str>,
}

impl CloudflareEmailRouting {
    pub fn new(token: Arc<str>, account_id: Arc<str>) -> Self {
        Self { token, account_id }
    }
}

impl EmailRoutingApi for CloudflareEmailRouting {
    fn configured(&self) -> bool {
        true
    }

    fn call(&self, req: ErRequest) -> BoxFuture<'_, Result<ErResponse, ErUnreachable>> {
        Box::pin(SendFuture::new(async move {
            let headers = Headers::new();
            headers
                .set("Authorization", &format!("Bearer {}", self.token))
                .map_err(|_| ErUnreachable("headers"))?;
            headers
                .set("Accept", "application/json")
                .map_err(|_| ErUnreachable("headers"))?;
            let mut init = RequestInit::new();
            match req.method {
                ErMethod::Post => {
                    headers
                        .set("Content-Type", "application/json")
                        .map_err(|_| ErUnreachable("headers"))?;
                    init.with_method(Method::Post)
                        .with_body(req.body.as_deref().map(JsValue::from_str));
                }
                ErMethod::Get => {
                    init.with_method(Method::Get);
                }
                ErMethod::Delete => {
                    init.with_method(Method::Delete);
                }
            }
            init.with_headers(headers)
                .with_redirect(RequestRedirect::Manual);
            let url = format!("{CLOUDFLARE_API}/{}{}", self.account_id, req.path);
            let request =
                Request::new_with_init(&url, &init).map_err(|_| ErUnreachable("request"))?;
            let mut resp = Fetch::Request(request)
                .send()
                .await
                .map_err(|_| ErUnreachable("fetch"))?;
            let status = resp.status_code();
            let body = resp.bytes().await.map_err(|_| ErUnreachable("body"))?;
            if body.len() > BODY_LIMIT {
                return Err(ErUnreachable("body"));
            }
            Ok(ErResponse { status, body })
        }))
    }
}

/// token か account ID が無い (binding が無い・secret が読めない)。Cloudflare は呼ばない
pub struct NoEmailRouting;

impl EmailRoutingApi for NoEmailRouting {
    fn configured(&self) -> bool {
        false
    }

    fn call(&self, _: ErRequest) -> BoxFuture<'_, Result<ErResponse, ErUnreachable>> {
        Box::pin(async { Err(ErUnreachable("not_configured")) })
    }
}
