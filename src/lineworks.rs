//! crates/notify の [`LineworksApi`] の Workers での実装: ippoan/alc-lineworks-worker の Service Binding (`ALC_LINEWORKS`)。
//! 型は ohishi-exp/rust-leave-worker の `src/bindings.rs` の `IchibanBinding` と同じ (Fetcher と future は JS の値で
//! `Send` でないので、`worker::send` の `SendWrapper`・`SendFuture` で包む。Workers は単一 thread)。
//!
//! エラーは段の label だけに落とす (ランタイムの生のエラー文・本文はログに出さない)。
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging / 本番の実物)。

use futures_util::future::BoxFuture;
use notify::lineworks::{LineworksApi, LwRequest, LwResponse, LwUnreachable};
use wasm_bindgen::JsValue;
use worker::send::{SendFuture, SendWrapper};
use worker::{Fetcher, Headers, Method, Request, RequestInit};

/// URL の host は名目の値 (Service Binding は host で経路が決まらない)
const LINEWORKS_NOMINAL_ORIGIN: &str = "https://alc-lineworks";
/// 応答の本文の上限 (組織のメンバー・ログイン状況の全件でも十分に収まる大きさ)
const BODY_LIMIT: usize = 16 * 1024 * 1024;

pub struct LineworksBinding(SendWrapper<Fetcher>);

impl LineworksBinding {
    pub fn new(fetcher: Fetcher) -> Self {
        Self(SendWrapper::new(fetcher))
    }
}

impl LineworksApi for LineworksBinding {
    fn call(&self, req: LwRequest) -> BoxFuture<'_, Result<LwResponse, LwUnreachable>> {
        Box::pin(SendFuture::new(async move {
            let headers = Headers::new();
            headers
                .set("X-Tenant-ID", &req.tenant_id.to_string())
                .map_err(|_| LwUnreachable("headers"))?;
            headers
                .set("Accept", "application/json")
                .map_err(|_| LwUnreachable("headers"))?;
            let mut init = RequestInit::new();
            if req.post {
                headers
                    .set("Content-Type", "application/json")
                    .map_err(|_| LwUnreachable("headers"))?;
                init.with_method(Method::Post)
                    .with_body(req.body.as_deref().map(JsValue::from_str));
            } else {
                init.with_method(Method::Get);
            }
            init.with_headers(headers);
            let request =
                Request::new_with_init(&format!("{LINEWORKS_NOMINAL_ORIGIN}{}", req.path), &init)
                    .map_err(|_| LwUnreachable("request"))?;
            let resp = self
                .0
                .fetch_request(request)
                .await
                .map_err(|_| LwUnreachable("fetch"))?;
            let status = resp.status().as_u16();
            let body = axum::body::to_bytes(axum::body::Body::new(resp.into_body()), BODY_LIMIT)
                .await
                .map_err(|_| LwUnreachable("body"))?;
            Ok(LwResponse {
                status,
                body: body.to_vec(),
            })
        }))
    }
}

/// binding が無い (wrangler.toml に `ALC_LINEWORKS` が無い env)。LINE WORKS の口は 502 になる
pub struct NoLineworks;

impl LineworksApi for NoLineworks {
    fn call(&self, _: LwRequest) -> BoxFuture<'_, Result<LwResponse, LwUnreachable>> {
        Box::pin(async { Err(LwUnreachable("not_configured")) })
    }
}
