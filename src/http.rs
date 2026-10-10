//! crates/notify の [`Http`] の Workers での実装 (`worker::Fetch` で外へ POST する。型は ippoan/alc-lineworks-worker の
//! `src/http.rs`)。LINE の Messaging API と viewer (ippoan/nuxt-notify) の register-view が宛先。
//!
//! **redirect は辿らない** (`redirect: "manual"`)。3xx はそのまま status として返し、crates/notify が 2xx 以外を失敗にする
//! (CF Access の 302 → login の 200 を成功と取り違えない。rust の viewer 登録の `Policy::none()` と同じ)。
//!
//! 値と future は JS の値で `Send` でないので、`worker::send` の `SendFuture` で包む (Workers は単一 thread)。
//! エラーは中身を持たない [`HttpError`] に落とす (URL・ヘッダー・ランタイムの生のエラー文をログに出さない)。
//!
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging / 本番の実物)。

use futures_util::future::BoxFuture;
use notify::http::{Http, HttpError, HttpRequest, HttpResponse};
use wasm_bindgen::JsValue;
use worker::send::SendFuture;
use worker::{Fetch, Headers, Method, Request, RequestInit, RequestRedirect};

/// 応答の本文の上限 (token・エラー・register-view の応答は小さい)
const BODY_LIMIT: usize = 1024 * 1024;

pub struct WorkerHttp;

impl Http for WorkerHttp {
    fn post(&self, req: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(SendFuture::new(async move {
            let headers = Headers::new();
            headers
                .set("Content-Type", req.content_type)
                .map_err(|_| HttpError)?;
            for (name, value) in &req.headers {
                headers.set(name, value).map_err(|_| HttpError)?;
            }
            let body = JsValue::from_str(&String::from_utf8_lossy(&req.body));
            let mut init = RequestInit::new();
            init.with_method(Method::Post)
                .with_headers(headers)
                .with_body(Some(body))
                .with_redirect(RequestRedirect::Manual);
            let request = Request::new_with_init(&req.url, &init).map_err(|_| HttpError)?;
            let mut resp = Fetch::Request(request)
                .send()
                .await
                .map_err(|_| HttpError)?;
            let status = resp.status_code();
            let bytes = resp.bytes().await.map_err(|_| HttpError)?;
            if bytes.len() > BODY_LIMIT {
                return Err(HttpError);
            }
            Ok(HttpResponse {
                status,
                body: bytes,
            })
        }))
    }
}
