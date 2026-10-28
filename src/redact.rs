//! crates/notify の [`RedactApi`] の Workers での実装: auth-worker の named entrypoint `AlcRedactEntrypoint` の RPC
//! `redact(pdf)`・`extract(pdf, selfCompany)` (Service Binding `ALC_REDACT`。W8c-2、Refs ippoan/rust-alc-api#680)。
//! auth-worker が ID token を作って alc-redact (GCP Cloud Run) を呼ぶ (SA の鍵はこの worker に配らない)。
//! 型は ippoan/alc-devices-worker の `src/pair.rs` (`fetcher.into_rpc`)。
//!
//! 戻り値は `{status, body: ArrayBuffer, contentType, headers: Record<string,string>}`。throw・形が違う・binding が無いは
//! どれも [`RedactUnreachable`] (consumer が一時的な失敗として送り直す。理由は捨てる)。
//! 値と future は JS の値で `Send` でないので、`worker::send` の `SendWrapper`・`SendFuture` で包む (Workers は単一 thread)。
//!
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging の実物)。

use futures_util::future::BoxFuture;
use notify::background::{RedactApi, RedactReply, RedactUnreachable};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use worker::send::{SendFuture, SendWrapper};
use worker::Fetcher;

#[wasm_bindgen]
extern "C" {
    /// auth-worker の named entrypoint `AlcRedactEntrypoint` (binding `ALC_REDACT`)
    #[wasm_bindgen(extends = js_sys::Object)]
    type AlcRedactRpc;

    #[wasm_bindgen(method, catch)]
    fn redact(this: &AlcRedactRpc, pdf: &js_sys::ArrayBuffer) -> Result<js_sys::Promise, JsValue>;

    #[wasm_bindgen(method, catch)]
    fn extract(
        this: &AlcRedactRpc,
        pdf: &js_sys::ArrayBuffer,
        self_company: &str,
    ) -> Result<js_sys::Promise, JsValue>;
}

pub struct RedactBinding(SendWrapper<AlcRedactRpc>);

impl RedactBinding {
    pub fn new(fetcher: Fetcher) -> Self {
        Self(SendWrapper::new(fetcher.into_rpc::<AlcRedactRpc>()))
    }
}

fn get(value: &JsValue, key: &str) -> Result<JsValue, RedactUnreachable> {
    js_sys::Reflect::get(value, &JsValue::from_str(key)).map_err(|_| RedactUnreachable)
}

/// `{status, body, headers}` を読む
fn reply(value: JsValue) -> Result<RedactReply, RedactUnreachable> {
    let status = get(&value, "status")?.as_f64().ok_or(RedactUnreachable)? as u16;
    let body = get(&value, "body")?
        .dyn_into::<js_sys::ArrayBuffer>()
        .map_err(|_| RedactUnreachable)?;
    let body = js_sys::Uint8Array::new(&body).to_vec();
    let mut headers = Vec::new();
    let h = get(&value, "headers")?;
    if h.is_object() {
        for entry in js_sys::Object::entries(h.unchecked_ref()).iter() {
            let pair: js_sys::Array = entry.unchecked_into();
            if let (Some(k), Some(v)) = (pair.get(0).as_string(), pair.get(1).as_string()) {
                headers.push((k.to_lowercase(), v));
            }
        }
    }
    Ok(RedactReply {
        status,
        body,
        headers,
    })
}

async fn call(promise: Result<js_sys::Promise, JsValue>) -> Result<RedactReply, RedactUnreachable> {
    let value = JsFuture::from(promise.map_err(|_| RedactUnreachable)?)
        .await
        .map_err(|_| RedactUnreachable)?;
    reply(value)
}

fn buffer(pdf: &[u8]) -> js_sys::ArrayBuffer {
    js_sys::Uint8Array::from(pdf).buffer()
}

impl RedactApi for RedactBinding {
    fn redact(&self, pdf: Vec<u8>) -> BoxFuture<'_, Result<RedactReply, RedactUnreachable>> {
        Box::pin(SendFuture::new(async move {
            call(self.0.redact(&buffer(&pdf))).await
        }))
    }

    fn extract(
        &self,
        pdf: Vec<u8>,
        self_company: String,
    ) -> BoxFuture<'_, Result<RedactReply, RedactUnreachable>> {
        Box::pin(SendFuture::new(async move {
            call(self.0.extract(&buffer(&pdf), &self_company)).await
        }))
    }
}

/// binding が無い env (送り直して、最後の回に失敗を書く)
pub struct NoRedact;

impl RedactApi for NoRedact {
    fn redact(&self, _: Vec<u8>) -> BoxFuture<'_, Result<RedactReply, RedactUnreachable>> {
        Box::pin(async { Err(RedactUnreachable) })
    }

    fn extract(
        &self,
        _: Vec<u8>,
        _: String,
    ) -> BoxFuture<'_, Result<RedactReply, RedactUnreachable>> {
        Box::pin(async { Err(RedactUnreachable) })
    }
}
