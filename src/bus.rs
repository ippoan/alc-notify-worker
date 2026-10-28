//! crates/notify の [`Broadcaster`] の Workers での実装: nuxt-notify の `notify-realtime-bus` (staging
//! `notify-realtime-bus-staging`) の `POST /broadcast` を Service Binding `NOTIFY_REALTIME_BUS` で呼ぶ
//! (W8c-2、Refs ippoan/rust-alc-api#680。rust の `RedactBroadcaster` は公開 URL へ POST していた)。
//! ヘッダー `X-Broadcast-Secret` は Secrets Store の `NOTIFY_REDACT_BROADCAST_SECRET` (realtime-bus と同じ secret)。
//!
//! 値と future は JS の値で `Send` でないので、`worker::send` の `SendWrapper`・`SendFuture` で包む (Workers は単一 thread)。
//! 失敗は中身を持たない `Err(())` (呼び手がログに固定の語だけ出す)。
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging の実物)。

use std::sync::Arc;

use futures_util::future::BoxFuture;
use notify::background::Broadcaster;
use wasm_bindgen::JsValue;
use worker::send::{SendFuture, SendWrapper};
use worker::{Fetcher, Headers, Method, Request, RequestInit};

/// URL の host は名目の値 (Service Binding は host で経路が決まらない。realtime-bus は path だけを見る)
const BROADCAST_URL: &str = "https://notify-realtime-bus/broadcast";

pub struct BusBinding {
    fetcher: SendWrapper<Fetcher>,
    secret: Arc<str>,
}

impl BusBinding {
    pub fn new(fetcher: Fetcher, secret: Arc<str>) -> Self {
        Self {
            fetcher: SendWrapper::new(fetcher),
            secret,
        }
    }
}

impl Broadcaster for BusBinding {
    fn broadcast(&self, body: Vec<u8>) -> BoxFuture<'_, Result<u16, ()>> {
        Box::pin(SendFuture::new(async move {
            let headers = Headers::new();
            headers
                .set("Content-Type", "application/json")
                .map_err(|_| ())?;
            headers
                .set("X-Broadcast-Secret", &self.secret)
                .map_err(|_| ())?;
            let mut init = RequestInit::new();
            init.with_method(Method::Post)
                .with_headers(headers)
                .with_body(Some(JsValue::from_str(&String::from_utf8_lossy(&body))));
            let request = Request::new_with_init(BROADCAST_URL, &init).map_err(|_| ())?;
            let resp = self.fetcher.fetch_request(request).await.map_err(|_| ())?;
            Ok(resp.status().as_u16())
        }))
    }
}
