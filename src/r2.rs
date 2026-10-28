//! 文書のファイルの置き場 ([`notify::store::FileStore`]) の Workers での実装: R2 の binding `NOTIFY_R2`
//! (ippoan/alc-carins-worker の `src/r2.rs` の型の、読みの `get` だけ)。
//!
//! `FileStore` は `Send + Sync` で `Send` な future を返す (axum の state と handler が要求する)。
//! R2 の値と future は JS の値で `Send` でないので、`worker::send` の `SendWrapper`・`SendFuture` で包む
//! (Workers は単一 thread)。
//!
//! エラーは段の名前だけを持つ `StoreError` に落とす。**key とランタイムの生のエラー文はログに出さない。**
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging の実物)。

use futures_util::future::BoxFuture;
use notify::store::{FileStore, StoreError};
use worker::send::{SendFuture, SendWrapper};
use worker::Bucket;

/// R2 の bucket (本番と staging で別の bucket)
pub struct R2Store(SendWrapper<Bucket>);

impl R2Store {
    pub fn new(bucket: Bucket) -> Self {
        Self(SendWrapper::new(bucket))
    }
}

impl FileStore for R2Store {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(SendFuture::new(async move {
            let object = self.0.get(key).execute().await;
            let object = object
                .map_err(|_| StoreError("get"))?
                .ok_or(StoreError("missing"))?;
            let body = object.body().ok_or(StoreError("get body"))?;
            body.bytes().await.map_err(|_| StoreError("read body"))
        }))
    }
}
