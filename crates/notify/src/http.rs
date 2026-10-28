//! 外への POST (LINE の Messaging API・viewer の register-view) と GET (LINE の profile) の口。worker は `worker::Fetch`
//! (直下の `src/http.rs`)、テストはモックに差し替える。
//!
//! **redirect は辿らない** (3xx はそのまま応答として返し、呼び手が 2xx 以外を失敗にする)。rust の viewer 登録が
//! `redirect::Policy::none()` にしていた理由 (CF Access の 302 → login の 200 を成功と取り違える) を、全部の宛先に当てる。
//! エラーは中身を持たない [`HttpError`] (URL・ヘッダー・ランタイムの生の文をログに出さない)。

use futures_util::future::BoxFuture;

/// 1 回の POST
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub url: String,
    pub content_type: &'static str,
    /// `Content-Type` のほかに付けるヘッダー (`Authorization`・`x-notify-internal-secret`)
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
}

/// 応答 (status と本文のバイト列)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// 届かなかった (fetch の失敗・本文が読めない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpError;

pub trait Http: Send + Sync {
    fn post(&self, req: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>>;

    /// 本文なしの GET (`headers` は `Authorization` など)
    fn get(
        &self,
        url: String,
        headers: Vec<(&'static str, String)>,
    ) -> BoxFuture<'_, Result<HttpResponse, HttpError>>;
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// 単体テストの偽物: 受けたリクエストを残し、積んだ応答を順に返す (尽きたら 200 `{}`)
#[cfg(test)]
pub(crate) mod fake {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    pub struct FakeHttp {
        pub calls: Mutex<Vec<HttpRequest>>,
        replies: Mutex<VecDeque<Result<HttpResponse, HttpError>>>,
    }

    impl FakeHttp {
        pub fn reply(&self, status: u16, body: &str) -> &Self {
            self.replies.lock().unwrap().push_back(Ok(HttpResponse {
                status,
                body: body.as_bytes().to_vec(),
            }));
            self
        }

        pub fn fail(&self) -> &Self {
            self.replies.lock().unwrap().push_back(Err(HttpError));
            self
        }

        pub fn calls(&self) -> Vec<HttpRequest> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl Http for FakeHttp {
        fn post(&self, req: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>> {
            self.calls.lock().unwrap().push(req);
            let reply = self.replies.lock().unwrap().pop_front();
            let reply = reply.unwrap_or(Ok(HttpResponse {
                status: 200,
                body: b"{}".to_vec(),
            }));
            Box::pin(async move { reply })
        }

        fn get(
            &self,
            url: String,
            headers: Vec<(&'static str, String)>,
        ) -> BoxFuture<'_, Result<HttpResponse, HttpError>> {
            self.post(HttpRequest {
                url,
                content_type: "",
                headers,
                body: Vec::new(),
            })
        }
    }
}
