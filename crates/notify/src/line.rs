//! LINE (Messaging API) への送信。rust-alc-api の `crates/alc-notify/src/clients/line.rs` の移植
//! (JWT assertion で channel access token v2.1 を取り、push で text / image を送る)。follow した利用者の表示名
//! (profile API) も引く ([`LineClient::display_name`]。rust の `line_webhook.rs` の `get_user_display_name`)。
//!
//! - JWT は `alc_worker_crypto::jwt::sign_rs256` に `kid` と rust と同じ claim を渡して組む ([`build_jwt_assertion`])
//! - channel access token は isolate の中に置く ([`LineTokenCache`]。ippoan/alc-lineworks-worker の token の cache と同じ方式で、
//!   期限が `now + 60` 秒より先なら使い回す)。rust は 1 リクエストの中だけの cache だった。isolate は tenant を跨ぐので、
//!   キーは (channel_id, key_id)
//! - 外への POST は [`Http`] の trait 越し。URL は [`LineEndpoints`] (テストで差し替え)
//!
//! エラーは段と status だけ ([`LineError`])。rust のように上流の応答本文をログ・エラーに載せない。

use std::collections::HashMap;
use std::sync::Mutex;

use alc_worker_crypto::jwt::sign_rs256;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::http::{Http, HttpRequest};

pub const PROD_TOKEN_ENDPOINT: &str = "https://api.line.me/oauth2/v2.1/token";
pub const PROD_PUSH_ENDPOINT: &str = "https://api.line.me/v2/bot/message/push";
/// 末尾に user id を足す
pub const PROD_PROFILE_ENDPOINT: &str = "https://api.line.me/v2/bot/profile/";
/// 期限まで残りがこれ以下の token は使わない (rust と同じ 60 秒)
const TOKEN_EXPIRY_MARGIN_SECS: u64 = 60;
const CLIENT_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// LINE の API の URL
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineEndpoints {
    pub token: String,
    pub push: String,
    /// 末尾に user id を足す
    pub profile: String,
}

impl Default for LineEndpoints {
    fn default() -> Self {
        Self {
            token: PROD_TOKEN_ENDPOINT.to_owned(),
            push: PROD_PUSH_ENDPOINT.to_owned(),
            profile: PROD_PROFILE_ENDPOINT.to_owned(),
        }
    }
}

/// JWT 方式の LINE 設定 (復号済み)。rust の `LineConfig` のうち送信に使うもの (`channel_secret` は JWT 方式では使わない)
#[derive(Clone)]
pub struct LineConfig {
    pub channel_id: String,
    pub key_id: String,
    /// PEM
    pub private_key: String,
}

/// rust の `JwtClaims` と同じ claim
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JwtClaims {
    pub iss: String,
    pub sub: String,
    pub aud: String,
    pub exp: u64,
    pub token_exp: u64,
    pub token_type: String,
}

/// 失敗の段 (本文・鍵を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineError {
    /// Private Key が読めない (JWT を組めない)
    Jwt,
    /// LINE に届かない
    Transport,
    /// token の口が 2xx 以外
    TokenStatus(u16),
    /// token の応答が読めない
    TokenParse,
    /// push が 2xx 以外
    SendStatus(u16),
}

/// LINE 向けの JWT assertion (rust の `build_jwt_assertion` と同じ header と claim)。
///
/// - `exp`       — assertion の有効期限。UNIX 秒 (max now+30分)
/// - `token_exp` — 発行される access token の有効期間、**秒数** (max 30日 = 2592000)
pub fn build_jwt_assertion(config: &LineConfig, now: u64) -> Result<String, LineError> {
    let claims = JwtClaims {
        iss: config.channel_id.clone(),
        sub: config.channel_id.clone(),
        aud: "https://api.line.me/".to_owned(),
        exp: now + 1800,
        token_exp: 60 * 60 * 24 * 30,
        token_type: "Bearer".to_owned(),
    };
    sign_rs256(&config.private_key, Some(&config.key_id), &claims).map_err(|_| LineError::Jwt)
}

type CacheKey = (String, String);

/// isolate の中の channel access token の置き場 ((channel_id, key_id) → (token, 期限の UNIX 秒))
#[derive(Default)]
pub struct LineTokenCache(Mutex<HashMap<CacheKey, (String, u64)>>);

impl LineTokenCache {
    fn get(&self, key: &CacheKey, now: u64) -> Option<String> {
        let map = self.0.lock().unwrap_or_else(|e| e.into_inner());
        map.get(key)
            .filter(|(_, expires_at)| *expires_at > now + TOKEN_EXPIRY_MARGIN_SECS)
            .map(|(token, _)| token.clone())
    }

    fn put(&self, key: CacheKey, token: String, expires_at: u64) {
        let mut map = self.0.lock().unwrap_or_else(|e| e.into_inner());
        map.insert(key, (token, expires_at));
    }

    /// 持っている token の数 (テスト用)
    pub fn len(&self) -> usize {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

#[derive(Deserialize)]
struct Profile {
    #[serde(rename = "displayName")]
    display_name: String,
}

/// 英数字と `-._~` 以外を `%XX` に
fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// `application/x-www-form-urlencoded`
fn form_urlencode(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// 呼び出しに要るもの一式
pub struct LineClient<'a> {
    pub http: &'a dyn Http,
    pub endpoints: &'a LineEndpoints,
    pub cache: &'a LineTokenCache,
    pub now: u64,
}

impl LineClient<'_> {
    /// channel access token。cache に在ればそれを返す
    pub async fn access_token(&self, config: &LineConfig) -> Result<String, LineError> {
        let key = (config.channel_id.clone(), config.key_id.clone());
        if let Some(hit) = self.cache.get(&key, self.now) {
            return Ok(hit);
        }
        let jwt = build_jwt_assertion(config, self.now)?;
        let form = form_urlencode(&[
            ("grant_type", "client_credentials"),
            ("client_assertion_type", CLIENT_ASSERTION_TYPE),
            ("client_assertion", &jwt),
        ]);
        let resp = self
            .http
            .post(HttpRequest {
                url: self.endpoints.token.clone(),
                content_type: "application/x-www-form-urlencoded",
                headers: Vec::new(),
                body: form.into_bytes(),
            })
            .await
            .map_err(|_| LineError::Transport)?;
        if !resp.is_success() {
            return Err(LineError::TokenStatus(resp.status));
        }
        let token: TokenResponse =
            serde_json::from_slice(&resp.body).map_err(|_| LineError::TokenParse)?;
        self.cache
            .put(key, token.access_token.clone(), self.now + token.expires_in);
        Ok(token.access_token)
    }

    /// 利用者の表示名 (profile API)。token が取れない・届かない・2xx 以外・読めないときは `None` (呼び手が代わりの名前にする)。
    /// user id は path に入れるので `%XX` にする
    pub async fn display_name(&self, config: &LineConfig, user_id: &str) -> Option<String> {
        let token = self.access_token(config).await.ok()?;
        let resp = self
            .http
            .get(
                format!("{}{}", self.endpoints.profile, percent_encode(user_id)),
                vec![("Authorization", format!("Bearer {token}"))],
            )
            .await
            .ok()?;
        if !resp.is_success() {
            return None;
        }
        serde_json::from_slice::<Profile>(&resp.body)
            .ok()
            .map(|p| p.display_name)
    }

    /// 1 通を push する
    async fn push(
        &self,
        config: &LineConfig,
        user_id: &str,
        message: Value,
    ) -> Result<(), LineError> {
        let token = self.access_token(config).await?;
        let body = json!({"to": user_id, "messages": [message]});
        let resp = self
            .http
            .post(HttpRequest {
                url: self.endpoints.push.clone(),
                content_type: "application/json",
                headers: vec![("Authorization", format!("Bearer {token}"))],
                body: body.to_string().into_bytes(),
            })
            .await
            .map_err(|_| LineError::Transport)?;
        if !resp.is_success() {
            return Err(LineError::SendStatus(resp.status));
        }
        Ok(())
    }

    pub async fn push_text(
        &self,
        config: &LineConfig,
        user_id: &str,
        text: &str,
    ) -> Result<(), LineError> {
        self.push(config, user_id, json!({"type": "text", "text": text}))
            .await
    }

    /// 画像を push する (`original_url` は最大 10 MB / JPEG・PNG、`preview_url` は最大 1 MB。同じ URL でよい)
    pub async fn push_image(
        &self,
        config: &LineConfig,
        user_id: &str,
        original_url: &str,
        preview_url: &str,
    ) -> Result<(), LineError> {
        self.push(
            config,
            user_id,
            json!({
                "type": "image",
                "originalContentUrl": original_url,
                "previewImageUrl": preview_url,
            }),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use rsa::signature::Verifier;
    use rsa::RsaPrivateKey;
    use sha2::Sha256;

    use super::*;
    use crate::http::fake::FakeHttp;

    /// テスト専用の鍵 (生成は 1 回だけ。1024 bit = テスト用の大きさ。ソースに鍵の文字列を置かない)
    fn test_key() -> &'static RsaPrivateKey {
        static KEY: OnceLock<RsaPrivateKey> = OnceLock::new();
        KEY.get_or_init(|| RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 1024).unwrap())
    }

    fn config(channel_id: &str) -> LineConfig {
        LineConfig {
            channel_id: channel_id.into(),
            key_id: "test-key-id".into(),
            private_key: test_key().to_pkcs8_pem(LineEnding::LF).unwrap().to_string(),
        }
    }

    fn b64json(part: &str) -> Value {
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(part).unwrap()).unwrap()
    }

    fn endpoints() -> LineEndpoints {
        LineEndpoints {
            token: "https://line.test/token".into(),
            push: "https://line.test/push".into(),
            profile: "https://line.test/profile/".into(),
        }
    }

    fn token_ok(http: &FakeHttp, token: &str, expires_in: u64) {
        http.reply(
            200,
            &json!({"access_token": token, "expires_in": expires_in, "token_type": "Bearer"})
                .to_string(),
        );
    }

    fn client<'a>(
        http: &'a FakeHttp,
        e: &'a LineEndpoints,
        cache: &'a LineTokenCache,
    ) -> LineClient<'a> {
        LineClient {
            http,
            endpoints: e,
            cache,
            now: 1_000,
        }
    }

    #[test]
    fn jwt_assertion_has_kid_and_line_claims() {
        let now = 1_700_000_000;
        let jwt = build_jwt_assertion(&config("test-channel-id"), now).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(
            b64json(parts[0]),
            json!({"alg": "RS256", "typ": "JWT", "kid": "test-key-id"})
        );
        let claims: JwtClaims = serde_json::from_value(b64json(parts[1])).unwrap();
        assert_eq!(
            claims,
            JwtClaims {
                iss: "test-channel-id".into(),
                sub: "test-channel-id".into(),
                aud: "https://api.line.me/".into(),
                exp: now + 1800,
                token_exp: 2_592_000,
                token_type: "Bearer".into(),
            }
        );
        // 署名は公開鍵で通る
        let sig =
            Signature::try_from(URL_SAFE_NO_PAD.decode(parts[2]).unwrap().as_slice()).unwrap();
        VerifyingKey::<Sha256>::new(test_key().to_public_key())
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &sig)
            .unwrap();
    }

    #[test]
    fn jwt_assertion_rejects_invalid_pem() {
        let mut c = config("x");
        c.private_key = "not a pem".into();
        assert_eq!(build_jwt_assertion(&c, 0), Err(LineError::Jwt));
    }

    #[test]
    fn default_endpoints_are_prod() {
        let e = LineEndpoints::default();
        assert_eq!(e.token, PROD_TOKEN_ENDPOINT);
        assert_eq!(e.push, PROD_PUSH_ENDPOINT);
        assert_eq!(e.profile, PROD_PROFILE_ENDPOINT);
    }

    #[test]
    fn form_is_urlencoded() {
        assert_eq!(
            form_urlencode(&[("a b", "x:y/z"), ("k", "A-._~0")]),
            "a%20b=x%3Ay%2Fz&k=A-._~0"
        );
    }

    #[tokio::test]
    async fn access_token_is_issued_once_and_cached() {
        let http = FakeHttp::default();
        token_ok(&http, "cached-token", 2_592_000);
        let (e, cache) = (endpoints(), LineTokenCache::default());
        assert!(cache.is_empty());
        let c = client(&http, &e, &cache);
        assert_eq!(c.access_token(&config("c1")).await.unwrap(), "cached-token");
        assert_eq!(c.access_token(&config("c1")).await.unwrap(), "cached-token");
        let calls = http.calls();
        assert_eq!(calls.len(), 1, "2 回目は cache");
        assert_eq!(calls[0].url, "https://line.test/token");
        assert_eq!(calls[0].content_type, "application/x-www-form-urlencoded");
        assert!(calls[0].headers.is_empty());
        let form = String::from_utf8(calls[0].body.clone()).unwrap();
        assert!(form.starts_with(
            "grant_type=client_credentials&client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer&client_assertion="
        ));
        assert_eq!(cache.len(), 1);
    }

    /// 期限が now + 60 秒以内の token は使わない。channel が違えば別の token
    #[tokio::test]
    async fn access_token_refreshes_near_expiry_and_keys_by_channel() {
        let http = FakeHttp::default();
        token_ok(&http, "short", 60);
        token_ok(&http, "fresh", 61);
        token_ok(&http, "other", 3_600);
        let (e, cache) = (endpoints(), LineTokenCache::default());
        let c = client(&http, &e, &cache);
        assert_eq!(c.access_token(&config("c1")).await.unwrap(), "short");
        assert_eq!(c.access_token(&config("c1")).await.unwrap(), "fresh");
        assert_eq!(c.access_token(&config("c1")).await.unwrap(), "fresh");
        assert_eq!(c.access_token(&config("c2")).await.unwrap(), "other");
        assert_eq!(http.calls().len(), 3);
        assert_eq!(cache.len(), 2);
    }

    #[tokio::test]
    async fn access_token_failures_are_stage_only() {
        let (e, cache) = (endpoints(), LineTokenCache::default());
        let http = FakeHttp::default();
        http.reply(400, r#"{"error":"invalid_client"}"#)
            .reply(200, "not json")
            .fail();
        let c = client(&http, &e, &cache);
        let cfg = config("c");
        assert_eq!(c.access_token(&cfg).await, Err(LineError::TokenStatus(400)));
        assert_eq!(c.access_token(&cfg).await, Err(LineError::TokenParse));
        assert_eq!(c.access_token(&cfg).await, Err(LineError::Transport));
        let mut bad = cfg.clone();
        bad.private_key = "x".into();
        assert_eq!(c.access_token(&bad).await, Err(LineError::Jwt));
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn push_text_and_image_bodies() {
        let http = FakeHttp::default();
        token_ok(&http, "tok", 86_400);
        let (e, cache) = (endpoints(), LineTokenCache::default());
        let c = client(&http, &e, &cache);
        let cfg = config("c");
        c.push_text(&cfg, "U123", "hello").await.unwrap();
        c.push_image(&cfg, "U123", "https://x/o.jpg", "https://x/p.jpg")
            .await
            .unwrap();
        let calls = http.calls();
        assert_eq!(calls.len(), 3);
        for push in &calls[1..] {
            assert_eq!(push.url, "https://line.test/push");
            assert_eq!(push.content_type, "application/json");
            assert_eq!(
                push.headers,
                vec![("Authorization", "Bearer tok".to_owned())]
            );
        }
        let body = |i: usize| serde_json::from_slice::<Value>(&calls[i].body).unwrap();
        assert_eq!(
            body(1),
            json!({"to": "U123", "messages": [{"type": "text", "text": "hello"}]})
        );
        assert_eq!(
            body(2),
            json!({"to": "U123", "messages": [{
                "type": "image",
                "originalContentUrl": "https://x/o.jpg",
                "previewImageUrl": "https://x/p.jpg",
            }]})
        );
    }

    #[tokio::test]
    async fn display_name_reads_profile() {
        let http = FakeHttp::default();
        token_ok(&http, "tok", 86_400);
        http.reply(200, r#"{"displayName":"山田","userId":"U1"}"#)
            .reply(404, "")
            .reply(200, "not json")
            .fail();
        let (e, cache) = (endpoints(), LineTokenCache::default());
        let c = client(&http, &e, &cache);
        let cfg = config("c");
        assert_eq!(c.display_name(&cfg, "U1").await.as_deref(), Some("山田"));
        assert_eq!(c.display_name(&cfg, "U/2?x").await, None);
        assert_eq!(c.display_name(&cfg, "U3").await, None);
        assert_eq!(c.display_name(&cfg, "U4").await, None);
        let calls = http.calls();
        assert_eq!(calls[1].url, "https://line.test/profile/U1");
        assert_eq!(calls[1].content_type, "");
        assert!(calls[1].body.is_empty());
        assert_eq!(
            calls[1].headers,
            vec![("Authorization", "Bearer tok".to_owned())]
        );
        assert_eq!(calls[2].url, "https://line.test/profile/U%2F2%3Fx");
        // token が取れなければ profile を引かない
        let http = FakeHttp::default();
        http.reply(500, "");
        let cache = LineTokenCache::default();
        let c = client(&http, &e, &cache);
        assert_eq!(c.display_name(&cfg, "U").await, None);
        assert_eq!(http.calls().len(), 1);
    }

    #[tokio::test]
    async fn push_failures() {
        let http = FakeHttp::default();
        token_ok(&http, "tok", 86_400);
        http.reply(400, "bad request").fail();
        let (e, cache) = (endpoints(), LineTokenCache::default());
        let c = client(&http, &e, &cache);
        let cfg = config("c");
        assert_eq!(
            c.push_text(&cfg, "U", "t").await,
            Err(LineError::SendStatus(400))
        );
        assert_eq!(
            c.push_image(&cfg, "U", "o", "p").await,
            Err(LineError::Transport)
        );
        // token が取れなければ push しない
        let http = FakeHttp::default();
        http.reply(500, "");
        let cache = LineTokenCache::default();
        let c = client(&http, &e, &cache);
        assert_eq!(
            c.push_text(&cfg, "U", "t").await,
            Err(LineError::TokenStatus(500))
        );
        assert_eq!(http.calls().len(), 1);
    }
}
