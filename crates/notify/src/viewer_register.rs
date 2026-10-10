//! viewer の Worker (ippoan/nuxt-notify) の KV に `view:{token}` を登録する (rust-alc-api の
//! `crates/alc-notify/src/viewer_register.rs` の移植)。配信のときに token → {r2_key, メタ} を
//! `{NOTIFY_FRONTEND_URL}/api/notify/register-view` へ **best-effort** で POST する (失敗しても配信は止めない)。
//!
//! 認証は `INTERNAL_SHARED_SECRET` (Secrets Store。全 stack 同値) を `x-notify-internal-secret` ヘッダーで。
//! redirect は辿らない ([`crate::http`])。CF Access に当たった 302 は 2xx でないので失敗にする。

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::http::{Http, HttpRequest};

pub const REGISTER_VIEW_PATH: &str = "/api/notify/register-view";
pub const SECRET_HEADER: &str = "x-notify-internal-secret";

/// 登録の失敗 (status か、届かない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterError {
    Transport,
    Status(u16),
}

/// register-view の URL (`frontend` の末尾の `/` は落とす)
pub fn register_endpoint(frontend: &str) -> String {
    format!("{}{REGISTER_VIEW_PATH}", frontend.trim_end_matches('/'))
}

/// register-view へ送る JSON (rust の `build_register_body` と同じ形)。`tenant_id` は viewer が既読を
/// `read:{tenant_id}:{document_id}:{recipient_id}` に tenant を前置して記録するのに要る
#[allow(clippy::too_many_arguments)]
pub fn build_register_body(
    token: Uuid,
    tenant_id: Uuid,
    r2_key: &str,
    document_id: Uuid,
    recipient_id: Uuid,
    file_name: Option<&str>,
    file_size_bytes: Option<i64>,
    source_subject: Option<&str>,
    source_sender: Option<&str>,
    source_received_at: Option<DateTime<Utc>>,
    expire_at: DateTime<Utc>,
) -> Value {
    json!({
        "token": token.to_string(),
        "tenant_id": tenant_id.to_string(),
        "r2_key": r2_key,
        "document_id": document_id.to_string(),
        "recipient_id": recipient_id.to_string(),
        "file_name": file_name,
        "file_size_bytes": file_size_bytes,
        "source_subject": source_subject,
        "source_sender": source_sender,
        "source_received_at": source_received_at.map(|t| t.to_rfc3339()),
        "expire_at": expire_at.to_rfc3339(),
    })
}

/// KV に view を登録する。2xx 以外 (3xx を含む)・届かない → `Err`
pub async fn register(
    http: &dyn Http,
    endpoint: &str,
    secret: &str,
    body: &Value,
) -> Result<(), RegisterError> {
    let resp = http
        .post(HttpRequest {
            url: endpoint.to_owned(),
            content_type: "application/json",
            headers: vec![(SECRET_HEADER, secret.to_owned())],
            body: body.to_string().into_bytes(),
        })
        .await
        .map_err(|_| RegisterError::Transport)?;
    if resp.is_success() {
        Ok(())
    } else {
        Err(RegisterError::Status(resp.status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::fake::FakeHttp;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn body_shape() {
        let (token, tenant, doc, rcp) = (
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
        );
        let body = build_register_body(
            token,
            tenant,
            "tenant/m/file.pdf",
            doc,
            rcp,
            Some("file.pdf"),
            Some(2048),
            Some("件名"),
            Some("from@example.com"),
            Some(ts("2026-06-27T00:00:00Z")),
            ts("2026-07-04T00:00:00Z"),
        );
        assert_eq!(
            body,
            json!({
                "token": token.to_string(),
                "tenant_id": tenant.to_string(),
                "r2_key": "tenant/m/file.pdf",
                "document_id": doc.to_string(),
                "recipient_id": rcp.to_string(),
                "file_name": "file.pdf",
                "file_size_bytes": 2048,
                "source_subject": "件名",
                "source_sender": "from@example.com",
                "source_received_at": "2026-06-27T00:00:00+00:00",
                "expire_at": "2026-07-04T00:00:00+00:00",
            })
        );
        let body = build_register_body(
            token,
            tenant,
            "k",
            doc,
            rcp,
            None,
            None,
            None,
            None,
            None,
            ts("2026-07-04T00:00:00Z"),
        );
        for k in [
            "file_name",
            "file_size_bytes",
            "source_subject",
            "source_sender",
            "source_received_at",
        ] {
            assert!(body[k].is_null(), "{k}");
        }
    }

    #[test]
    fn endpoint_trims_slash() {
        assert_eq!(
            register_endpoint("https://v.test/"),
            "https://v.test/api/notify/register-view"
        );
    }

    #[tokio::test]
    async fn register_sends_secret_header_and_treats_non_2xx_as_failure() {
        let http = FakeHttp::default();
        http.reply(200, r#"{"ok":true}"#)
            .reply(401, "")
            .reply(302, "")
            .fail();
        let body = json!({"token": "t"});
        let ep = "https://v.test/api/notify/register-view";
        assert_eq!(register(&http, ep, "sek", &body).await, Ok(()));
        assert_eq!(
            register(&http, ep, "sek", &body).await,
            Err(RegisterError::Status(401))
        );
        // CF Access の 302 は辿らずに失敗 (Http の実装は redirect を辿らない)
        assert_eq!(
            register(&http, ep, "sek", &body).await,
            Err(RegisterError::Status(302))
        );
        assert_eq!(
            register(&http, ep, "sek", &body).await,
            Err(RegisterError::Transport)
        );
        let calls = http.calls();
        assert_eq!(calls[0].url, ep);
        assert_eq!(calls[0].content_type, "application/json");
        assert_eq!(
            calls[0].headers,
            vec![("x-notify-internal-secret", "sek".to_owned())]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&calls[0].body).unwrap(),
            body
        );
    }
}
