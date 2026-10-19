//! Cloudflare Email Routing の「送り先 (destination address)」の口 (Refs ippoan/alc-app#436)。
//!
//! 宛先 (notify_recipients) のメールアドレスを、Cloudflare の送り先として登録し、確認 (verified) の状態を引く。
//! Email Routing の `send_email` binding で送れるのは、アカウントで確認済みの送り先だけのため。
//! Cloudflare の API は [`EmailRoutingApi`] 越しに呼ぶ (worker の実装は直下の `src/email_routing.rs`、テストはモック)。
//!
//! | 操作 | Cloudflare の API |
//! |---|---|
//! | 送り先を作る | `POST /accounts/{account_id}/email/routing/addresses` 本文 `{"email": ...}` |
//! | 送り先の一覧 | `GET /accounts/{account_id}/email/routing/addresses?page=&per_page=` (応答の `result[].id` が送り先の ID、`result[].verified` が確認日時。未確認は null。`result_info` は `page`・`per_page`・`count`・`total_count` だけで `total_pages` を持たない。ページ数は `total_count` から数える) |
//! | 送り先を消す (再送のときの未確認のものだけ) | `DELETE /accounts/{account_id}/email/routing/addresses/{id}` |
//!
//! - token と account ID は worker が Secrets Store から引く。引けなければ [`EmailRoutingApi::configured`] が `false` で、
//!   Cloudflare は呼ばない (宛先の保存は止めない)。
//! - 送り先を作るのは best-effort: 失敗しても宛先の保存の応答は変えない。ログは固定の語と status か label だけ
//!   (メールアドレス・上流の本文・識別子を出さない)。
//! - 送り先は消さない (アカウント全体で共有で、他の tenant も使うことがあるため)。例外は確認メールの再送
//!   ([`NotifyState::resend_verification`]): Cloudflare に再送の API が無いので、**未確認の**送り先に限り消して作り直す
//!   (作り直しで確認メールが再び飛ぶ)。確認済みの送り先は消さない。

use std::collections::HashMap;

use axum::http::Method;
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::recipients::{EMAIL_STATUS_PATH, RECIPIENTS_PATH};
use crate::NotifyState;

/// account の下の path (token・account ID を持つ実装が `https://api.cloudflare.com/client/v4/accounts/{id}` を前に付ける)
pub const ADDRESSES_PATH: &str = "/email/routing/addresses";
/// 一覧の 1 ページの件数 (Cloudflare の上限)
const PER_PAGE: u32 = 50;
/// 一覧を辿るページ数の上限 (応答の件数が壊れていても止まる)
const MAX_PAGES: u32 = 200;

/// Cloudflare への要求の method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErMethod {
    Get,
    /// `body` を JSON で送る
    Post,
    Delete,
}

/// Cloudflare への 1 リクエスト。認証と account は実装が付ける
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErRequest {
    pub method: ErMethod,
    /// [`ADDRESSES_PATH`] (と query、または `/{id}`)
    pub path: String,
    pub body: Option<String>,
}

/// Cloudflare の応答 (status と本文のバイト列)
#[derive(Debug, Clone)]
pub struct ErResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Cloudflare に届かなかった (fetch の失敗・本文が読めない)。中身はログに出す段の label
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErUnreachable(pub &'static str);

/// Cloudflare の API の口。テストはモックに差し替える
pub trait EmailRoutingApi: Send + Sync {
    /// token と account ID が揃っているか。`false` なら呼ばない
    fn configured(&self) -> bool;
    fn call(&self, req: ErRequest) -> BoxFuture<'_, Result<ErResponse, ErUnreachable>>;
}

/// 比べるとき・Cloudflare に渡すときのメールアドレス (前後の空白を削って小文字。DB の値は変えない)
pub fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

/// この要求が Cloudflare の口を使うか (worker は使うときだけ Secrets Store を引く)
pub fn needs_email_routing(method: &Method, path: &str) -> bool {
    (matches!(*method, Method::POST | Method::PUT) && path.starts_with(RECIPIENTS_PATH))
        || (*method == Method::GET && path == EMAIL_STATUS_PATH)
}

/// 重複 (その送り先は既に在る) の応答か。status は 409、または 4xx で `errors[].message` が既存を言うもの
/// (Cloudflare の API リファレンスに重複のコードの記載が無いので、message で見る)
fn is_already_exists(resp: &ErResponse) -> bool {
    if resp.status == 409 {
        return true;
    }
    let v: Value = serde_json::from_slice(&resp.body).unwrap_or(Value::Null);
    (400..500).contains(&resp.status)
        && v["errors"].as_array().is_some_and(|errors| {
            errors.iter().any(|e| {
                e["message"].as_str().is_some_and(|m| {
                    let m = m.to_lowercase();
                    m.contains("already") || m.contains("exist")
                })
            })
        })
}

#[derive(Deserialize)]
struct ListBody {
    result: Option<Vec<Address>>,
    result_info: Option<ResultInfo>,
}

#[derive(Deserialize)]
struct Address {
    id: String,
    email: String,
    verified: Option<String>,
}

/// 一覧の 1 件: Cloudflare の送り先の ID と、確認済みか
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    pub id: String,
    pub verified: bool,
}

impl From<Address> for Destination {
    fn from(a: Address) -> Self {
        Self {
            id: a.id,
            verified: a.verified.is_some(),
        }
    }
}

#[derive(Deserialize)]
struct ResultInfo {
    /// 資料にはあるが本物の応答には無い。在れば優先する
    total_pages: Option<u32>,
    total_count: Option<u32>,
}

/// 一覧のページ数: `total_pages` があればそれ、無ければ `total_count` を `PER_PAGE` で割って切り上げ (0 件は 1)、
/// どちらも無ければ 1
fn page_count(info: &ResultInfo) -> u32 {
    match (info.total_pages, info.total_count) {
        (Some(pages), _) => pages,
        (None, Some(count)) => count.div_ceil(PER_PAGE).max(1),
        (None, None) => 1,
    }
}

/// 一覧の取得の失敗 (ログは取得した段で出し済み)
#[derive(Debug, PartialEq, Eq)]
pub struct ListFailed;

/// 確認メールの再送の結果
#[derive(Debug, PartialEq, Eq)]
pub enum Resend {
    /// 送り先が無かったので作った (確認メールが飛ぶ)
    Created,
    /// 未確認の送り先を消して作り直した (確認メールが再び飛ぶ)
    Resent,
    /// 確認済み。消さない・作らない
    AlreadyVerified,
}

/// 再送の途中で Cloudflare が失敗した (ログは失敗した段で出し済み)
#[derive(Debug, PartialEq, Eq)]
pub struct ResendFailed;

impl NotifyState {
    /// 送り先を作る (best-effort)。未設定・空のアドレスなら何もしない。既に在る応答は成功として扱う
    pub(crate) async fn ensure_destination(&self, email: &str) {
        let email = normalize_email(email);
        if email.is_empty() || !self.email_routing.configured() {
            return;
        }
        let _ = self.create_destination(&email, "create").await;
    }

    /// 送り先を 1 つ作る (`email` は正規化済み)。既に在る応答は成功。失敗は `notify: email routing <stage> failed (..)` をログに出す
    async fn create_destination(&self, email: &str, stage: &str) -> Result<(), ResendFailed> {
        let req = ErRequest {
            method: ErMethod::Post,
            path: ADDRESSES_PATH.to_owned(),
            body: Some(json!({ "email": email }).to_string()),
        };
        self.call_ok(req, stage, is_already_exists).await
    }

    /// 1 リクエストを打ち、2xx (と `also_ok` の応答) なら成功。失敗は固定の語と label か status だけをログに出す
    async fn call_ok(
        &self,
        req: ErRequest,
        stage: &str,
        also_ok: fn(&ErResponse) -> bool,
    ) -> Result<(), ResendFailed> {
        match self.email_routing.call(req).await {
            Err(ErUnreachable(label)) => {
                (self.log)(&format!("notify: email routing {stage} failed ({label})"));
                Err(ResendFailed)
            }
            Ok(resp) if (200..300).contains(&resp.status) || also_ok(&resp) => Ok(()),
            Ok(resp) => {
                (self.log)(&format!(
                    "notify: email routing {stage} failed (status {})",
                    resp.status
                ));
                Err(ResendFailed)
            }
        }
    }

    /// 確認メールを再送する (`email` は空でないこと。呼び手が未設定でないことを確かめ済み)。
    /// 送り先が無ければ作る・未確認なら消して作り直す・確認済みなら何もしない (消さない)。
    /// 消した後に作るのが失敗したら、送り先が消えたままなのでログの語を `recreate` にして見分ける
    pub(crate) async fn resend_verification(&self, email: &str) -> Result<Resend, ResendFailed> {
        let email = normalize_email(email);
        let destinations = self.list_destinations().await.map_err(|_| ResendFailed)?;
        match destinations.get(&email) {
            None => {
                self.create_destination(&email, "create").await?;
                Ok(Resend::Created)
            }
            Some(Destination { verified: true, .. }) => Ok(Resend::AlreadyVerified),
            Some(Destination { id, .. }) => {
                let req = ErRequest {
                    method: ErMethod::Delete,
                    path: format!("{ADDRESSES_PATH}/{id}"),
                    body: None,
                };
                self.call_ok(req, "delete", |_| false).await?;
                self.create_destination(&email, "recreate").await?;
                Ok(Resend::Resent)
            }
        }
    }

    /// 送り先の全件を引く: 正規化したメールアドレス → 送り先の ID と確認済みか。ページを全部辿る
    pub(crate) async fn list_destinations(
        &self,
    ) -> Result<HashMap<String, Destination>, ListFailed> {
        let mut out = HashMap::new();
        let mut page = 1;
        loop {
            let req = ErRequest {
                method: ErMethod::Get,
                path: format!("{ADDRESSES_PATH}?page={page}&per_page={PER_PAGE}"),
                body: None,
            };
            let resp = self
                .email_routing
                .call(req)
                .await
                .map_err(|ErUnreachable(label)| {
                    (self.log)(&format!("notify: email routing list failed ({label})"));
                    ListFailed
                })?;
            if !(200..300).contains(&resp.status) {
                (self.log)(&format!(
                    "notify: email routing list failed (status {})",
                    resp.status
                ));
                return Err(ListFailed);
            }
            let body: ListBody = serde_json::from_slice(&resp.body).map_err(|_| {
                (self.log)("notify: email routing list failed (body)");
                ListFailed
            })?;
            for a in body.result.unwrap_or_default() {
                out.insert(normalize_email(&a.email), Destination::from(a));
            }
            let total = body.result_info.as_ref().map_or(1, page_count);
            if page >= total.min(MAX_PAGES) {
                return Ok(out);
            }
            page += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(status: u16, body: &str) -> ErResponse {
        ErResponse {
            status,
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn page_count_prefers_total_pages_then_counts_total_count() {
        let info = |total_pages, total_count| ResultInfo {
            total_pages,
            total_count,
        };
        assert_eq!(page_count(&info(Some(3), Some(1))), 3);
        assert_eq!(page_count(&info(Some(2), None)), 2);
        assert_eq!(page_count(&info(None, Some(0))), 1);
        assert_eq!(page_count(&info(None, Some(50))), 1);
        assert_eq!(page_count(&info(None, Some(51))), 2);
        assert_eq!(page_count(&info(None, Some(120))), 3);
        assert_eq!(page_count(&info(None, None)), 1);
    }

    #[test]
    fn list_body_reads_id_and_verified() {
        let body: ListBody = serde_json::from_str(
            r#"{"result":[{"id":"aa11","email":"a@example.test","verified":"2026-01-01T00:00:00Z","created":"x"},{"id":"bb22","email":"b@example.test","verified":null}],"result_info":{"page":1,"per_page":50,"count":2,"total_count":2}}"#,
        )
        .unwrap();
        let got: Vec<Destination> = body
            .result
            .unwrap()
            .into_iter()
            .map(Destination::from)
            .collect();
        assert_eq!(
            got,
            vec![
                Destination {
                    id: "aa11".to_owned(),
                    verified: true
                },
                Destination {
                    id: "bb22".to_owned(),
                    verified: false
                },
            ]
        );
    }

    #[test]
    fn methods_are_distinct() {
        let req = |method| ErRequest {
            method,
            path: ADDRESSES_PATH.to_owned(),
            body: None,
        };
        assert_ne!(req(ErMethod::Get), req(ErMethod::Post));
        assert_ne!(req(ErMethod::Post), req(ErMethod::Delete));
        assert_eq!(format!("{:?}", ErMethod::Delete), "Delete");
    }

    #[test]
    fn normalize_trims_and_lowercases() {
        assert_eq!(normalize_email("  Foo@Example.TEST \n"), "foo@example.test");
        assert_eq!(normalize_email("   "), "");
    }

    #[test]
    fn already_exists_is_detected() {
        assert!(is_already_exists(&resp(409, "")));
        assert!(is_already_exists(&resp(
            400,
            r#"{"success":false,"errors":[{"code":1234,"message":"Destination address already exists"}]}"#
        )));
        assert!(!is_already_exists(&resp(
            400,
            r#"{"errors":[{"code":1,"message":"Invalid email"}]}"#
        )));
        // 4xx 以外は message が何であっても重複としない
        assert!(!is_already_exists(&resp(
            500,
            r#"{"errors":[{"message":"already exists"}]}"#
        )));
        assert!(!is_already_exists(&resp(400, "not json")));
        assert!(!is_already_exists(&resp(400, r#"{"errors":[{"code":1}]}"#)));
    }

    #[test]
    fn only_recipient_writes_and_status_need_cloudflare() {
        assert!(needs_email_routing(&Method::POST, RECIPIENTS_PATH));
        assert!(needs_email_routing(
            &Method::POST,
            "/api/notify/recipients/bulk"
        ));
        assert!(needs_email_routing(
            &Method::PUT,
            "/api/notify/recipients/x"
        ));
        assert!(needs_email_routing(&Method::GET, EMAIL_STATUS_PATH));
        assert!(needs_email_routing(
            &Method::POST,
            "/api/notify/recipients/x/resend-verification"
        ));
        assert!(!needs_email_routing(&Method::GET, RECIPIENTS_PATH));
        assert!(!needs_email_routing(
            &Method::DELETE,
            "/api/notify/recipients/x"
        ));
        assert!(!needs_email_routing(&Method::POST, "/api/notify/groups"));
    }
}
