//! 配信の口 (rust-alc-api の `crates/alc-notify/src/distribute.rs` の移植。path・method・JSON・status は rust と同じ)。
//!
//! | method | path | 成功 | 失敗 |
//! |---|---|---|---|
//! | POST | `/api/notify/documents/{id}/distribute` | 200 `{sent, failed, total}` / 宛先 0 件は 200 `{"message": "No enabled recipients"}` | 404 文書が無い (別 tenant を含む) / 400 黒塗りが済んでいない / 500 DB |
//! | POST | `/api/notify/test-distribute` | 200 `{sent, failed, total}` (`{"message", "recipient_ids"}` の宛先へ text だけ) | 400 `recipient_ids` が空 / 500 DB |
//!
//! distribute の流れ (rust と同じ順):
//! 1. 文書を読む。`redaction_status` が `completed` / `skipped` 以外は 400 (金額入りの原本を送らない安全装置、migration 109)
//! 2. 宛先: `target.group_id` > `target.recipient_ids` > 全 enabled ([`resolve_target_recipients`])
//! 3. 文書を `in_progress` にし、宛先ごとに notify_deliveries を作る。配信した人 (`X-User-ID` 系から復元した AuthUser) と
//!    既定 (7 日) 以外の閲覧期限 (`retention_days`、1..90 に丸める) は 1 つの短い transaction で後から書く (best-effort)
//! 4. 宛先ごとに viewer の KV へ登録 (best-effort、[`crate::viewer_register`]) → text → 画像 (配車手配票の PDF のときだけ。
//!    失敗しても成功扱い) を送り、行を `sent` / `failed` (理由は [`Failure::reason`] の固定の語) にする
//! 5. 文書を `completed` にする
//!
//! 送信: LINE は [`crate::line`] (鍵は notify_line_configs の暗号文を `SSO_ENCRYPTION_KEY` で復号)、LINE WORKS は
//! ippoan/alc-lineworks-worker の `/messages` を Service Binding で呼ぶ (`bot_config_id` を渡さない = tenant の既定の bot。
//! CLAUDE.md の bot の選び方の契約)。外への fetch・lineworks-worker の呼び出しは transaction の外 (DB の読み書きは
//! それぞれ短い transaction に分ける)。
//!
//! rust との違い: LINE の `channel_secret` は JWT 方式の送信で使わないので復号しない (rust は復号して捨てていた)。
//! ログ・DB に残す理由は固定の語だけ (rust は上流の応答本文・宛先の名前を載せていた)。

use std::collections::HashSet;
use std::sync::Arc;

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_crypto::secret::decrypt_pem_secret;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::http::Http;
use crate::line::{LineClient, LineConfig, LineEndpoints, LineError, LineTokenCache};
use crate::lineworks::{LwRequest, LwUnreachable, LW_MESSAGES_PATH};
use crate::pg::{self, NotifyDocument, NotifyRecipient};
use crate::viewer_register::{build_register_body, register, register_endpoint};
use crate::NotifyState;

pub const DISTRIBUTE_PATH: &str = "/api/notify/documents/{id}/distribute";
pub const TEST_DISTRIBUTE_PATH: &str = "/api/notify/test-distribute";
const DOCUMENTS_PREFIX: &str = "/api/notify/documents/";
const DISTRIBUTE_SUFFIX: &str = "/distribute";
/// `NOTIFY_FRONTEND_URL` が無いときの viewer の URL (rust の `read_tracker::frontend_url` と同じ)
pub const DEFAULT_FRONTEND_URL: &str = "https://notify.example.com";
/// migration 106 の `expire_at` の既定 (`NOW() + 7 days`)
const DEFAULT_RETENTION_DAYS: i64 = 7;

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(DISTRIBUTE_PATH, post(distribute))
        .route(TEST_DISTRIBUTE_PATH, post(test_distribute))
}

/// 配信の口への POST か (worker はこのときだけ `SSO_ENCRYPTION_KEY`・`INTERNAL_SHARED_SECRET` を Secrets Store から引く)
pub fn is_distribute_request(post: bool, path: &str) -> bool {
    post && (path == TEST_DISTRIBUTE_PATH
        || path
            .strip_prefix(DOCUMENTS_PREFIX)
            .and_then(|rest| rest.strip_suffix(DISTRIBUTE_SUFFIX))
            .is_some_and(|id| !id.is_empty() && !id.contains('/')))
}

/// 外への送信に要るもの (worker は `worker::Fetch`・isolate の token の cache・vars と Secrets Store、テストはモック)
#[derive(Clone)]
pub struct Outbound {
    pub http: Arc<dyn Http>,
    pub line_endpoints: LineEndpoints,
    pub line_tokens: Arc<LineTokenCache>,
    /// `NOTIFY_FRONTEND_URL` (viewer の Worker の公開 URL)
    pub frontend_url: Option<Arc<str>>,
    /// `INTERNAL_SHARED_SECRET` (register-view の認証)。worker は配信の口でだけ引く
    pub viewer_secret: Option<Arc<str>>,
    /// 今の UNIX 秒
    pub now: Arc<dyn Fn() -> u64 + Send + Sync>,
}

#[derive(Debug, Deserialize, Default)]
pub struct DistributeTarget {
    /// rust と同じく受けるだけ (group_id も recipient_ids も無ければ全 enabled)
    #[serde(default)]
    pub all: bool,
    pub group_id: Option<Uuid>,
    #[serde(default)]
    pub recipient_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize, Default)]
pub struct DistributeRequest {
    pub target: Option<DistributeTarget>,
    /// 配信から何日後に閲覧期限切れにするか (既定 7 日、1..90 に丸める)
    pub retention_days: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct TestDistributeRequest {
    pub message: String,
    pub recipient_ids: Vec<Uuid>,
}

/// 1 宛先の送信の失敗。[`Self::reason`] が DB (`error_message`) とログに残す固定の語
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    NoLineUserId,
    NoLineworksUserId,
    UnknownProvider,
    LineConfigDb,
    LineConfigNotFound,
    EncryptionKey,
    LineKeyIdMissing,
    LinePrivateKeyMissing,
    LinePrivateKeyDecrypt,
    Line(LineError),
    LineworksUnreachable,
    LineworksStatus,
}

impl Failure {
    pub fn reason(self) -> &'static str {
        match self {
            Self::NoLineUserId => "No line_user_id",
            Self::NoLineworksUserId => "No lineworks_user_id",
            Self::UnknownProvider => "Unknown provider",
            Self::LineConfigDb => "LINE config: db error",
            Self::LineConfigNotFound => "LINE config not found",
            Self::EncryptionKey => "Encryption key not set",
            Self::LineKeyIdMissing => "LINE config missing key_id",
            Self::LinePrivateKeyMissing => "LINE config missing private_key",
            Self::LinePrivateKeyDecrypt => "LINE config: decrypt private_key failed",
            Self::Line(LineError::Jwt) => "LINE: JWT error",
            Self::Line(LineError::Transport) => "LINE: unreachable",
            Self::Line(LineError::TokenStatus(_) | LineError::TokenParse) => {
                "LINE: token issue failed"
            }
            Self::Line(LineError::SendStatus(_)) => "LINE: send failed",
            Self::LineworksUnreachable => "LINE WORKS: unreachable",
            Self::LineworksStatus => "LINE WORKS: send failed",
        }
    }
}

/// LINE の鍵を読んで復号する (rust の `resolve_line_config`。宛先ごとに呼ぶのも rust と同じ)
async fn resolve_line_config(state: &NotifyState, tenant_id: Uuid) -> Result<LineConfig, Failure> {
    let secrets = {
        let mut pg = state.pg.lock().await;
        pg::get_line_secrets(&mut pg, tenant_id).await
    }
    .map_err(|e| {
        state.db_fail("get line config", &e);
        Failure::LineConfigDb
    })?
    .ok_or(Failure::LineConfigNotFound)?;
    let key = state.sso_key.as_deref().ok_or(Failure::EncryptionKey)?;
    let key_id = secrets.key_id.ok_or(Failure::LineKeyIdMissing)?;
    let encrypted = secrets
        .private_key_encrypted
        .ok_or(Failure::LinePrivateKeyMissing)?;
    let private_key =
        decrypt_pem_secret(&encrypted, key).map_err(|_| Failure::LinePrivateKeyDecrypt)?;
    Ok(LineConfig {
        channel_id: secrets.channel_id,
        key_id,
        private_key,
    })
}

/// lineworks-worker の `/messages` で個人へ 1 通 (tenant の既定の bot。`bot_config_id` を渡さない)
async fn lineworks_send(
    state: &NotifyState,
    tenant_id: Uuid,
    user_id: &str,
    content: Value,
) -> Result<(), Failure> {
    let body = json!({"to": {"user_id": user_id}, "content": content});
    let resp = state
        .lineworks
        .call(LwRequest {
            post: true,
            path: LW_MESSAGES_PATH.to_owned(),
            tenant_id,
            body: Some(body.to_string()),
        })
        .await
        .map_err(|LwUnreachable(_)| Failure::LineworksUnreachable)?;
    if (200..300).contains(&resp.status) {
        Ok(())
    } else {
        Err(Failure::LineworksStatus)
    }
}

/// 1 宛先へ text を送り、`image_url` が在ればその後に画像も送る (画像の失敗はログだけで成功扱い。rust と同じ)
async fn send_to_recipient(
    state: &NotifyState,
    tenant_id: Uuid,
    recipient: &NotifyRecipient,
    message: &str,
    image_url: Option<&str>,
) -> Result<(), Failure> {
    match recipient.provider.as_str() {
        "line" => {
            let user_id = recipient
                .line_user_id
                .as_deref()
                .ok_or(Failure::NoLineUserId)?;
            let config = resolve_line_config(state, tenant_id).await?;
            let out = &state.outbound;
            let client = LineClient {
                http: out.http.as_ref(),
                endpoints: &out.line_endpoints,
                cache: &out.line_tokens,
                now: (out.now)(),
            };
            client
                .push_text(&config, user_id, message)
                .await
                .map_err(Failure::Line)?;
            if let Some(url) = image_url {
                if client.push_image(&config, user_id, url, url).await.is_err() {
                    (state.log)("notify: deliver image failed (line)");
                }
            }
            Ok(())
        }
        "lineworks" => {
            let user_id = recipient
                .lineworks_user_id
                .as_deref()
                .ok_or(Failure::NoLineworksUserId)?;
            lineworks_send(
                state,
                tenant_id,
                user_id,
                json!({"type": "text", "text": message}),
            )
            .await?;
            if let Some(url) = image_url {
                let image = json!({"type": "image", "original_url": url, "preview_url": url});
                if lineworks_send(state, tenant_id, user_id, image)
                    .await
                    .is_err()
                {
                    (state.log)("notify: deliver image failed (lineworks)");
                }
            }
            Ok(())
        }
        _ => Err(Failure::UnknownProvider),
    }
}

/// 宛先: group_id (そのグループの enabled なメンバー) > recipient_ids (enabled のうち指定のもの) > 全 enabled
async fn resolve_target_recipients(
    state: &NotifyState,
    tenant_id: Uuid,
    target: &DistributeTarget,
) -> Result<Vec<NotifyRecipient>, StatusCode> {
    let mut pg = state.pg.lock().await;
    if let Some(group_id) = target.group_id {
        return pg::list_enabled_members(&mut pg, tenant_id, group_id)
            .await
            .map_err(|e| state.db_fail("list group members", &e));
    }
    let all = pg::list_enabled_recipients(&mut pg, tenant_id)
        .await
        .map_err(|e| state.db_fail("list recipients", &e))?;
    if target.recipient_ids.is_empty() {
        return Ok(all);
    }
    let want: HashSet<Uuid> = target.recipient_ids.iter().copied().collect();
    Ok(all.into_iter().filter(|r| want.contains(&r.id)).collect())
}

/// viewer のページ (rust の `read_tracker::build_view_url`)
pub fn build_view_url(frontend_url: &str, token: Uuid) -> String {
    format!("{}/v/{token}", frontend_url.trim_end_matches('/'))
}

/// 画像メッセージの URL。**末尾の `.jpg` は必須** (LINE・LINE WORKS は image の URL を拡張子で画像と判定する。
/// viewer の Worker の image の口は黒塗り済みの jpg だけを返す)
pub fn build_image_url(frontend_url: &str, token: Uuid) -> String {
    format!(
        "{}/api/notify/v/{token}/image.jpg",
        frontend_url.trim_end_matches('/')
    )
}

fn sent_body(sent: u32, failed: u32) -> Json<Value> {
    Json(json!({"sent": sent, "failed": failed, "total": sent + failed}))
}

async fn distribute(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(document_id): Path<Uuid>,
    body: Option<Json<DistributeRequest>>,
) -> Result<Json<Value>, StatusCode> {
    let tenant_id = tenant.0;
    let triggered_by = auth_user.map(|Extension(u)| u.user_id);

    let doc = {
        let mut pg = state.pg.lock().await;
        pg::get_document(&mut pg, tenant_id, document_id).await
    }
    .map_err(|e| state.db_fail("get document", &e))?
    .ok_or(StatusCode::NOT_FOUND)?;

    // 黒塗りが済んだ (`completed`) か PDF 以外で飛ばした (`skipped`) ものだけ (金額入りの原本を送らない)
    if !matches!(doc.redaction_status.as_str(), "completed" | "skipped") {
        (state.log)("notify: distribute refused: redaction not done");
        return Err(StatusCode::BAD_REQUEST);
    }

    let req = body.map(|Json(b)| b).unwrap_or_default();
    let target = req.target.unwrap_or(DistributeTarget {
        all: true,
        ..Default::default()
    });
    let retention_days = req
        .retention_days
        .unwrap_or(DEFAULT_RETENTION_DAYS)
        .clamp(1, 90);
    let recipients = resolve_target_recipients(&state, tenant_id, &target).await?;
    if recipients.is_empty() {
        return Ok(Json(json!({"message": "No enabled recipients"})));
    }

    let deliveries = {
        let mut pg = state.pg.lock().await;
        // 失敗しても止めない (rust と同じ)
        let r =
            pg::update_distribution_status(&mut pg, tenant_id, document_id, "in_progress").await;
        state.best_effort("set distribution in_progress", r);
        let pairs = recipients
            .iter()
            .map(|r| (r.id, r.provider.clone()))
            .collect();
        let deliveries = pg::create_deliveries(&mut pg, tenant_id, document_id, pairs)
            .await
            .map_err(|e| state.db_fail("create deliveries", &e))?;
        // 配信した人と既定以外の閲覧期限 (best-effort)
        let ids = deliveries.iter().map(|d| d.id).collect();
        let expire_days =
            (retention_days != DEFAULT_RETENTION_DAYS).then_some(retention_days as i32);
        let r = pg::adjust_deliveries(&mut pg, tenant_id, ids, triggered_by, expire_days).await;
        state.best_effort("adjust deliveries", r);
        deliveries
    };

    let out = state.outbound.clone();
    let frontend = out.frontend_url.as_deref().unwrap_or(DEFAULT_FRONTEND_URL);
    // viewer の KV への登録は NOTIFY_FRONTEND_URL と INTERNAL_SHARED_SECRET の両方が在るときだけ (rust の `from_env` と同じ)
    let viewer = out
        .frontend_url
        .as_deref()
        .zip(out.viewer_secret.as_deref())
        .map(|(f, secret)| (register_endpoint(f), secret));
    // viewer が配る r2_key は黒塗り済みを優先 (rust の viewer の COALESCE(redacted_r2_key, r2_key) と同じ)
    let view_r2_key = doc.redacted_r2_key.as_deref().unwrap_or(&doc.r2_key);
    let view_expire = DateTime::<Utc>::from_timestamp((out.now)() as i64, 0).unwrap_or_default()
        + chrono::Duration::days(retention_days);
    let send_image = should_send_image(&doc);

    let mut sent = 0;
    let mut failed = 0;
    for (delivery, recipient) in deliveries.iter().zip(recipients.iter()) {
        if let Some((endpoint, secret)) = &viewer {
            let body = build_register_body(
                delivery.read_token,
                tenant_id,
                view_r2_key,
                document_id,
                delivery.recipient_id,
                doc.file_name.as_deref(),
                doc.file_size_bytes,
                doc.source_subject.as_deref(),
                doc.source_sender.as_deref(),
                Some(doc.created_at),
                view_expire,
            );
            if register(out.http.as_ref(), endpoint, secret, &body)
                .await
                .is_err()
            {
                (state.log)("notify: register view failed");
            }
        }

        // 画像を併送するときは本文の「▶ 詳細」行を省く (画像が inline で見えるので冗長)
        let view_url = build_view_url(frontend, delivery.read_token);
        let message = build_distribute_message(&doc, (!send_image).then_some(view_url.as_str()));
        let image_url = send_image.then(|| build_image_url(frontend, delivery.read_token));

        let result =
            send_to_recipient(&state, tenant_id, recipient, &message, image_url.as_deref()).await;
        let mut pg = state.pg.lock().await;
        match result {
            Ok(()) => {
                let r = pg::mark_sent(&mut pg, tenant_id, delivery.id).await;
                state.best_effort("mark delivery sent", r);
                sent += 1;
            }
            Err(f) => {
                (state.log)(&format!("notify: deliver failed ({})", f.reason()));
                let r = pg::mark_failed(&mut pg, tenant_id, delivery.id, f.reason()).await;
                state.best_effort("fail delivery", r);
                failed += 1;
            }
        }
    }

    let r = {
        let mut pg = state.pg.lock().await;
        pg::update_distribution_status(&mut pg, tenant_id, document_id, "completed").await
    };
    state.best_effort("set distribution completed", r);
    Ok(sent_body(sent, failed))
}

/// 指定の宛先 (enabled のもの) へ text だけを送る。DB には何も残さない (rust と同じ)
async fn test_distribute(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Json(input): Json<TestDistributeRequest>,
) -> Result<Json<Value>, StatusCode> {
    let tenant_id = tenant.0;
    if input.recipient_ids.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let enabled = {
        let mut pg = state.pg.lock().await;
        pg::list_enabled_recipients(&mut pg, tenant_id).await
    }
    .map_err(|e| state.db_fail("list recipients", &e))?;
    let mut sent = 0;
    let mut failed = 0;
    for recipient in enabled
        .iter()
        .filter(|r| input.recipient_ids.contains(&r.id))
    {
        match send_to_recipient(&state, tenant_id, recipient, &input.message, None).await {
            Ok(()) => sent += 1,
            Err(f) => {
                (state.log)(&format!("notify: test deliver failed ({})", f.reason()));
                failed += 1;
            }
        }
    }
    Ok(sent_body(sent, failed))
}

/// LINE / LINE WORKS に送る本文 (rust の `build_distribute_message` と同じ)。
///
/// `extracted_data.logistics` (object) が在れば物流のテンプレ、無ければ既存のテンプレ (`title` + `summary` + URL)。
///
/// ```text
/// 📄 {title}
/// 📍 積地: {loading_place}
///    {loading_place_address}
///    ☎ {loading_place_phone}
/// 📦 卸地: {unloading_place}
///    {unloading_place_address}
///    ☎ {unloading_place_phone}
/// 🕐 積込: {loading_at}
/// 🕓 卸し: {unloading_at}
/// ⚠️ 注意: {notes}
/// 🏢 連絡先: {contact_company}
/// 👤 担当: {contact_person}
/// 📞 電話: {contact_phone}
///
/// ▶ 詳細: {url}
/// ```
/// 在る項目だけを並べる。場所の住所・担当・電話は全角スペース 1 個 + 空白 1 個で字下げする。
/// `read_url` が `None` (画像を併送する) なら「▶ 詳細」行を省く。
pub fn build_distribute_message(doc: &NotifyDocument, read_url: Option<&str>) -> String {
    let title = doc
        .extracted_title
        .as_deref()
        .unwrap_or(doc.file_name.as_deref().unwrap_or("ドキュメント"));

    let Some(logistics) = doc
        .extracted_data
        .as_ref()
        .and_then(|d| d.get("logistics"))
        .filter(|v| v.is_object())
    else {
        return fallback_template(doc, read_url);
    };

    let mut lines: Vec<String> = Vec::with_capacity(15);
    lines.push(format!("📄 {title}"));

    let get_str = |key: &str| -> Option<&str> {
        logistics
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
    };
    let push_field = |lines: &mut Vec<String>, prefix: &str, key: &str| {
        if let Some(v) = get_str(key) {
            lines.push(format!("{prefix} {v}"));
        }
    };
    // 場所 (住所・担当・電話は place の下に字下げ。place が空でも住所などが在れば捨てずに並べる)
    let push_place = |lines: &mut Vec<String>, prefix: &str, keys: [&str; 4]| {
        let [place, address, phone, person] = keys;
        match get_str(place) {
            Some(v) => {
                lines.push(format!("{prefix} {v}"));
                if let Some(addr) = get_str(address) {
                    lines.push(format!("　 {addr}"));
                }
            }
            None => {
                if let Some(addr) = get_str(address) {
                    lines.push(format!("{prefix} {addr}"));
                }
            }
        }
        if let Some(p) = get_str(person) {
            lines.push(format!("　 👤 {p}"));
        }
        if let Some(p) = get_str(phone) {
            lines.push(format!("　 ☎ {p}"));
        }
    };

    push_place(
        &mut lines,
        "📍 積地:",
        [
            "loading_place",
            "loading_place_address",
            "loading_place_phone",
            "loading_place_person",
        ],
    );
    push_place(
        &mut lines,
        "📦 卸地:",
        [
            "unloading_place",
            "unloading_place_address",
            "unloading_place_phone",
            "unloading_place_person",
        ],
    );
    push_field(&mut lines, "🕐 積込:", "loading_at");
    push_field(&mut lines, "🕓 卸し:", "unloading_at");
    push_field(&mut lines, "⚠️ 注意:", "notes");
    // 相手先の連絡先 (自社は取り込みの側で除いてある)
    push_field(&mut lines, "🏢 連絡先:", "contact_company");
    push_field(&mut lines, "👤 担当:", "contact_person");
    push_field(&mut lines, "📞 電話:", "contact_phone");

    // logistics が object でも中身が全部空なら、「📄 タイトル」だけにならないよう既存のテンプレへ
    if lines.len() == 1 {
        return fallback_template(doc, read_url);
    }
    if let Some(url) = read_url {
        lines.push(String::new());
        lines.push(format!("▶ 詳細: {url}"));
    }
    lines.join("\n")
}

/// 画像メッセージを送るか (rust と同じ純関数): PDF で、かつ `extracted_data.logistics` が object のときだけ
pub fn should_send_image(doc: &NotifyDocument) -> bool {
    let is_pdf = doc
        .file_name
        .as_deref()
        .is_some_and(|n| n.to_lowercase().ends_with(".pdf"));
    is_pdf
        && doc
            .extracted_data
            .as_ref()
            .and_then(|d| d.get("logistics"))
            .is_some_and(|v| v.is_object())
}

fn fallback_template(doc: &NotifyDocument, read_url: Option<&str>) -> String {
    let summary = doc
        .extracted_summary
        .as_deref()
        .unwrap_or("新しいドキュメントが届きました");
    let title = doc
        .extracted_title
        .as_deref()
        .unwrap_or(doc.file_name.as_deref().unwrap_or("ドキュメント"));
    match read_url {
        Some(url) => format!("📄 {title}\n\n{summary}\n\n▶ 詳細を見る: {url}"),
        None => format!("📄 {title}\n\n{summary}"),
    }
}

/// rust の `distribute.rs` の単体テストの移植 (本文の組み立て) と、純関数の表
#[cfg(test)]
mod tests {
    use super::*;

    fn build_doc() -> NotifyDocument {
        NotifyDocument {
            id: Uuid::nil(),
            r2_key: "k".into(),
            file_name: Some("haisou.pdf".into()),
            file_size_bytes: None,
            source_subject: None,
            source_sender: None,
            extracted_title: None,
            extracted_summary: None,
            extracted_data: None,
            redaction_status: "completed".into(),
            redacted_r2_key: None,
            created_at: DateTime::<Utc>::default(),
        }
    }

    #[test]
    fn message_uses_logistics_template_when_all_fields_present() {
        let mut doc = build_doc();
        doc.extracted_data = Some(json!({
            "logistics": {
                "loading_place": "東京都港区",
                "loading_place_address": null,
                "loading_place_phone": null,
                "loading_place_person": null,
                "unloading_place": "大阪府大阪市",
                "unloading_place_address": null,
                "unloading_place_phone": null,
                "unloading_place_person": null,
                "loading_at": "5/9 10:00",
                "unloading_at": "5/10 14:00",
                "notes": "冷凍便\n要時間厳守",
                "contact_company": "ABC運送株式会社",
                "contact_person": "田中太郎",
                "contact_phone": "03-1234-5678"
            }
        }));
        let msg = build_distribute_message(&doc, Some("https://x/v/abc"));
        assert_eq!(
            msg,
            "📄 haisou.pdf\n📍 積地: 東京都港区\n📦 卸地: 大阪府大阪市\n🕐 積込: 5/9 10:00\n🕓 卸し: 5/10 14:00\n\
             ⚠️ 注意: 冷凍便\n要時間厳守\n🏢 連絡先: ABC運送株式会社\n👤 担当: 田中太郎\n📞 電話: 03-1234-5678\n\n\
             ▶ 詳細: https://x/v/abc"
        );
        assert!(!msg.contains("新しいドキュメント"));
    }

    #[test]
    fn message_indents_place_address_phone_and_person_under_place() {
        let mut doc = build_doc();
        doc.extracted_data = Some(json!({
            "logistics": {
                "loading_place": "イオン関西RDC",
                "loading_place_address": "京都府乙訓郡大山崎町字大山崎小字鏡田38",
                "loading_place_phone": "075-959-5008",
                "loading_place_person": "佐藤",
                "unloading_place": "熊本県八代市新港町3-9-8",
                "loading_at": "令和8年4月17日 (金) 19時"
            }
        }));
        let msg = build_distribute_message(&doc, Some("https://x/v/abc"));
        let i_place = msg.find("📍 積地: イオン関西RDC").unwrap();
        let i_addr = msg.find("　 京都府乙訓郡").unwrap();
        let i_person = msg.find("　 👤 佐藤").unwrap();
        let i_phone = msg.find("　 ☎ 075-959-5008").unwrap();
        let i_unload = msg.find("📦 卸地: 熊本県八代市").unwrap();
        let i_loadat = msg.find("🕐 積込:").unwrap();
        assert!(i_place < i_addr);
        assert!(i_addr < i_person);
        assert!(i_person < i_phone);
        assert!(i_phone < i_unload);
        assert!(i_unload < i_loadat);
        assert_eq!(msg.matches('☎').count(), 1);
        assert_eq!(msg.matches("👤 ").count(), 1);
    }

    /// place が空でも住所・担当・電話が在れば、住所を place の位置に置いて捨てずに並べる
    #[test]
    fn message_keeps_address_person_phone_without_place() {
        let mut doc = build_doc();
        doc.extracted_data = Some(json!({
            "logistics": {
                "unloading_place": " ",
                "unloading_place_address": "札幌市",
                "unloading_place_person": "鈴木",
                "unloading_place_phone": "011-000"
            }
        }));
        let msg = build_distribute_message(&doc, None);
        assert_eq!(
            msg,
            "📄 haisou.pdf\n📦 卸地: 札幌市\n　 👤 鈴木\n　 ☎ 011-000"
        );
    }

    #[test]
    fn message_omits_null_contact_fields() {
        let mut doc = build_doc();
        doc.extracted_data = Some(json!({"logistics": {"contact_phone": "06-9876-5432"}}));
        let msg = build_distribute_message(&doc, Some("https://x/v/abc"));
        assert!(msg.contains("📞 電話: 06-9876-5432"));
        assert!(!msg.contains("🏢 連絡先"));
        assert!(!msg.contains("👤 担当"));
        assert!(!msg.contains("📍 積地"));
        assert!(msg.contains("▶ 詳細: https://x/v/abc"));
    }

    #[test]
    fn message_includes_contact_in_correct_order_after_logistics() {
        let mut doc = build_doc();
        doc.extracted_data = Some(json!({
            "logistics": {
                "loading_place": "東京",
                "notes": "冷凍",
                "contact_company": "ABC",
                "contact_phone": "03-1234"
            }
        }));
        let msg = build_distribute_message(&doc, Some("https://x/v/abc"));
        let i_notes = msg.find("⚠️ 注意").unwrap();
        let i_contact = msg.find("🏢 連絡先").unwrap();
        let i_phone = msg.find("📞 電話").unwrap();
        let i_url = msg.find("▶ 詳細").unwrap();
        assert!(i_notes < i_contact);
        assert!(i_contact < i_phone);
        assert!(i_phone < i_url);
    }

    #[test]
    fn message_without_url_when_image_is_sent() {
        let mut doc = build_doc();
        doc.extracted_data = Some(json!({"logistics": {"loading_place": "成田"}}));
        assert_eq!(
            build_distribute_message(&doc, None),
            "📄 haisou.pdf\n📍 積地: 成田"
        );
    }

    #[test]
    fn message_falls_back_to_legacy_when_no_logistics_key() {
        let mut doc = build_doc();
        doc.extracted_title = Some("見積書".into());
        doc.extracted_summary = Some("〇〇社からの見積です".into());
        doc.extracted_data = Some(json!({"phone_numbers_ext": ["090-..."]}));
        assert_eq!(
            build_distribute_message(&doc, Some("https://x/v/abc")),
            "📄 見積書\n\n〇〇社からの見積です\n\n▶ 詳細を見る: https://x/v/abc"
        );
        assert_eq!(
            build_distribute_message(&doc, None),
            "📄 見積書\n\n〇〇社からの見積です"
        );
    }

    #[test]
    fn message_falls_back_when_extracted_data_is_none_or_empty_or_not_object() {
        let mut doc = build_doc();
        let msg = build_distribute_message(&doc, Some("https://x/v/abc"));
        assert!(msg.contains("📄 haisou.pdf"));
        assert!(msg.contains("新しいドキュメントが届きました"));
        assert!(msg.contains("▶ 詳細を見る: https://x/v/abc"));
        for data in [
            json!({"logistics": {"loading_place": "  ", "unloading_place": "", "notes": null}}),
            json!({"logistics": "broken"}),
        ] {
            doc.extracted_data = Some(data);
            let msg = build_distribute_message(&doc, Some("https://x/v/abc"));
            assert!(!msg.contains("📍 積地"));
            assert!(msg.contains("新しいドキュメントが届きました"));
        }
    }

    #[test]
    fn message_title_prefers_extracted_title_then_file_name_then_default() {
        let mut doc = build_doc();
        doc.extracted_title = Some("配車手配票".into());
        doc.extracted_data = Some(json!({"logistics": {"loading_place": "東京"}}));
        let msg = build_distribute_message(&doc, Some("https://x/v/abc"));
        assert!(msg.starts_with("📄 配車手配票"));
        assert!(!msg.contains("haisou.pdf"));
        let mut doc = build_doc();
        doc.file_name = None;
        assert!(build_distribute_message(&doc, None).starts_with("📄 ドキュメント"));
    }

    #[test]
    fn image_only_for_pdf_with_logistics_object() {
        let mut doc = build_doc();
        assert!(!should_send_image(&doc), "extracted_data が無い");
        doc.extracted_data = Some(json!({"logistics": {"loading_place": "x"}}));
        assert!(should_send_image(&doc));
        doc.file_name = Some("HAISOU.PDF".into());
        assert!(should_send_image(&doc));
        doc.file_name = Some("a.png".into());
        assert!(!should_send_image(&doc));
        doc.file_name = None;
        assert!(!should_send_image(&doc));
        doc.file_name = Some("a.pdf".into());
        doc.extracted_data = Some(json!({"logistics": "broken"}));
        assert!(!should_send_image(&doc));
    }

    #[test]
    fn view_and_image_urls() {
        let t = Uuid::from_u128(7);
        assert_eq!(
            build_view_url("https://v.test/", t),
            format!("https://v.test/v/{t}")
        );
        // image は `.jpg` で終わる (LINE・LINE WORKS が拡張子で画像と判定する)
        assert_eq!(
            build_image_url("https://v.test/", t),
            format!("https://v.test/api/notify/v/{t}/image.jpg")
        );
    }

    #[test]
    fn distribute_request_paths() {
        let id = Uuid::from_u128(1);
        assert!(is_distribute_request(true, TEST_DISTRIBUTE_PATH));
        assert!(is_distribute_request(
            true,
            &format!("/api/notify/documents/{id}/distribute")
        ));
        assert!(!is_distribute_request(
            false,
            &format!("/api/notify/documents/{id}/distribute")
        ));
        for path in [
            "/api/notify/documents//distribute",
            "/api/notify/documents/a/b/distribute",
            "/api/notify/documents/x",
            "/api/notify/line-config",
        ] {
            assert!(!is_distribute_request(true, path), "{path}");
        }
    }

    /// DB (`error_message`) とログに残す語は固定 (上流の本文・宛先を含まない)
    #[test]
    fn failure_reasons_are_fixed_words() {
        let table = [
            (Failure::NoLineUserId, "No line_user_id"),
            (Failure::NoLineworksUserId, "No lineworks_user_id"),
            (Failure::UnknownProvider, "Unknown provider"),
            (Failure::LineConfigDb, "LINE config: db error"),
            (Failure::LineConfigNotFound, "LINE config not found"),
            (Failure::EncryptionKey, "Encryption key not set"),
            (Failure::LineKeyIdMissing, "LINE config missing key_id"),
            (
                Failure::LinePrivateKeyMissing,
                "LINE config missing private_key",
            ),
            (
                Failure::LinePrivateKeyDecrypt,
                "LINE config: decrypt private_key failed",
            ),
            (Failure::Line(LineError::Jwt), "LINE: JWT error"),
            (Failure::Line(LineError::Transport), "LINE: unreachable"),
            (
                Failure::Line(LineError::TokenStatus(400)),
                "LINE: token issue failed",
            ),
            (
                Failure::Line(LineError::TokenParse),
                "LINE: token issue failed",
            ),
            (
                Failure::Line(LineError::SendStatus(400)),
                "LINE: send failed",
            ),
            (Failure::LineworksUnreachable, "LINE WORKS: unreachable"),
            (Failure::LineworksStatus, "LINE WORKS: send failed"),
        ];
        for (f, want) in table {
            assert_eq!(f.reason(), want);
        }
    }
}
