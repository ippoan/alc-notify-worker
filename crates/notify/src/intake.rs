//! 文書の upload と、メールの取り込み (W8c-2 PR-3b。rust-alc-api の `crates/alc-notify/src/documents.rs` の upload と
//! `ingest.rs` の写し。Refs ippoan/rust-alc-api#680)。
//!
//! - `POST /api/notify/documents/upload` (multipart。tenant の layer の中) — pdf・docx・xlsx・png・jpg・jpeg のファイルの部分を
//!   R2 `{tenant}/manual/{batch}/{名前}` に置いて行を作る。201 `{document_ids, count}`。拡張子が違う・ファイルが 0 件・
//!   multipart が読めない 400、20 件超・25 MiB 超 413
//! - `POST /api/notify/ingest` (JSON。path は rust と同じ。**tenant の layer の外**) —
//!   nuxt-notify の email-receiver が Service Binding で送るメールの添付 (base64) を R2 `{tenant}/email/{email_message_id}/{名前}`
//!   に置いて行を作る。201 `{email_message_id, document_ids, count}`。tenant は `tenant_short_id` → `tenants.short_id`
//!   (空 400・無い 404)。添付 0 件 400、21 件以上 413、base64 が読めない 400、25 MiB 超 413
//!
//! どちらも本文の上限は rust と同じ 20 MiB ([`BODY_LIMIT`]。rust は app 全体の `DefaultBodyLimit`)。R2 の binding が無ければ 500。
//! 行を作ったら、PDF は黒塗り、全部の文書で抽出を queue へ積む ([`crate::jobs`]。抽出は PDF 以外を中で完了扱いにする)。
//!
//! **ingest の信頼は Service Binding だけ** (共有の秘密 `X-Worker-Secret` は廃止。オーナー決定)。この worker は workers.dev も
//! route も持たず (scripts/check-exposure.sh)、**auth-worker の振り分け表 (`ALC_BINDING_ROUTES`) に ingest を載せない**
//! (載せると外からどの tenant にも書ける。auth-worker のテストが固定する)。届くのは email-receiver の binding `NOTIFY` だけ。
//!
//! rust との違い:
//! - 行は 1 つの transaction でまとめて作る (rust は 1 件ずつで、途中で落ちると一部だけ残る)
//! - upload の batch と email_message_id は DB の `gen_random_uuid()` で作る (wasm で乱数の crate を持たない)
//! - queue に積めなくても 201 (行は `pending` のまま残り、画面のやり直しで積み直せる。ログは固定の語だけ)
//!
//! ログは固定の語だけ (ファイル名・キー・tenant・差出人を出さない)。

use alc_core_wasm::TenantId;
use axum::extract::{DefaultBodyLimit, Multipart, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::jobs::{Job, JobKind};
use crate::pg::{self, EmailSource, StoredFile};
use crate::store::SharedStore;
use crate::NotifyState;

pub const UPLOAD_PATH: &str = "/api/notify/documents/upload";
pub const INGEST_PATH: &str = "/api/notify/ingest";

/// 1 回に置けるファイルの数・合計の大きさ (rust と同じ)
pub const MAX_FILES: usize = 20;
pub const MAX_TOTAL_BYTES: usize = 25 * 1024 * 1024;
/// 本文の上限 (rust の monolith の `DefaultBodyLimit` と同じ 20 MiB)
pub const BODY_LIMIT: usize = 20 * 1024 * 1024;
const ALLOWED_EXTENSIONS: &[&str] = &["pdf", "docx", "xlsx", "png", "jpg", "jpeg"];

/// upload (tenant の layer の中に merge する)
pub fn upload_router() -> Router<NotifyState> {
    Router::new()
        .route(UPLOAD_PATH, post(upload))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
}

/// ingest (tenant の layer の外に merge する)
pub fn ingest_router() -> Router<NotifyState> {
    Router::new()
        .route(INGEST_PATH, post(ingest))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
}

/// 危険なパス文字を除く (rust の `ingest.rs` の `sanitize_filename` と同じ)
pub fn sanitize_filename(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return "attachment".to_string();
    }
    let cleaned: String = trimmed
        .chars()
        .map(|c| {
            if c == '/' || c == '\\' || c.is_control() {
                '_'
            } else {
                c
            }
        })
        .collect();
    cleaned.replace("..", "_")
}

pub fn is_allowed_extension(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    ALLOWED_EXTENSIONS
        .iter()
        .any(|ext| lower.ends_with(&format!(".{ext}")))
}

/// 合計に足す。[`MAX_TOTAL_BYTES`] を超えたら 413 (本文の上限 20 MiB の下では届かないが、rust と同じ検査を残す)
fn add_total(total: usize, len: usize) -> Result<usize, StatusCode> {
    let total = total.saturating_add(len);
    if total > MAX_TOTAL_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    Ok(total)
}

fn is_pdf(name: &str) -> bool {
    name.to_lowercase().ends_with(".pdf")
}

/// 受けたファイル (名前・Content-Type・中身)
struct Incoming {
    name: String,
    content_type: String,
    bytes: Vec<u8>,
}

fn storage(state: &NotifyState, what: &str) -> Result<SharedStore, StatusCode> {
    state.files.clone().ok_or_else(|| {
        (state.log)(&format!("notify: {what} failed: storage"));
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

async fn new_id(state: &NotifyState, what: &str, tenant_id: Uuid) -> Result<Uuid, StatusCode> {
    let id = {
        let mut pg = state.pg.lock().await;
        pg::new_uuid(&mut pg, tenant_id).await
    };
    id.map_err(|e| state.db_fail(what, &e))
}

/// R2 に置き (`{tenant}/{dir}/{id}/{名前}`)、行を作り、queue へ積む。作った文書の id を返す
async fn store(
    state: &NotifyState,
    what: &str,
    files: SharedStore,
    tenant_id: Uuid,
    prefix: String,
    incoming: Vec<Incoming>,
    email: Option<EmailSource>,
) -> Result<Vec<Uuid>, StatusCode> {
    let mut stored = Vec::with_capacity(incoming.len());
    for f in incoming {
        let r2_key = format!("{prefix}/{}", sanitize_filename(&f.name));
        let size = f.bytes.len() as i64;
        if files.put(&r2_key, f.bytes, &f.content_type).await.is_err() {
            (state.log)(&format!("notify: {what} failed: r2 put"));
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        stored.push(StoredFile {
            r2_key,
            file_name: f.name,
            size,
        });
    }
    let pdfs: Vec<bool> = stored.iter().map(|f| is_pdf(&f.file_name)).collect();
    let ids = {
        let mut pg = state.pg.lock().await;
        pg::insert_documents(&mut pg, tenant_id, stored, email).await
    };
    let ids = ids.map_err(|e| state.db_fail(what, &e))?;
    for (id, pdf) in ids.iter().zip(pdfs) {
        let mut jobs = vec![JobKind::Extract];
        if pdf {
            jobs.insert(0, JobKind::Redact);
        }
        for kind in jobs {
            if state
                .jobs
                .send(Job::new(kind, tenant_id, *id), 0)
                .await
                .is_err()
            {
                (state.log)(&format!("notify: {what} enqueue failed: queue"));
            }
        }
    }
    Ok(ids)
}

#[derive(Debug, Serialize)]
struct UploadResponse {
    document_ids: Vec<Uuid>,
    count: usize,
}

async fn upload(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<UploadResponse>), StatusCode> {
    let files = storage(&state, "upload")?;

    let mut incoming: Vec<Incoming> = Vec::new();
    let mut total: usize = 0;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        // ファイルの部分のほか (テキストの欄) は無視
        let Some(name) = field.file_name().map(str::to_owned) else {
            continue;
        };
        let content_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_owned();
        if !is_allowed_extension(&name) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let bytes = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
        total = add_total(total, bytes.len())?;
        incoming.push(Incoming {
            name,
            content_type,
            bytes: bytes.to_vec(),
        });
        if incoming.len() > MAX_FILES {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
    }
    if incoming.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let batch = new_id(&state, "upload", tenant_id).await?;
    let prefix = format!("{tenant_id}/manual/{batch}");
    let document_ids = store(&state, "upload", files, tenant_id, prefix, incoming, None).await?;
    let count = document_ids.len();
    Ok((
        StatusCode::CREATED,
        Json(UploadResponse {
            document_ids,
            count,
        }),
    ))
}

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    /// `tenants.short_id` (email-receiver がメールの宛先 `tenant-{short_id}@…` から取る)
    pub tenant_short_id: String,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub body_text: Option<String>,
    pub received_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Deserialize)]
pub struct Attachment {
    pub filename: String,
    pub content_type: String,
    pub content_base64: String,
}

#[derive(Debug, Serialize)]
struct IngestResponse {
    email_message_id: Uuid,
    document_ids: Vec<Uuid>,
    count: usize,
}

async fn ingest(
    State(state): State<NotifyState>,
    Json(payload): Json<IngestRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), StatusCode> {
    let short_id = payload.tenant_short_id.trim().to_owned();
    if short_id.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let tenant_id = {
        let mut pg = state.pg.lock().await;
        pg::tenant_by_short_id(&mut pg, short_id).await
    };
    let tenant_id = tenant_id
        .map_err(|e| state.db_fail("ingest tenant", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;

    if payload.attachments.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if payload.attachments.len() > MAX_FILES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let files = storage(&state, "ingest")?;

    let mut incoming = Vec::with_capacity(payload.attachments.len());
    let mut total: usize = 0;
    for a in payload.attachments {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&a.content_base64)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        total = add_total(total, bytes.len())?;
        incoming.push(Incoming {
            name: a.filename,
            content_type: a.content_type,
            bytes,
        });
    }

    let email_message_id = new_id(&state, "ingest", tenant_id).await?;
    let email = EmailSource {
        message_id: email_message_id,
        from: payload.from,
        subject: payload.subject,
        body_text: payload.body_text,
        received_at: payload.received_at.unwrap_or_else(Utc::now),
    };
    let prefix = format!("{tenant_id}/email/{email_message_id}");
    let document_ids = store(
        &state,
        "ingest",
        files,
        tenant_id,
        prefix,
        incoming,
        Some(email),
    )
    .await?;
    let count = document_ids.len();
    Ok((
        StatusCode::CREATED,
        Json(IngestResponse {
            email_message_id,
            document_ids,
            count,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_filename_strips_path_chars() {
        assert_eq!(sanitize_filename("a/b\\c"), "a_b_c");
        assert_eq!(sanitize_filename("../etc/passwd"), "__etc_passwd");
        assert_eq!(sanitize_filename("配車\t票.pdf"), "配車_票.pdf");
        assert_eq!(sanitize_filename(" normal.pdf "), "normal.pdf");
        assert_eq!(sanitize_filename(""), "attachment");
        assert_eq!(sanitize_filename("   "), "attachment");
    }

    #[test]
    fn allowed_extensions_match_rust() {
        for ok in [
            "a.pdf",
            "A.PDF",
            "foo.docx",
            "foo.bar.xlsx",
            "photo.jpeg",
            "photo.JPG",
            "img.png",
        ] {
            assert!(is_allowed_extension(ok), "{ok}");
        }
        for ng in ["a.exe", "a", "", ".pdf.exe", "pdf"] {
            assert!(!is_allowed_extension(ng), "{ng}");
        }
        assert_eq!(
            (MAX_FILES, MAX_TOTAL_BYTES, BODY_LIMIT),
            (20, 25 << 20, 20 << 20)
        );
    }

    #[test]
    fn add_total_caps_at_25_mib() {
        assert_eq!(add_total(MAX_TOTAL_BYTES - 1, 1), Ok(MAX_TOTAL_BYTES));
        assert_eq!(
            add_total(MAX_TOTAL_BYTES, 1),
            Err(StatusCode::PAYLOAD_TOO_LARGE)
        );
        assert_eq!(add_total(usize::MAX, 1), Err(StatusCode::PAYLOAD_TOO_LARGE));
    }
}
