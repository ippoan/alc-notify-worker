//! 文書とメールの閲覧の口 (W8c-1。rust-alc-api の `crates/alc-notify` の `documents.rs` の list・get・preview と、
//! `email_documents.rs` の list_emails・get_email・download の写し。Refs ippoan/rust-alc-api#680)。
//!
//! - `GET /api/notify/documents?limit=&offset=` — 新しい順 (既定 50 件・0 件目から)
//! - `GET /api/notify/documents/{id}` — `{document, deliveries}` (配信の記録に宛先の名前と送った人)。無ければ 404
//! - `GET /api/notify/documents/{id}/preview?original=` — 黒塗り済み (`redacted_r2_key`。無ければ原本) か、`original=true` なら原本
//! - `GET /api/notify/documents/{id}/download` — 原本 (`r2_key`) を添付として
//! - `GET /api/notify/emails` — メール (`email_message_id`) ごとの一覧 (新しい順に 200 件)
//! - `GET /api/notify/emails/{message_id}` — メールの見出しと添付。添付が無ければ 404
//!
//! 書きの口 (upload・DELETE・redact-recompute・extract-recompute・ingest) は Cloud Run に残る (W8c-2)。
//!
//! rust との違い (ファイルの口だけ):
//! - `Content-Disposition` は ASCII だけで組む ([`content_disposition`])
//! - preview は `X-Content-Type-Options: nosniff` を付け、inline で返すのは pdf と画像 (png・jpeg・gif・webp) だけ。
//!   それ以外 (svg・txt) は `application/octet-stream` の attachment (rust は inline。同じ origin で開いて
//!   script が動かないよう、nuxt-notify の `server/utils/notify-view.ts` の `isSafeInline` と同じ範囲に絞る)
//! - R2 の binding が無いときの status は rust と同じ (preview は 503、download は 500)

use alc_core_wasm::TenantId;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::Deserialize;
use uuid::Uuid;

use crate::pg::{self, DocumentWithDeliveries, EmailDetail, EmailSummary, NotifyDocument};
use crate::store::{content_disposition, StoreError};
use crate::NotifyState;

pub const DOCUMENTS_PATH: &str = "/api/notify/documents";
pub const DOCUMENT_PATH: &str = "/api/notify/documents/{id}";
pub const PREVIEW_PATH: &str = "/api/notify/documents/{id}/preview";
pub const DOWNLOAD_PATH: &str = "/api/notify/documents/{id}/download";
pub const EMAILS_PATH: &str = "/api/notify/emails";
pub const EMAIL_PATH: &str = "/api/notify/emails/{message_id}";

/// 名前の無い文書の `Content-Disposition` の名前 (rust と同じ)
const DEFAULT_FILE_NAME: &str = "attachment";

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(DOCUMENTS_PATH, get(list))
        .route(DOCUMENT_PATH, get(get_one))
        .route(PREVIEW_PATH, get(preview))
        .route(DOWNLOAD_PATH, get(download))
        .route(EMAILS_PATH, get(list_emails))
        .route(EMAIL_PATH, get(get_email))
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

async fn list(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<NotifyDocument>>, StatusCode> {
    let docs = {
        let mut pg = state.pg.lock().await;
        pg::list_documents(
            &mut pg,
            tenant_id,
            q.limit.unwrap_or(50),
            q.offset.unwrap_or(0),
        )
        .await
    };
    docs.map(Json)
        .map_err(|e| state.db_fail("list_documents", &e))
}

async fn get_one(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<DocumentWithDeliveries>, StatusCode> {
    let found = {
        let mut pg = state.pg.lock().await;
        pg::get_document_with_deliveries(&mut pg, tenant_id, id).await
    };
    found
        .map_err(|e| state.db_fail("get_document", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn list_emails(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<EmailSummary>>, StatusCode> {
    let emails = {
        let mut pg = state.pg.lock().await;
        pg::list_emails(&mut pg, tenant_id).await
    };
    emails
        .map(Json)
        .map_err(|e| state.db_fail("list_emails", &e))
}

async fn get_email(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(message_id): Path<Uuid>,
) -> Result<Json<EmailDetail>, StatusCode> {
    let found = {
        let mut pg = state.pg.lock().await;
        pg::get_email(&mut pg, tenant_id, message_id).await
    };
    found
        .map_err(|e| state.db_fail("get_email", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Debug, Deserialize)]
pub struct PreviewQuery {
    pub original: Option<bool>,
}

/// preview と download の違い (key の選び方は呼び手が決める)
struct Serve {
    what: &'static str,
    /// R2 の binding が無いときの status (rust: preview は 503、download は 500)
    no_store: StatusCode,
    /// `true` = 画面で開く (Content-Type は key の拡張子。pdf と画像のほかは添付)、`false` = 添付 (`application/octet-stream`)
    inline: bool,
}

async fn preview(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Query(q): Query<PreviewQuery>,
) -> Result<Response, StatusCode> {
    let original = q.original.unwrap_or(false);
    serve(
        state,
        tenant_id,
        id,
        move |doc| {
            if original {
                doc.r2_key
            } else {
                doc.redacted_r2_key.unwrap_or(doc.r2_key)
            }
        },
        Serve {
            what: "preview",
            no_store: StatusCode::SERVICE_UNAVAILABLE,
            inline: true,
        },
    )
    .await
}

async fn download(
    State(state): State<NotifyState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Response, StatusCode> {
    serve(
        state,
        tenant_id,
        id,
        |doc| doc.r2_key,
        Serve {
            what: "download",
            no_store: StatusCode::INTERNAL_SERVER_ERROR,
            inline: false,
        },
    )
    .await
}

/// 文書を引き (無ければ 404)、`pick` が選んだ key の本体を R2 から返す
async fn serve(
    state: NotifyState,
    tenant_id: Uuid,
    id: Uuid,
    pick: impl FnOnce(NotifyDocument) -> String,
    how: Serve,
) -> Result<Response, StatusCode> {
    let found = {
        let mut pg = state.pg.lock().await;
        pg::get_document(&mut pg, tenant_id, id).await
    };
    let doc = found
        .map_err(|e| state.db_fail(how.what, &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let store = state.files.clone().ok_or_else(|| {
        (state.log)(&format!("notify: {} failed: r2 (not configured)", how.what));
        how.no_store
    })?;
    let file_name = doc
        .file_name
        .clone()
        .unwrap_or_else(|| DEFAULT_FILE_NAME.to_owned());
    let key = pick(doc);
    let bytes = store
        .get(&key)
        .await
        .map_err(|e| state.store_fail(how.what, e))?;
    // Content-Type は **実際の R2 key の拡張子** で決める (黒塗り済みは .jpg のことがあり、file_name で決めると PDF.js が壊れる)。
    // inline で返すのは pdf と画像だけ。ほかと download は添付
    let (content_type, disposition) = match how
        .inline
        .then(|| guess_content_type(&key))
        .filter(|t| is_safe_inline(t))
    {
        Some(t) => (t, "inline"),
        None => (OCTET_STREAM, "attachment"),
    };
    let mut resp = Body::from(bytes).into_response();
    let headers = resp.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    // ASCII だけで組むので必ずヘッダの値になる
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::try_from(content_disposition(disposition, &file_name))
            .expect("ASCII の Content-Disposition"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    Ok(resp)
}

const OCTET_STREAM: &str = "application/octet-stream";

/// key の拡張子から Content-Type (rust の `guess_content_type` と同じ表。不明は PDF に倒す)
pub fn guess_content_type(key: &str) -> &'static str {
    let name = key.to_ascii_lowercase();
    if name.ends_with(".pdf") {
        "application/pdf"
    } else if name.ends_with(".png") {
        "image/png"
    } else if name.ends_with(".jpg") || name.ends_with(".jpeg") {
        "image/jpeg"
    } else if name.ends_with(".gif") {
        "image/gif"
    } else if name.ends_with(".webp") {
        "image/webp"
    } else if name.ends_with(".svg") {
        "image/svg+xml"
    } else if name.ends_with(".txt") {
        "text/plain; charset=utf-8"
    } else {
        "application/pdf"
    }
}

/// 同じ origin で inline に開いてよい型 (nuxt-notify の `SAFE_INLINE_TYPES` と同じ)
pub fn is_safe_inline(content_type: &str) -> bool {
    matches!(
        content_type,
        "application/pdf" | "image/png" | "image/jpeg" | "image/gif" | "image/webp"
    )
}

impl NotifyState {
    /// R2 の失敗を固定の語でログに出し、500 にする
    fn store_fail(&self, what: &str, e: StoreError) -> StatusCode {
        (self.log)(&format!("notify: {what} failed: r2 ({})", e.0));
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_type_by_extension() {
        for (key, want) in [
            ("a.pdf", "application/pdf"),
            ("A.PDF", "application/pdf"),
            ("a.png", "image/png"),
            ("a.jpg", "image/jpeg"),
            ("a.JPEG", "image/jpeg"),
            ("a.gif", "image/gif"),
            ("a.webp", "image/webp"),
            ("a.svg", "image/svg+xml"),
            ("a.txt", "text/plain; charset=utf-8"),
            ("a.xlsx", "application/pdf"),
            ("noext", "application/pdf"),
        ] {
            assert_eq!(guess_content_type(key), want, "{key}");
        }
    }

    #[test]
    fn only_pdf_and_images_are_inline() {
        for t in [
            "application/pdf",
            "image/png",
            "image/jpeg",
            "image/gif",
            "image/webp",
        ] {
            assert!(is_safe_inline(t), "{t}");
        }
        for t in ["image/svg+xml", "text/plain; charset=utf-8", OCTET_STREAM] {
            assert!(!is_safe_inline(t), "{t}");
        }
    }
}
