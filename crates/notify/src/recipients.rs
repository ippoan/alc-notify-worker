//! 宛先 (notify_recipients) の口。rust-alc-api の `crates/alc-notify/src/recipients.rs` と同じ path・method・JSON・status。
//!
//! | method | path | 成功 |
//! |---|---|---|
//! | GET | `/api/notify/recipients` | 200 名前の順の配列 |
//! | POST | `/api/notify/recipients` | 201 作った行 |
//! | POST | `/api/notify/recipients/bulk` | 200 `{created, updated, skipped, touched_recipient_ids}` |
//! | GET | `/api/notify/recipients/{id}` | 200 / 無ければ 404 |
//! | PUT | `/api/notify/recipients/{id}` | 200 直した行 (無ければ 500。rust と同じ) |
//! | DELETE | `/api/notify/recipients/{id}` | 204 (無くても 204) |
//! | GET | `/api/notify/recipients/email-status` | 200 `{configured, items: [{recipient_id, email_status}]}` (rust に無い口。[`crate::email_routing`]) |
//!
//! POST / PUT / bulk で、メールアドレスが新しく入った・変わったときは、保存の後に Cloudflare Email Routing の送り先を作る
//! (best-effort。DB の lock の外。`X-User-Role` が `admin` のときだけ。送り先は宛先を消しても・変えても消さない)。

use std::collections::HashMap;

use alc_core_wasm::api_error::{internal_error_msg, ApiError};
use alc_core_wasm::{AuthUser, TenantId};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::email_routing::normalize_email;
use crate::pg::{self, CreateNotifyRecipient, UpdateNotifyRecipient};
use crate::{groups, NotifyState};

pub const RECIPIENTS_PATH: &str = "/api/notify/recipients";
pub const RECIPIENTS_BULK_PATH: &str = "/api/notify/recipients/bulk";
pub const RECIPIENT_PATH: &str = "/api/notify/recipients/{id}";
pub const EMAIL_STATUS_PATH: &str = "/api/notify/recipients/email-status";

/// 送り先を作ってよい呼び手か (auth-worker が付ける `X-User-Role`。任意のアドレスに Cloudflare の確認メールが飛ぶため)
fn is_admin(auth_user: &Option<Extension<AuthUser>>) -> bool {
    auth_user
        .as_ref()
        .is_some_and(|Extension(u)| u.role == "admin")
}

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(RECIPIENTS_PATH, get(list).post(create))
        .route(RECIPIENTS_BULK_PATH, post(bulk_upsert))
        .route(EMAIL_STATUS_PATH, get(email_status))
        .route(RECIPIENT_PATH, get(get_one).put(update).delete(delete))
}

#[derive(Debug, Deserialize)]
pub struct BulkUpsertInput {
    pub recipients: Vec<CreateNotifyRecipient>,
    /// 作った・直した宛先を、これらのグループにも入れる
    #[serde(default)]
    pub group_ids: Vec<Uuid>,
}

/// `lineworks_user_id` で既存の行と突き合わせ、在れば直し・無ければ作る (rust の `bulk_upsert` と同じ)。
/// 行ごとに別の transaction で、失敗した行は `skipped` に `db_error` として載せて続ける。
/// グループへの追加は best-effort (失敗はログだけで、応答は変えない)
async fn bulk_upsert(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Json(input): Json<BulkUpsertInput>,
) -> Result<Json<Value>, StatusCode> {
    let admin = is_admin(&auth_user);
    let mut pg = state.pg.lock().await;
    let mut created = 0usize;
    let mut updated = 0usize;
    let mut skipped: Vec<Value> = Vec::new();
    let mut touched_ids: Vec<Uuid> = Vec::new();
    // 保存の後に送り先を作るメールアドレス (正規化済み・重複なし)
    let mut new_emails: Vec<String> = Vec::new();

    // lineworks_user_id → (id, 保存前のメールアドレス) の対応を 1 回だけ作る
    let existing = pg::list_recipients(&mut pg, tenant.0)
        .await
        .map_err(|e| state.db_fail("bulk list recipients", &e))?;
    let existing_by_lw: HashMap<String, (Uuid, Option<String>)> = existing
        .into_iter()
        .filter_map(|r| r.lineworks_user_id.map(|id| (id, (r.id, r.email))))
        .collect();

    for (idx, r) in input.recipients.into_iter().enumerate() {
        let Some(lw_id) = r.lineworks_user_id.clone() else {
            skipped.push(json!({"index": idx, "reason": "missing_lineworks_user_id"}));
            continue;
        };
        let pre_existing = existing_by_lw.get(&lw_id);
        let pre_existing_id = pre_existing.map(|(id, _)| *id);
        let result = match pre_existing_id {
            Some(id) => {
                let input = UpdateNotifyRecipient {
                    name: Some(r.name),
                    provider: Some(r.provider),
                    lineworks_user_id: Some(lw_id),
                    line_user_id: None,
                    phone_number: r.phone_number,
                    email: r.email,
                    enabled: None,
                };
                pg::update_recipient(&mut pg, tenant.0, id, input).await
            }
            None => pg::create_recipient(&mut pg, tenant.0, r).await,
        };
        match result {
            Ok(rec) => {
                if pre_existing_id.is_some() {
                    updated += 1;
                } else {
                    created += 1;
                }
                touched_ids.push(rec.id);
                let before = pre_existing.and_then(|(_, email)| email.as_deref());
                if let Some(email) = changed_email(admin, before, rec.email.as_deref()) {
                    if !new_emails.contains(&email) {
                        new_emails.push(email);
                    }
                }
            }
            Err(e) => {
                state.db_fail("bulk upsert recipient", &e);
                skipped.push(json!({"index": idx, "reason": "db_error"}));
            }
        }
    }

    for group_id in input.group_ids {
        if let Err(e) = pg::add_members(&mut pg, tenant.0, group_id, touched_ids.clone()).await {
            groups::add_members_fail(&state, "bulk add members", e);
        }
    }

    // Cloudflare は DB の lock の外で呼ぶ
    drop(pg);
    for email in &new_emails {
        state.ensure_destination(email).await;
    }

    Ok(Json(json!({
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "touched_recipient_ids": touched_ids,
    })))
}

async fn list(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let recipients = pg::list_recipients(&mut pg, tenant.0)
        .await
        .map_err(|e| state.db_fail("list recipients", &e))?;
    Ok(Json(json!(recipients)))
}

async fn get_one(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let recipient = pg::get_recipient(&mut pg, tenant.0, id)
        .await
        .map_err(|e| state.db_fail("get recipient", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(json!(recipient)))
}

async fn create(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Json(input): Json<CreateNotifyRecipient>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    let recipient = {
        let mut pg = state.pg.lock().await;
        pg::create_recipient(&mut pg, tenant.0, input).await
    }
    .map_err(|e| state.db_fail("create recipient", &e))?;
    if let Some(email) = changed_email(is_admin(&auth_user), None, recipient.email.as_deref()) {
        state.ensure_destination(&email).await;
    }
    Ok((StatusCode::CREATED, Json(json!(recipient))))
}

async fn update(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    Json(input): Json<UpdateNotifyRecipient>,
) -> Result<Json<Value>, StatusCode> {
    let admin = is_admin(&auth_user);
    // メールアドレスを送ってきたときだけ、保存前の行を読んで比べる (送らなければ COALESCE で変わらない)
    let check_email = admin && input.email.is_some();
    let recipient = {
        let mut pg = state.pg.lock().await;
        let before = if check_email {
            pg::get_recipient(&mut pg, tenant.0, id)
                .await
                .map_err(|e| state.db_fail("update recipient", &e))?
                .and_then(|r| r.email)
        } else {
            None
        };
        pg::update_recipient(&mut pg, tenant.0, id, input)
            .await
            .map(|r| (before, r))
    }
    .map_err(|e| state.db_fail("update recipient", &e))?;
    let (before, recipient) = recipient;
    if let Some(email) = changed_email(check_email, before.as_deref(), recipient.email.as_deref()) {
        state.ensure_destination(&email).await;
    }
    Ok(Json(json!(recipient)))
}

async fn delete(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    pg::delete_recipient(&mut pg, tenant.0, id)
        .await
        .map_err(|e| state.db_fail("delete recipient", &e))?;
    Ok(StatusCode::NO_CONTENT)
}

/// 保存の後に送り先を作るメールアドレス (正規化済み): `admin` で、保存後のアドレスが在り、保存前と(正規化して)違うとき
fn changed_email(admin: bool, before: Option<&str>, after: Option<&str>) -> Option<String> {
    let after = normalize_email(after?);
    let changed = before.map(normalize_email).as_deref() != Some(after.as_str());
    (admin && !after.is_empty() && changed).then_some(after)
}

/// テナントの宛先のうちメールアドレスがあるものの、Cloudflare の送り先としての状態。
/// 宛先を DB から読み (lock を放してから) Cloudflare の一覧を 1 回だけ全ページ引いて突き合わせる。
/// `verified` = 送り先が在って確認済み、`pending` = 在るが未確認、`not_registered` = 無い。
/// 未設定なら Cloudflare を呼ばず `{"configured": false, "items": []}`。一覧が取れなければ 502 (本文は固定の語)
async fn email_status(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    if !state.email_routing.configured() {
        return Ok(Json(json!({"configured": false, "items": []})));
    }
    let recipients = {
        let mut pg = state.pg.lock().await;
        pg::list_recipients(&mut pg, tenant.0).await.map_err(|e| {
            state.db_fail("email status list recipients", &e);
            internal_error_msg("list_recipients_failed")
        })?
    };
    let destinations = state.list_destinations().await.map_err(|_| {
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "upstream_error"})),
        )
    })?;
    let items: Vec<Value> = recipients
        .into_iter()
        .filter_map(|r| {
            let status = match destinations.get(&normalize_email(r.email.as_deref()?)) {
                Some(true) => "verified",
                Some(false) => "pending",
                None => "not_registered",
            };
            Some(json!({"recipient_id": r.id, "email_status": status}))
        })
        .collect();
    Ok(Json(json!({"configured": true, "items": items})))
}
