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

use std::collections::HashMap;

use alc_core_wasm::TenantId;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::pg::{self, CreateNotifyRecipient, UpdateNotifyRecipient};
use crate::NotifyState;

pub const RECIPIENTS_PATH: &str = "/api/notify/recipients";
pub const RECIPIENTS_BULK_PATH: &str = "/api/notify/recipients/bulk";
pub const RECIPIENT_PATH: &str = "/api/notify/recipients/{id}";

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(RECIPIENTS_PATH, get(list).post(create))
        .route(RECIPIENTS_BULK_PATH, post(bulk_upsert))
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
    Json(input): Json<BulkUpsertInput>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let mut created = 0usize;
    let mut updated = 0usize;
    let mut skipped: Vec<Value> = Vec::new();
    let mut touched_ids: Vec<Uuid> = Vec::new();

    // lineworks_user_id → id の対応を 1 回だけ作る
    let existing = pg::list_recipients(&mut pg, tenant.0)
        .await
        .map_err(|e| state.db_fail("bulk list recipients", &e))?;
    let existing_by_lw: HashMap<String, Uuid> = existing
        .into_iter()
        .filter_map(|r| r.lineworks_user_id.map(|id| (id, r.id)))
        .collect();

    for (idx, r) in input.recipients.into_iter().enumerate() {
        let Some(lw_id) = r.lineworks_user_id.clone() else {
            skipped.push(json!({"index": idx, "reason": "missing_lineworks_user_id"}));
            continue;
        };
        let pre_existing_id = existing_by_lw.get(&lw_id).copied();
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
            }
            Err(e) => {
                state.db_fail("bulk upsert recipient", &e);
                skipped.push(json!({"index": idx, "reason": "db_error"}));
            }
        }
    }

    for group_id in input.group_ids {
        if let Err(e) = pg::add_members(&mut pg, tenant.0, group_id, touched_ids.clone()).await {
            state.db_fail("bulk add members", &e);
        }
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
    Json(input): Json<CreateNotifyRecipient>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    let mut pg = state.pg.lock().await;
    let recipient = pg::create_recipient(&mut pg, tenant.0, input)
        .await
        .map_err(|e| state.db_fail("create recipient", &e))?;
    Ok((StatusCode::CREATED, Json(json!(recipient))))
}

async fn update(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(input): Json<UpdateNotifyRecipient>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let recipient = pg::update_recipient(&mut pg, tenant.0, id, input)
        .await
        .map_err(|e| state.db_fail("update recipient", &e))?;
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
