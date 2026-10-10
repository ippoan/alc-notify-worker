//! 宛先のグループ (notify_groups) とメンバー (notify_recipient_groups) の口。
//! rust-alc-api の `crates/alc-notify/src/groups.rs` と同じ path・method・JSON・status。
//!
//! | method | path | 成功 |
//! |---|---|---|
//! | GET | `/api/notify/groups` | 200 名前の順の配列 |
//! | POST | `/api/notify/groups` | 201 作った行 |
//! | GET | `/api/notify/groups/{id}` | 200 / 無ければ 404 |
//! | PUT | `/api/notify/groups/{id}` | 200 直した行 (無ければ 500。rust と同じ) |
//! | DELETE | `/api/notify/groups/{id}` | 204 (無くても 204) |
//! | GET | `/api/notify/groups/{id}/members` | 200 メンバーの宛先 (名前の順) |
//! | POST | `/api/notify/groups/{id}/members` | 204 (`{"recipient_ids": [...]}`、既に在る組は飛ばす) |
//! | DELETE | `/api/notify/groups/{id}/members/{recipient_id}` | 204 |

use alc_core_wasm::TenantId;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::pg::{self, CreateNotifyGroup, UpdateNotifyGroup};
use crate::NotifyState;

pub const GROUPS_PATH: &str = "/api/notify/groups";
pub const GROUP_PATH: &str = "/api/notify/groups/{id}";
pub const GROUP_MEMBERS_PATH: &str = "/api/notify/groups/{id}/members";
pub const GROUP_MEMBER_PATH: &str = "/api/notify/groups/{id}/members/{recipient_id}";

pub fn router() -> Router<NotifyState> {
    Router::new()
        .route(GROUPS_PATH, get(list).post(create))
        .route(GROUP_PATH, get(get_one).put(update).delete(delete_one))
        .route(GROUP_MEMBERS_PATH, get(list_members).post(add_members))
        .route(GROUP_MEMBER_PATH, delete(remove_member))
}

#[derive(Debug, Deserialize)]
pub struct AddMembersInput {
    pub recipient_ids: Vec<Uuid>,
}

async fn list(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let groups = pg::list_groups(&mut pg, tenant.0)
        .await
        .map_err(|e| state.db_fail("list groups", &e))?;
    Ok(Json(json!(groups)))
}

async fn get_one(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let group = pg::get_group(&mut pg, tenant.0, id)
        .await
        .map_err(|e| state.db_fail("get group", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(json!(group)))
}

async fn create(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Json(input): Json<CreateNotifyGroup>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    let mut pg = state.pg.lock().await;
    let group = pg::create_group(&mut pg, tenant.0, input)
        .await
        .map_err(|e| state.db_fail("create group", &e))?;
    Ok((StatusCode::CREATED, Json(json!(group))))
}

async fn update(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(input): Json<UpdateNotifyGroup>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let group = pg::update_group(&mut pg, tenant.0, id, input)
        .await
        .map_err(|e| state.db_fail("update group", &e))?;
    Ok(Json(json!(group)))
}

async fn delete_one(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    pg::delete_group(&mut pg, tenant.0, id)
        .await
        .map_err(|e| state.db_fail("delete group", &e))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_members(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let members = pg::list_members(&mut pg, tenant.0, id)
        .await
        .map_err(|e| state.db_fail("list group members", &e))?;
    Ok(Json(json!(members)))
}

async fn add_members(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(input): Json<AddMembersInput>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    pg::add_members(&mut pg, tenant.0, id, input.recipient_ids)
        .await
        .map_err(|e| state.db_fail("add group members", &e))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_member(
    State(state): State<NotifyState>,
    Extension(tenant): Extension<TenantId>,
    Path((id, recipient_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    pg::remove_member(&mut pg, tenant.0, id, recipient_id)
        .await
        .map_err(|e| state.db_fail("remove group member", &e))?;
    Ok(StatusCode::NO_CONTENT)
}
