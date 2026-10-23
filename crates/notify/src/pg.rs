//! SQL を流す 1 か所 (`PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement は使わない)。
//!
//! 文の意味は rust-alc-api の `crates/alc-notify/src/repo/notify_recipients.rs`・`notify_groups.rs`・`notify_line_config.rs`・
//! `lineworks_channels.rs`・`notify_documents.rs`・`notify_deliveries.rs` と同じ。違いは 2 つ:
//! - `SELECT *` を列の名前の並びにする (rust は列の名前で struct に詰める。ここは位置で読むので並びを固定する)
//! - tenant_id の列が在る表は、RLS に加えて `WHERE tenant_id` でも絞る (RLS の下では同じ行になる)
//!
//! `notify_recipient_groups` は tenant_id の列を持たず、RLS (migration 099) が group の tenant で絞る。
//! 行は transaction の中で owned な [`NotifyRecipient`] / [`NotifyGroup`] / [`NotifyLineConfig`] / [`LineworksChannel`] /
//! [`NotifyDocument`] / [`DeliveryRow`] / [`LineSecrets`] に
//! 詰め替えてから外へ返す。

use alc_worker_db::{PgClient, TxOutput};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

pub mod sql {
    /// notify_recipients の列 (rust の `NotifyRecipient` の並び)
    macro_rules! recipient_cols {
        () => {
            "id, tenant_id, name, provider, lineworks_user_id, line_user_id, phone_number, email, enabled, created_at, updated_at, employee_id"
        };
    }
    /// notify_groups の列 (rust の `NotifyGroup` の並び)
    macro_rules! group_cols {
        () => {
            "id, tenant_id, name, description, created_at, updated_at"
        };
    }

    pub const LIST_RECIPIENTS: &str = concat!(
        "SELECT ",
        recipient_cols!(),
        " FROM notify_recipients WHERE tenant_id = $1 ORDER BY name"
    );

    pub const GET_RECIPIENT: &str = concat!(
        "SELECT ",
        recipient_cols!(),
        " FROM notify_recipients WHERE tenant_id = $1 AND id = $2"
    );

    pub const CREATE_RECIPIENT: &str = concat!(
        "INSERT INTO notify_recipients (tenant_id, name, provider, lineworks_user_id, line_user_id, phone_number, email) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING ",
        recipient_cols!()
    );

    /// 指定の無い項目は元のまま (rust と同じ COALESCE。null で消す口は無い)。
    /// 例外は employee_id: `$10` (送ってきたか) が真のときだけ `$11` で置く (`$11` が NULL なら結び付けを外す)
    pub const UPDATE_RECIPIENT: &str = concat!(
        "UPDATE notify_recipients SET \
         name = COALESCE($1, name), \
         provider = COALESCE($2, provider), \
         lineworks_user_id = COALESCE($3, lineworks_user_id), \
         line_user_id = COALESCE($4, line_user_id), \
         phone_number = COALESCE($5, phone_number), \
         email = COALESCE($6, email), \
         enabled = COALESCE($7, enabled), \
         employee_id = CASE WHEN $10 THEN $11 ELSE employee_id END, \
         updated_at = NOW() \
         WHERE tenant_id = $8 AND id = $9 RETURNING ",
        recipient_cols!()
    );

    /// 社員マスタの選択肢 (削除済みを除く。社員コードの順、コードの無い人は後ろ)
    pub const LIST_EMPLOYEES: &str = "SELECT id, name, code, role FROM employees \
         WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY code NULLS LAST, name, id";

    /// 結び付けてよい社員 (この tenant の、削除されていない社員) か。FK は RLS を通らないので別 tenant の id も通ってしまうため先に確かめる
    pub const EMPLOYEE_EXISTS: &str =
        "SELECT 1 FROM employees WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL";

    /// 氏名での自動結合が書く 1 件。結び付いていない宛先にだけ書く
    pub const LINK_EMPLOYEE: &str =
        "UPDATE notify_recipients SET employee_id = $1, updated_at = NOW() \
         WHERE tenant_id = $2 AND id = $3 AND employee_id IS NULL";

    pub const DELETE_RECIPIENT: &str =
        "DELETE FROM notify_recipients WHERE tenant_id = $1 AND id = $2";

    pub const LIST_GROUPS: &str = concat!(
        "SELECT ",
        group_cols!(),
        " FROM notify_groups WHERE tenant_id = $1 ORDER BY name"
    );

    pub const GET_GROUP: &str = concat!(
        "SELECT ",
        group_cols!(),
        " FROM notify_groups WHERE tenant_id = $1 AND id = $2"
    );

    pub const CREATE_GROUP: &str = concat!(
        "INSERT INTO notify_groups (tenant_id, name, description) VALUES ($1, $2, $3) RETURNING ",
        group_cols!()
    );

    pub const UPDATE_GROUP: &str = concat!(
        "UPDATE notify_groups SET \
         name = COALESCE($1, name), \
         description = COALESCE($2, description), \
         updated_at = NOW() \
         WHERE tenant_id = $3 AND id = $4 RETURNING ",
        group_cols!()
    );

    pub const DELETE_GROUP: &str = "DELETE FROM notify_groups WHERE tenant_id = $1 AND id = $2";

    /// 冪等 (既に在る組は飛ばす)。別 tenant の group は RLS の WITH CHECK で断られる
    /// 足す宛先のうち、この tenant に在るものの数 (`$2` は重複を除いた id)。[`ADD_MEMBERS`] の前に同じ transaction で数える
    pub const COUNT_TENANT_RECIPIENTS: &str =
        "SELECT count(*) FROM notify_recipients WHERE tenant_id = $1 AND id = ANY($2)";

    /// 冪等 (既に在る組は飛ばす)。宛先が同じ tenant に在ることは [`COUNT_TENANT_RECIPIENTS`] で先に確かめる
    /// (RLS の WITH CHECK は group の tenant しか見ず、FK は RLS を通らないので、別 tenant の宛先も入ってしまう)。
    /// 別 tenant の group は RLS の WITH CHECK で断られる
    pub const ADD_MEMBERS: &str = "INSERT INTO notify_recipient_groups (group_id, recipient_id) \
         SELECT $1, UNNEST($2::uuid[]) ON CONFLICT DO NOTHING";

    pub const REMOVE_MEMBER: &str =
        "DELETE FROM notify_recipient_groups WHERE group_id = $1 AND recipient_id = $2";

    pub const LIST_MEMBERS: &str =
        "SELECT r.id, r.tenant_id, r.name, r.provider, r.lineworks_user_id, r.line_user_id, \
         r.phone_number, r.email, r.enabled, r.created_at, r.updated_at, r.employee_id \
         FROM notify_recipients r \
         JOIN notify_recipient_groups rg ON rg.recipient_id = r.id \
         WHERE rg.group_id = $1 AND r.tenant_id = $2 \
         ORDER BY r.name";

    /// notify_line_configs の返してよい列 (rust の `NotifyLineConfig` の並び。暗号文の列は読まない)
    macro_rules! line_config_cols {
        () => {
            "id, tenant_id, name, channel_id, bot_basic_id, public_key_jwk, enabled, created_at, updated_at"
        };
    }
    /// lineworks_channels の列 (rust の `LineworksChannel` の並び)
    macro_rules! channel_cols {
        () => {
            "id, tenant_id, bot_config_id, channel_id, title, channel_type, joined_at, active, created_at, updated_at"
        };
    }

    pub const GET_LINE_CONFIG: &str = concat!(
        "SELECT ",
        line_config_cols!(),
        " FROM notify_line_configs WHERE tenant_id = $1 LIMIT 1"
    );

    /// tenant に 1 行 (UNIQUE (tenant_id))。在れば全部の項目を差し替える (rust と同じ)
    pub const UPSERT_LINE_CONFIG: &str = concat!(
        "INSERT INTO notify_line_configs \
         (tenant_id, name, channel_id, channel_secret_encrypted, key_id, private_key_encrypted, bot_basic_id, public_key_jwk) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (tenant_id) DO UPDATE SET \
         name = EXCLUDED.name, \
         channel_id = EXCLUDED.channel_id, \
         channel_secret_encrypted = EXCLUDED.channel_secret_encrypted, \
         key_id = EXCLUDED.key_id, \
         private_key_encrypted = EXCLUDED.private_key_encrypted, \
         bot_basic_id = EXCLUDED.bot_basic_id, \
         public_key_jwk = EXCLUDED.public_key_jwk, \
         updated_at = NOW() \
         RETURNING ",
        line_config_cols!()
    );

    pub const DELETE_LINE_CONFIG: &str = "DELETE FROM notify_line_configs WHERE tenant_id = $1";

    pub const LIST_ACTIVE_CHANNELS: &str = concat!(
        "SELECT ",
        channel_cols!(),
        " FROM lineworks_channels WHERE tenant_id = $1 AND active = TRUE ORDER BY joined_at DESC"
    );

    pub const GET_CHANNEL: &str = concat!(
        "SELECT ",
        channel_cols!(),
        " FROM lineworks_channels WHERE tenant_id = $1 AND id = $2"
    );

    pub const DELETE_CHANNEL: &str =
        "DELETE FROM lineworks_channels WHERE tenant_id = $1 AND id = $2";

    // ---- 配信 (rust の `repo/notify_documents.rs`・`notify_deliveries.rs`・`notify_recipients.rs` の list_enabled・
    //      `notify_groups.rs` の list_enabled_members・`notify_line_config.rs` の get_full) ----

    /// 配信に使う notify_documents の列 ([`super::NotifyDocument`] の並び)。extracted_data は text で読んで JSON にする
    pub const GET_DOCUMENT: &str = "SELECT id, r2_key, file_name, file_size_bytes, source_subject, source_sender, \
         extracted_title, extracted_summary, extracted_data::text, redaction_status, redacted_r2_key, created_at \
         FROM notify_documents WHERE tenant_id = $1 AND id = $2";

    pub const LIST_ENABLED_RECIPIENTS: &str = concat!(
        "SELECT ",
        recipient_cols!(),
        " FROM notify_recipients WHERE tenant_id = $1 AND enabled = TRUE ORDER BY name"
    );

    pub const LIST_ENABLED_MEMBERS: &str =
        "SELECT r.id, r.tenant_id, r.name, r.provider, r.lineworks_user_id, r.line_user_id, \
         r.phone_number, r.email, r.enabled, r.created_at, r.updated_at, r.employee_id \
         FROM notify_recipients r \
         JOIN notify_recipient_groups rg ON rg.recipient_id = r.id \
         WHERE rg.group_id = $1 AND r.tenant_id = $2 AND r.enabled = TRUE \
         ORDER BY r.name";

    /// LINE の送信に要る列 (暗号文のまま。復号は呼び手)
    pub const GET_LINE_SECRETS: &str = "SELECT channel_id, key_id, private_key_encrypted \
         FROM notify_line_configs WHERE tenant_id = $1 LIMIT 1";

    /// `completed` のときだけ distributed_at を今にする (rust の `COALESCE($2, distributed_at)` と同じ)
    pub const UPDATE_DISTRIBUTION_STATUS: &str = "UPDATE notify_documents SET \
         distribution_status = $1, \
         distributed_at = CASE WHEN $1 = 'completed' THEN NOW() ELSE distributed_at END, \
         updated_at = NOW() \
         WHERE tenant_id = $2 AND id = $3";

    pub const CREATE_DELIVERY: &str =
        "INSERT INTO notify_deliveries (tenant_id, document_id, recipient_id, provider) \
         VALUES ($1, $2, $3, $4) RETURNING id, recipient_id, read_token";

    pub const SET_TRIGGERED_BY: &str = "UPDATE notify_deliveries SET triggered_by_user_id = $1 \
         WHERE tenant_id = $2 AND id = ANY($3)";

    /// 既定 (migration 106 の `NOW() + 7 days`) 以外の閲覧期限
    pub const SET_EXPIRE_AT: &str =
        "UPDATE notify_deliveries SET expire_at = NOW() + make_interval(days => $1) \
         WHERE tenant_id = $2 AND id = ANY($3)";

    pub const MARK_SENT: &str = "UPDATE notify_deliveries SET status = 'sent', sent_at = NOW(), \
         attempt = attempt + 1 WHERE tenant_id = $1 AND id = $2";

    /// `error_message` は固定の語だけ (上流の本文・宛先を入れない)
    pub const MARK_FAILED: &str = "UPDATE notify_deliveries SET status = 'failed', \
         error_message = $1, attempt = attempt + 1 WHERE tenant_id = $2 AND id = $3";
}

/// rust の `alc_core::repository::notify_recipients::NotifyRecipient` と同じ JSON
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NotifyRecipient {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub provider: String,
    pub lineworks_user_id: Option<String>,
    pub line_user_id: Option<String>,
    pub phone_number: Option<String>,
    pub email: Option<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// 社員マスタのどの社員か (結び付けていなければ null。migration 184)
    pub employee_id: Option<Uuid>,
}

impl TxOutput for NotifyRecipient {}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateNotifyRecipient {
    pub name: String,
    pub provider: String,
    pub lineworks_user_id: Option<String>,
    pub line_user_id: Option<String>,
    pub phone_number: Option<String>,
    pub email: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpdateNotifyRecipient {
    pub name: Option<String>,
    pub provider: Option<String>,
    pub lineworks_user_id: Option<String>,
    pub line_user_id: Option<String>,
    pub phone_number: Option<String>,
    pub email: Option<String>,
    pub enabled: Option<bool>,
    /// 送らなければ変えない (`None`)・`null` で結び付けを外す (`Some(None)`)・UUID で結び付ける (`Some(Some(id))`)
    #[serde(default, deserialize_with = "present_or_null")]
    pub employee_id: Option<Option<Uuid>>,
}

/// 項目が在る (`null` を含む) ことを `Some` で見分ける (`Option<Option<T>>` の既定の読みは `null` と無しを区別しない)
fn present_or_null<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

/// rust の `alc_core::repository::notify_groups::NotifyGroup` と同じ JSON
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NotifyGroup {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TxOutput for NotifyGroup {}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateNotifyGroup {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateNotifyGroup {
    pub name: Option<String>,
    pub description: Option<String>,
}

/// rust の `alc_core::repository::notify_line_config::NotifyLineConfig` と同じ JSON (暗号文の項目を持たない)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NotifyLineConfig {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub channel_id: String,
    pub bot_basic_id: Option<String>,
    pub public_key_jwk: Option<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TxOutput for NotifyLineConfig {}

/// [`upsert_line_config`] に渡す値。秘密値 2 つは暗号化した後の文字列
#[derive(Debug, Clone)]
pub struct UpsertLineConfigRow {
    pub name: String,
    pub channel_id: String,
    pub channel_secret_encrypted: String,
    pub key_id: String,
    pub private_key_encrypted: String,
    pub bot_basic_id: Option<String>,
    pub public_key_jwk: Option<String>,
}

/// rust の `alc_core::repository::lineworks_channels::LineworksChannel` と同じ JSON
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LineworksChannel {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub bot_config_id: Uuid,
    pub channel_id: String,
    pub title: Option<String>,
    pub channel_type: Option<String>,
    pub joined_at: DateTime<Utc>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TxOutput for LineworksChannel {}

/// 配信に使う notify_documents の列 (rust の `NotifyDocument` のうち distribute が読むもの)
#[derive(Debug, Clone, PartialEq)]
pub struct NotifyDocument {
    pub id: Uuid,
    pub r2_key: String,
    pub file_name: Option<String>,
    pub file_size_bytes: Option<i64>,
    pub source_subject: Option<String>,
    pub source_sender: Option<String>,
    pub extracted_title: Option<String>,
    pub extracted_summary: Option<String>,
    pub extracted_data: Option<serde_json::Value>,
    pub redaction_status: String,
    pub redacted_r2_key: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for NotifyDocument {}

/// notify_deliveries に作った 1 行
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryRow {
    pub id: Uuid,
    pub recipient_id: Uuid,
    pub read_token: Uuid,
}

impl TxOutput for DeliveryRow {}

/// LINE の送信に要る設定 (暗号文のまま)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineSecrets {
    pub channel_id: String,
    pub key_id: Option<String>,
    pub private_key_encrypted: Option<String>,
}

impl TxOutput for LineSecrets {}

fn document(r: &Row) -> NotifyDocument {
    NotifyDocument {
        id: r.get(0),
        r2_key: r.get(1),
        file_name: r.get(2),
        file_size_bytes: r.get(3),
        source_subject: r.get(4),
        source_sender: r.get(5),
        extracted_title: r.get(6),
        extracted_summary: r.get(7),
        extracted_data: r
            .get::<_, Option<String>>(8)
            .and_then(|s| serde_json::from_str(&s).ok()),
        redaction_status: r.get(9),
        redacted_r2_key: r.get(10),
        created_at: r.get(11),
    }
}

fn line_config(r: &Row) -> NotifyLineConfig {
    NotifyLineConfig {
        id: r.get(0),
        tenant_id: r.get(1),
        name: r.get(2),
        channel_id: r.get(3),
        bot_basic_id: r.get(4),
        public_key_jwk: r.get(5),
        enabled: r.get(6),
        created_at: r.get(7),
        updated_at: r.get(8),
    }
}

fn channel(r: &Row) -> LineworksChannel {
    LineworksChannel {
        id: r.get(0),
        tenant_id: r.get(1),
        bot_config_id: r.get(2),
        channel_id: r.get(3),
        title: r.get(4),
        channel_type: r.get(5),
        joined_at: r.get(6),
        active: r.get(7),
        created_at: r.get(8),
        updated_at: r.get(9),
    }
}

fn recipient(r: &Row) -> NotifyRecipient {
    NotifyRecipient {
        id: r.get(0),
        tenant_id: r.get(1),
        name: r.get(2),
        provider: r.get(3),
        lineworks_user_id: r.get(4),
        line_user_id: r.get(5),
        phone_number: r.get(6),
        email: r.get(7),
        enabled: r.get(8),
        created_at: r.get(9),
        updated_at: r.get(10),
        employee_id: r.get(11),
    }
}

fn group(r: &Row) -> NotifyGroup {
    NotifyGroup {
        id: r.get(0),
        tenant_id: r.get(1),
        name: r.get(2),
        description: r.get(3),
        created_at: r.get(4),
        updated_at: r.get(5),
    }
}

pub async fn list_recipients(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<NotifyRecipient>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_RECIPIENTS, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(recipient).collect())
        })
    })
    .await
}

pub async fn get_recipient(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<NotifyRecipient>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::GET_RECIPIENT,
                    &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                )
                .await?;
            Ok(row.as_ref().map(recipient))
        })
    })
    .await
}

pub async fn create_recipient(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateNotifyRecipient,
) -> Result<NotifyRecipient, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::CREATE_RECIPIENT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.name, Type::TEXT),
                        (&input.provider, Type::TEXT),
                        (&input.lineworks_user_id, Type::TEXT),
                        (&input.line_user_id, Type::TEXT),
                        (&input.phone_number, Type::TEXT),
                        (&input.email, Type::TEXT),
                    ],
                )
                .await?;
            Ok(recipient(&row))
        })
    })
    .await
}

/// 行が無い (別 tenant を含む) ときは `Err` (rust の `fetch_one` と同じく 500 になる)
pub async fn update_recipient(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    input: UpdateNotifyRecipient,
) -> Result<NotifyRecipient, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::UPDATE_RECIPIENT,
                    &[
                        (&input.name, Type::TEXT),
                        (&input.provider, Type::TEXT),
                        (&input.lineworks_user_id, Type::TEXT),
                        (&input.line_user_id, Type::TEXT),
                        (&input.phone_number, Type::TEXT),
                        (&input.email, Type::TEXT),
                        (&input.enabled, Type::BOOL),
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&input.employee_id.is_some(), Type::BOOL),
                        (&input.employee_id.flatten(), Type::UUID),
                    ],
                )
                .await?;
            Ok(recipient(&row))
        })
    })
    .await
}

/// 社員マスタの 1 行 (宛先の画面の選択肢)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Employee {
    pub id: Uuid,
    pub name: String,
    pub code: Option<String>,
    /// 役割 (`driver` / `manager` / `admin` など。employees.role の TEXT[])。血圧の日報を受け取れる人 (manager・admin) の判定に使う
    pub role: Vec<String>,
}

impl TxOutput for Employee {}

pub async fn list_employees(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<Employee>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_EMPLOYEES, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(employee).collect())
        })
    })
    .await
}

fn employee(r: &Row) -> Employee {
    Employee {
        id: r.get(0),
        name: r.get(1),
        code: r.get(2),
        role: r.get(3),
    }
}

/// この tenant の、削除されていない社員か
pub async fn employee_exists(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::EMPLOYEE_EXISTS,
                    &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                )
                .await?;
            Ok(row.is_some())
        })
    })
    .await
}

/// 氏名の比べ方: 空白 (半角・全角を含む `char::is_whitespace`) を全部除く
pub fn name_key(name: &str) -> String {
    name.chars().filter(|c| !c.is_whitespace()).collect()
}

/// 自動結合の候補 (宛先 ↔ 社員)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LinkCandidate {
    pub recipient_id: Uuid,
    pub recipient_name: String,
    pub employee_id: Uuid,
    pub employee_name: String,
    pub employee_code: Option<String>,
}

/// 結ばなかった宛先とその理由。`reason` は固定の語:
/// `already_linked` (その宛先は既に社員に結び付いている)・`no_match` (一致する社員がいない・名前が空)・
/// `multiple_employees` (同じ名前の社員が 2 人以上)・`employee_already_linked` (その社員は既に他の宛先に結び付いている)・
/// `multiple_recipients` (同じ名前の宛先が 2 つ以上で、同じ社員を取り合う)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LinkSkipped {
    pub recipient_id: Uuid,
    pub recipient_name: String,
    pub reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LinkPlan {
    pub candidates: Vec<LinkCandidate>,
    pub skipped: Vec<LinkSkipped>,
}

impl TxOutput for LinkPlan {}

/// 規則: 宛先の名前と社員の名前を [`name_key`] で比べ、一致する社員がちょうど 1 人で、その宛先がまだ結び付いておらず、
/// その社員もまだ他の宛先に結び付いていないものだけを結ぶ。同じ名前の未結合の宛先が 2 つ以上ある組は、どちらも結ばない
/// (同じ社員を取り合い、部分 UNIQUE に当たるため)
pub fn plan_links(recipients: &[NotifyRecipient], employees: &[Employee]) -> LinkPlan {
    let linked: std::collections::HashSet<Uuid> =
        recipients.iter().filter_map(|r| r.employee_id).collect();
    let mut unlinked_per_key: std::collections::HashMap<String, usize> = Default::default();
    for r in recipients.iter().filter(|r| r.employee_id.is_none()) {
        *unlinked_per_key.entry(name_key(&r.name)).or_default() += 1;
    }
    let mut plan = LinkPlan {
        candidates: Vec::new(),
        skipped: Vec::new(),
    };
    for r in recipients {
        let skip = |reason| LinkSkipped {
            recipient_id: r.id,
            recipient_name: r.name.clone(),
            reason,
        };
        if r.employee_id.is_some() {
            plan.skipped.push(skip("already_linked"));
            continue;
        }
        let key = name_key(&r.name);
        let matches: Vec<&Employee> = employees
            .iter()
            .filter(|e| !key.is_empty() && name_key(&e.name) == key)
            .collect();
        match matches.as_slice() {
            [] => plan.skipped.push(skip("no_match")),
            [e] if linked.contains(&e.id) => plan.skipped.push(skip("employee_already_linked")),
            [_] if unlinked_per_key[&key] > 1 => plan.skipped.push(skip("multiple_recipients")),
            [e] => plan.candidates.push(LinkCandidate {
                recipient_id: r.id,
                recipient_name: r.name.clone(),
                employee_id: e.id,
                employee_name: e.name.clone(),
                employee_code: e.code.clone(),
            }),
            _ => plan.skipped.push(skip("multiple_employees")),
        }
    }
    plan
}

/// 氏名での自動結合。宛先と社員を読んで [`plan_links`] で候補を決め、`dry_run` でなければ同じ transaction で結ぶ
/// (途中で部分 UNIQUE に当たったら全部やめる = `Err`)
pub async fn link_employees_by_name(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dry_run: bool,
) -> Result<LinkPlan, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_RECIPIENTS, &[(&tenant_id, Type::UUID)])
                .await?;
            let recipients: Vec<NotifyRecipient> = rows.iter().map(recipient).collect();
            let rows = tx
                .query_typed(sql::LIST_EMPLOYEES, &[(&tenant_id, Type::UUID)])
                .await?;
            let employees: Vec<Employee> = rows.iter().map(employee).collect();
            let plan = plan_links(&recipients, &employees);
            if !dry_run {
                for c in &plan.candidates {
                    tx.execute_typed(
                        sql::LINK_EMPLOYEE,
                        &[
                            (&c.employee_id, Type::UUID),
                            (&tenant_id, Type::UUID),
                            (&c.recipient_id, Type::UUID),
                        ],
                    )
                    .await?;
                }
            }
            Ok(plan)
        })
    })
    .await
}

pub async fn delete_recipient(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::DELETE_RECIPIENT,
                &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

pub async fn list_groups(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<NotifyGroup>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_GROUPS, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(group).collect())
        })
    })
    .await
}

pub async fn get_group(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<NotifyGroup>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::GET_GROUP,
                    &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                )
                .await?;
            Ok(row.as_ref().map(group))
        })
    })
    .await
}

pub async fn create_group(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateNotifyGroup,
) -> Result<NotifyGroup, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::CREATE_GROUP,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.name, Type::TEXT),
                        (&input.description, Type::TEXT),
                    ],
                )
                .await?;
            Ok(group(&row))
        })
    })
    .await
}

/// 行が無い (別 tenant を含む) ときは `Err` (rust の `fetch_one` と同じく 500 になる)
pub async fn update_group(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    input: UpdateNotifyGroup,
) -> Result<NotifyGroup, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::UPDATE_GROUP,
                    &[
                        (&input.name, Type::TEXT),
                        (&input.description, Type::TEXT),
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                    ],
                )
                .await?;
            Ok(group(&row))
        })
    })
    .await
}

pub async fn delete_group(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::DELETE_GROUP,
                &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// [`add_members`] の失敗
#[derive(Debug)]
pub enum AddMembersError {
    /// 足す宛先のどれかが、この tenant に無い (別 tenant・存在しない)。何も足さない
    RecipientNotFound,
    Db(Error),
}

/// 宛先をグループに足す。**宛先がすべてこの tenant に在るときだけ**足し、1 つでも無ければ何も足さずに
/// [`AddMembersError::RecipientNotFound`] (rust はここを検査せず、別 tenant の宛先も入った)。空なら DB に触らない (rust と同じ)
pub async fn add_members(
    pg: &mut PgClient,
    tenant_id: Uuid,
    group_id: Uuid,
    mut recipient_ids: Vec<Uuid>,
) -> Result<(), AddMembersError> {
    if recipient_ids.is_empty() {
        return Ok(());
    }
    recipient_ids.sort_unstable();
    recipient_ids.dedup();
    let added = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                let found: i64 = tx
                    .query_typed_one(
                        sql::COUNT_TENANT_RECIPIENTS,
                        &[(&tenant_id, Type::UUID), (&recipient_ids, Type::UUID_ARRAY)],
                    )
                    .await?
                    .get(0);
                if found != recipient_ids.len() as i64 {
                    return Ok(false);
                }
                tx.execute_typed(
                    sql::ADD_MEMBERS,
                    &[(&group_id, Type::UUID), (&recipient_ids, Type::UUID_ARRAY)],
                )
                .await?;
                Ok(true)
            })
        })
        .await
        .map_err(AddMembersError::Db)?;
    if added {
        Ok(())
    } else {
        Err(AddMembersError::RecipientNotFound)
    }
}

pub async fn remove_member(
    pg: &mut PgClient,
    tenant_id: Uuid,
    group_id: Uuid,
    recipient_id: Uuid,
) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::REMOVE_MEMBER,
                &[(&group_id, Type::UUID), (&recipient_id, Type::UUID)],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

pub async fn list_members(
    pg: &mut PgClient,
    tenant_id: Uuid,
    group_id: Uuid,
) -> Result<Vec<NotifyRecipient>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST_MEMBERS,
                    &[(&group_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(recipient).collect())
        })
    })
    .await
}

pub async fn get_line_config(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Option<NotifyLineConfig>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET_LINE_CONFIG, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(line_config))
        })
    })
    .await
}

pub async fn upsert_line_config(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: UpsertLineConfigRow,
) -> Result<NotifyLineConfig, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::UPSERT_LINE_CONFIG,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.name, Type::TEXT),
                        (&input.channel_id, Type::TEXT),
                        (&input.channel_secret_encrypted, Type::TEXT),
                        (&input.key_id, Type::TEXT),
                        (&input.private_key_encrypted, Type::TEXT),
                        (&input.bot_basic_id, Type::TEXT),
                        (&input.public_key_jwk, Type::TEXT),
                    ],
                )
                .await?;
            Ok(line_config(&row))
        })
    })
    .await
}

pub async fn delete_line_config(pg: &mut PgClient, tenant_id: Uuid) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(sql::DELETE_LINE_CONFIG, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(())
        })
    })
    .await
}

pub async fn list_active_channels(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<LineworksChannel>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_ACTIVE_CHANNELS, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(channel).collect())
        })
    })
    .await
}

pub async fn get_channel(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<LineworksChannel>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::GET_CHANNEL,
                    &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                )
                .await?;
            Ok(row.as_ref().map(channel))
        })
    })
    .await
}

pub async fn delete_channel(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::DELETE_CHANNEL,
                &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

pub async fn get_document(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<NotifyDocument>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::GET_DOCUMENT,
                    &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                )
                .await?;
            Ok(row.as_ref().map(document))
        })
    })
    .await
}

pub async fn list_enabled_recipients(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<NotifyRecipient>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_ENABLED_RECIPIENTS, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(recipient).collect())
        })
    })
    .await
}

pub async fn list_enabled_members(
    pg: &mut PgClient,
    tenant_id: Uuid,
    group_id: Uuid,
) -> Result<Vec<NotifyRecipient>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST_ENABLED_MEMBERS,
                    &[(&group_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(recipient).collect())
        })
    })
    .await
}

pub async fn get_line_secrets(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Option<LineSecrets>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET_LINE_SECRETS, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(row.map(|r| LineSecrets {
                channel_id: r.get(0),
                key_id: r.get(1),
                private_key_encrypted: r.get(2),
            }))
        })
    })
    .await
}

pub async fn update_distribution_status(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    status: &'static str,
) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::UPDATE_DISTRIBUTION_STATUS,
                &[
                    (&status, Type::TEXT),
                    (&tenant_id, Type::UUID),
                    (&id, Type::UUID),
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// 宛先ごとに 1 行 (渡した順に返す。rust の `create_batch` と同じく 1 つの transaction)
pub async fn create_deliveries(
    pg: &mut PgClient,
    tenant_id: Uuid,
    document_id: Uuid,
    recipients: Vec<(Uuid, String)>,
) -> Result<Vec<DeliveryRow>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let mut out = Vec::with_capacity(recipients.len());
            for (recipient_id, provider) in &recipients {
                let row = tx
                    .query_typed_one(
                        sql::CREATE_DELIVERY,
                        &[
                            (&tenant_id, Type::UUID),
                            (&document_id, Type::UUID),
                            (recipient_id, Type::UUID),
                            (provider, Type::TEXT),
                        ],
                    )
                    .await?;
                out.push(DeliveryRow {
                    id: row.get(0),
                    recipient_id: row.get(1),
                    read_token: row.get(2),
                });
            }
            Ok(out)
        })
    })
    .await
}

/// 作った行に、配信した人 (`triggered_by`) と既定以外の閲覧期限 (`expire_days`) を書く (1 つの transaction。
/// rust は同じ 2 本の UPDATE を別々に流していた)。どちらも無ければ DB に触らない
pub async fn adjust_deliveries(
    pg: &mut PgClient,
    tenant_id: Uuid,
    ids: Vec<Uuid>,
    triggered_by: Option<Uuid>,
    expire_days: Option<i32>,
) -> Result<(), Error> {
    if triggered_by.is_none() && expire_days.is_none() {
        return Ok(());
    }
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            if let Some(user_id) = triggered_by {
                tx.execute_typed(
                    sql::SET_TRIGGERED_BY,
                    &[
                        (&user_id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&ids, Type::UUID_ARRAY),
                    ],
                )
                .await?;
            }
            if let Some(days) = expire_days {
                tx.execute_typed(
                    sql::SET_EXPIRE_AT,
                    &[
                        (&days, Type::INT4),
                        (&tenant_id, Type::UUID),
                        (&ids, Type::UUID_ARRAY),
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

pub async fn mark_sent(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::MARK_SENT,
                &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// `reason` は固定の語 (上流の本文・宛先を入れない)
pub async fn mark_failed(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    reason: &'static str,
) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::MARK_FAILED,
                &[
                    (&reason, Type::TEXT),
                    (&tenant_id, Type::UUID),
                    (&id, Type::UUID),
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
}
