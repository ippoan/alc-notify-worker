//! SQL を流す 1 か所 (`PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement は使わない)。
//!
//! 文の意味は rust-alc-api の `crates/alc-notify/src/repo/notify_recipients.rs`・`notify_groups.rs`・`notify_line_config.rs`・
//! `lineworks_channels.rs` と同じ。違いは 2 つ:
//! - `SELECT *` を列の名前の並びにする (rust は列の名前で struct に詰める。ここは位置で読むので並びを固定する)
//! - tenant_id の列が在る表は、RLS に加えて `WHERE tenant_id` でも絞る (RLS の下では同じ行になる)
//!
//! `notify_recipient_groups` は tenant_id の列を持たず、RLS (migration 099) が group の tenant で絞る。
//! 行は transaction の中で owned な [`NotifyRecipient`] / [`NotifyGroup`] / [`NotifyLineConfig`] / [`LineworksChannel`] に
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
            "id, tenant_id, name, provider, lineworks_user_id, line_user_id, phone_number, email, enabled, created_at, updated_at"
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

    /// 指定の無い項目は元のまま (rust と同じ COALESCE。null で消す口は無い)
    pub const UPDATE_RECIPIENT: &str = concat!(
        "UPDATE notify_recipients SET \
         name = COALESCE($1, name), \
         provider = COALESCE($2, provider), \
         lineworks_user_id = COALESCE($3, lineworks_user_id), \
         line_user_id = COALESCE($4, line_user_id), \
         phone_number = COALESCE($5, phone_number), \
         email = COALESCE($6, email), \
         enabled = COALESCE($7, enabled), \
         updated_at = NOW() \
         WHERE tenant_id = $8 AND id = $9 RETURNING ",
        recipient_cols!()
    );

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
         r.phone_number, r.email, r.enabled, r.created_at, r.updated_at \
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
                    ],
                )
                .await?;
            Ok(recipient(&row))
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
