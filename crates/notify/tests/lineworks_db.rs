//! line-config・LINE WORKS の channel・users・login-activity の口を、実 DB (`alc_api_rt` で接続) と
//! lineworks-worker のモック (tests/support の `MockLineworks`) で通す。
//! 接続先は `NOTIFY_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。
//! 期待値は rust-alc-api の `crates/alc-notify/src/line_config.rs`・`lineworks_channels.rs`・`lineworks_directory.rs`・
//! `lineworks_login_activity.rs` と、ippoan/alc-lineworks-worker の internal の口の契約。

mod support;

use alc_worker_crypto::secret::decrypt_secret;
use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const LINE_CONFIG: &str = "/api/notify/line-config";
const CHANNELS: &str = "/api/notify/lineworks/channels";
const USERS: &str = "/api/notify/lineworks/users";
const LOGIN_ACTIVITY: &str = "/api/notify/lineworks/login-activity";

fn keys(v: &Value) -> Vec<&str> {
    let mut k: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    k.sort();
    k
}

fn line_config_body(name: &str) -> Value {
    json!({
        "name": name,
        "channel_id": "ch-1",
        "channel_secret": "secret-平文",
        "key_id": "kid-1",
        "private_key": "dummy-private-key\nline-2\n",
        "bot_basic_id": "@bot",
    })
}

/// bot_configs の行を 1 つ作る (superuser で。lineworks_channels の FK 用)
async fn bot_config(t: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.bot_configs (id, tenant_id, name, client_id, client_secret_encrypted, \
             service_account, bot_id) VALUES ($1, $2, 'bot', 'c', 'x', 's', $3)",
            &[&id, &t, &format!("bot-{id}")],
        )
        .await
        .unwrap();
    id
}

/// lineworks_channels の行を 1 つ作る (superuser で)。`days_ago` 日前に招かれた
async fn channel(t: Uuid, bot: Uuid, channel_id: &str, days_ago: i32, active: bool) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.lineworks_channels (id, tenant_id, bot_config_id, channel_id, title, \
             channel_type, joined_at, active) \
             VALUES ($1, $2, $3, $4, 'room', 'group', now() - make_interval(days => $5), $6)",
            &[&id, &t, &bot, &channel_id, &days_ago, &active],
        )
        .await
        .unwrap();
    id
}

/// 保存 (暗号化) → 取得 (暗号文を返さない) → 差し替え → 別 tenant から見えない → 削除。鍵が無ければ 500
#[tokio::test]
async fn line_config_crud() {
    let t = tenant().await;
    let other = tenant().await;
    let app = app().await;

    let got = call(&app, "GET", LINE_CONFIG, Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, Value::Null));

    let got = call(
        &app,
        "POST",
        LINE_CONFIG,
        Some(t),
        Some(line_config_body("LINE")),
    )
    .await;
    assert_eq!(got.status, 200);
    let saved = got.json();
    assert_eq!(
        keys(&saved),
        vec![
            "bot_basic_id",
            "channel_id",
            "created_at",
            "enabled",
            "id",
            "name",
            "public_key_jwk",
            "tenant_id",
            "updated_at"
        ]
    );
    assert_eq!(saved["tenant_id"], json!(t.to_string()));
    assert_eq!(saved["bot_basic_id"], json!("@bot"));
    assert_eq!(saved["public_key_jwk"], Value::Null);
    assert_eq!(saved["enabled"], json!(true));

    // DB には暗号文で入り、rust と同じ形式の復号で元に戻る
    let row = admin()
        .await
        .query_one(
            "SELECT channel_secret_encrypted, private_key_encrypted, key_id FROM alc_api.notify_line_configs WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap();
    let (cs, pk, kid): (String, String, String) = (row.get(0), row.get(1), row.get(2));
    assert_ne!(cs, "secret-平文");
    assert_eq!(decrypt_secret(&cs, TEST_SSO_KEY).unwrap(), "secret-平文");
    assert_eq!(
        decrypt_secret(&pk, TEST_SSO_KEY).unwrap(),
        "dummy-private-key\nline-2\n"
    );
    assert_eq!(kid, "kid-1");

    let got = call(&app, "GET", LINE_CONFIG, Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, saved.clone()));

    // tenant に 1 行。2 回目は同じ行を差し替える
    let mut body = line_config_body("LINE 2");
    body["public_key_jwk"] = json!("{\"kty\":\"RSA\"}");
    let got = call(&app, "POST", LINE_CONFIG, Some(t), Some(body)).await;
    assert_eq!(got.status, 200);
    let replaced = got.json();
    assert_eq!(replaced["id"], saved["id"]);
    assert_eq!(replaced["name"], json!("LINE 2"));
    assert_eq!(replaced["public_key_jwk"], json!("{\"kty\":\"RSA\"}"));

    let got = call(&app, "GET", LINE_CONFIG, Some(other), None).await;
    assert_eq!((got.status, got.json()), (200, Value::Null));
    let got = call(&app, "DELETE", LINE_CONFIG, Some(other), None).await;
    assert_eq!(got.status, 204);
    let got = call(&app, "GET", LINE_CONFIG, Some(t), None).await;
    assert_eq!(got.json()["id"], saved["id"]);

    let got = call(&app, "DELETE", LINE_CONFIG, Some(t), None).await;
    assert_eq!((got.status, got.body.len()), (204, 0));
    let got = call(&app, "GET", LINE_CONFIG, Some(t), None).await;
    assert_eq!(got.json(), Value::Null);
    assert!(app.logs().is_empty());
    assert!(app.lineworks.calls().is_empty());

    // 鍵が無い → 本文なしの 500、何も保存しない
    let no_key = build_with_key(rt_client().await, None);
    let got = call(
        &no_key,
        "POST",
        LINE_CONFIG,
        Some(t),
        Some(line_config_body("x")),
    )
    .await;
    assert_eq!((got.status, got.body.len()), (500, 0));
    assert_eq!(
        no_key.logs(),
        vec!["notify: upsert line config failed: encrypt".to_owned()]
    );
    let got = call(&app, "GET", LINE_CONFIG, Some(t), None).await;
    assert_eq!(got.json(), Value::Null);
}

/// 一覧は自 tenant の active な行だけ (joined_at の新しい順)。削除は自 tenant の行だけ消える
#[tokio::test]
async fn channels_list_and_delete() {
    let t = tenant().await;
    let other = tenant().await;
    let bot = bot_config(t).await;
    let other_bot = bot_config(other).await;
    let old = channel(t, bot, "lw-old", 3, true).await;
    let new = channel(t, bot, "lw-new", 1, true).await;
    channel(t, bot, "lw-left", 0, false).await;
    let others = channel(other, other_bot, "lw-other", 0, true).await;
    let app = app().await;

    let got = call(&app, "GET", CHANNELS, Some(t), None).await;
    assert_eq!(got.status, 200);
    let rows = got.json();
    let ids: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![new.to_string(), old.to_string()]);
    let first = &rows[0];
    assert_eq!(
        keys(first),
        vec![
            "active",
            "bot_config_id",
            "channel_id",
            "channel_type",
            "created_at",
            "id",
            "joined_at",
            "tenant_id",
            "title",
            "updated_at"
        ]
    );
    assert_eq!(first["bot_config_id"], json!(bot.to_string()));
    assert_eq!(first["channel_id"], json!("lw-new"));
    assert_eq!(first["title"], json!("room"));
    assert_eq!(first["channel_type"], json!("group"));
    assert_eq!(first["active"], json!(true));

    // 別 tenant の行は消せない (204 だが残る)
    let got = call(
        &app,
        "DELETE",
        &format!("{CHANNELS}/{others}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    let got = call(&app, "GET", CHANNELS, Some(other), None).await;
    assert_eq!(got.json().as_array().unwrap().len(), 1);

    let got = call(&app, "DELETE", &format!("{CHANNELS}/{new}"), Some(t), None).await;
    assert_eq!((got.status, got.body.len()), (204, 0));
    let got = call(&app, "GET", CHANNELS, Some(t), None).await;
    assert_eq!(got.json()[0]["id"], json!(old.to_string()));
    assert_eq!(got.json().as_array().unwrap().len(), 1);
    assert!(app.logs().is_empty());
}

/// test-send は lineworks-worker の /messages に行の bot と LINE WORKS の channel_id を X-Tenant-ID 付きで送る。
/// 無い行・別 tenant の行は 404 で呼ばない。lineworks-worker の断りは status と語だけを写す
#[tokio::test]
async fn channel_test_send() {
    let t = tenant().await;
    let other = tenant().await;
    let bot = bot_config(t).await;
    let id = channel(t, bot, "lw-room", 0, true).await;
    let others = channel(other, bot_config(other).await, "lw-other", 0, true).await;
    let app = app().await;
    let path = format!("{CHANNELS}/{id}/test-send");
    let text = json!({"text": "予約番号は J5JZPEQJ です"});

    let got = call(&app, "POST", &path, Some(t), Some(text.clone())).await;
    assert_eq!((got.status, got.json()), (200, json!({"ok": true})));
    let calls = app.lineworks.calls();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].post);
    assert_eq!(calls[0].path, "/api/internal/lineworks/messages");
    assert_eq!(calls[0].tenant_id, t);
    let sent: Value = serde_json::from_str(calls[0].body.as_deref().unwrap()).unwrap();
    assert_eq!(
        sent,
        json!({
            "bot_config_id": bot.to_string(),
            "to": {"channel_id": "lw-room"},
            "content": {"type": "text", "text": "予約番号は J5JZPEQJ です"},
        })
    );

    for missing in [Uuid::new_v4(), others] {
        let got = call(
            &app,
            "POST",
            &format!("{CHANNELS}/{missing}/test-send"),
            Some(t),
            Some(text.clone()),
        )
        .await;
        assert_eq!(
            (got.status, got.json()),
            (404, json!({"error": "channel_not_found"}))
        );
    }
    assert_eq!(app.lineworks.calls().len(), 1);
    assert!(app.logs().is_empty());

    // lineworks-worker の断り: status と語は写し、自由文は落とす
    let cases: [(u16, &[u8], u16, Value); 4] = [
        (
            404,
            br#"{"error":"bot_config_not_found"}"#,
            404,
            json!({"error": "bot_config_not_found"}),
        ),
        (
            502,
            br#"{"error":"upstream_error","message":"Send failed: HTTP 500"}"#,
            502,
            json!({"error": "upstream_error"}),
        ),
        (
            500,
            br#"{"error":"internal_error","message":"private_key_invalid"}"#,
            500,
            json!({"error": "internal_error", "message": "private_key_invalid"}),
        ),
        // 本文が JSON でない・status が失敗の範囲でない → 502 upstream_error
        (302, b"<html>", 502, json!({"error": "upstream_error"})),
    ];
    for (status, body, want_status, want) in cases {
        app.logs.lock().unwrap().clear();
        app.lineworks.reply(status, body);
        let got = call(&app, "POST", &path, Some(t), Some(text.clone())).await;
        assert_eq!((got.status, got.json()), (want_status, want));
        assert_eq!(
            app.logs(),
            vec![format!(
                "notify: lineworks test send failed: lineworks (status {status})"
            )]
        );
    }

    // 届かない → 502、ログは label だけ
    app.logs.lock().unwrap().clear();
    app.lineworks.unreachable("fetch");
    let got = call(&app, "POST", &path, Some(t), Some(text)).await;
    assert_eq!(
        (got.status, got.json()),
        (502, json!({"error": "upstream_error"}))
    );
    assert_eq!(
        app.logs(),
        vec!["notify: lineworks test send failed: lineworks (fetch)".to_owned()]
    );
}

/// users は lineworks-worker のメンバーに、自 tenant の宛先に在るかを足す (別 tenant の宛先は数えない)
#[tokio::test]
async fn lineworks_users() {
    let t = tenant().await;
    let other = tenant().await;
    let app = app().await;
    for (tenant, name, lw) in [(t, "山田", "u-yamada"), (other, "伊藤", "u-ito")] {
        let got = call(
            &app,
            "POST",
            "/api/notify/recipients",
            Some(tenant),
            Some(json!({"name": name, "provider": "lineworks", "lineworks_user_id": lw})),
        )
        .await;
        assert_eq!(got.status, 201);
    }
    app.lineworks.reply(
        200,
        r#"[{"user_id":"u-yamada","user_name":"山田 太郎","email":"y@example.com"},
             {"user_id":"u-ito","user_name":null,"email":null},
             {"user_id":"u-new","user_name":"新人","email":"n@example.com"}]"#
            .as_bytes(),
    );

    let got = call(&app, "GET", USERS, Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(
        got.json(),
        json!([
            {"user_id": "u-yamada", "user_name": "山田 太郎", "email": "y@example.com", "already_registered": true},
            {"user_id": "u-ito", "user_name": null, "email": null, "already_registered": false},
            {"user_id": "u-new", "user_name": "新人", "email": "n@example.com", "already_registered": false},
        ])
    );
    let calls = app.lineworks.calls();
    assert_eq!(calls.len(), 1);
    assert!(!calls[0].post);
    assert_eq!(calls[0].path, "/api/internal/lineworks/users");
    assert_eq!((calls[0].tenant_id, calls[0].body.as_deref()), (t, None));
    assert!(app.logs().is_empty());

    // 権限が無い → 403 missing_scope (rust と同じ固定の文)
    app.lineworks.reply(
        403,
        br#"{"error":"missing_scope","scope":"directory.read","message":"x"}"#,
    );
    let got = call(&app, "GET", USERS, Some(t), None).await;
    assert_eq!(
        (got.status, got.json()),
        (
            403,
            json!({
                "error": "missing_scope",
                "scope": "directory.read",
                "message": "LINE WORKS Developer Console で directory.read scope を追加してください",
            })
        )
    );
    // scope が語でなければ写さない
    app.lineworks
        .reply(403, br#"{"error":"missing_scope","scope":"a b"}"#);
    let got = call(&app, "GET", USERS, Some(t), None).await;
    assert_eq!(
        (got.status, got.json()),
        (403, json!({"error": "missing_scope"}))
    );
    // error が語でない → upstream_error
    app.lineworks.reply(
        502,
        br#"{"error":"Bad Gateway: <html>","message":"Bad Gateway"}"#,
    );
    let got = call(&app, "GET", USERS, Some(t), None).await;
    assert_eq!(
        (got.status, got.json()),
        (502, json!({"error": "upstream_error"}))
    );

    // 2xx でも本文が読めない → 502
    app.logs.lock().unwrap().clear();
    app.lineworks.reply(200, b"not json");
    let got = call(&app, "GET", USERS, Some(t), None).await;
    assert_eq!(
        (got.status, got.json()),
        (502, json!({"error": "upstream_error"}))
    );
    assert_eq!(
        app.logs(),
        vec!["notify: list lineworks users failed: lineworks (body)".to_owned()]
    );
}

/// login-activity は lineworks-worker の応答の中継 (days を渡す。既定 3)
#[tokio::test]
async fn lineworks_login_activity() {
    let t = tenant().await;
    let app = app().await;
    let entries = json!([
        {"user_id": "u1", "user_name": null, "email": null, "last_login_at": null,
         "days_since_login": null, "stale": true, "last_login_source": null},
        {"user_id": "u2", "user_name": "B", "email": "b@example.com", "last_login_at": "2026-10-01T00:00:00+00:00",
         "days_since_login": 9, "stale": true, "last_login_source": "auth"},
    ]);
    app.lineworks.reply(200, entries.to_string().as_bytes());

    let got = call(
        &app,
        "GET",
        &format!("{LOGIN_ACTIVITY}?days=7"),
        Some(t),
        None,
    )
    .await;
    assert_eq!((got.status, got.json()), (200, entries.clone()));
    let got = call(&app, "GET", LOGIN_ACTIVITY, Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, entries));
    let paths: Vec<(String, Uuid)> = app
        .lineworks
        .calls()
        .into_iter()
        .map(|c| (c.path, c.tenant_id))
        .collect();
    assert_eq!(
        paths,
        vec![
            (
                "/api/internal/lineworks/login-activity?days=7".to_owned(),
                t
            ),
            (
                "/api/internal/lineworks/login-activity?days=3".to_owned(),
                t
            ),
        ]
    );

    // days が数でない → 400 (rust と同じ Query の拒否)。lineworks-worker は呼ばない
    let got = call(
        &app,
        "GET",
        &format!("{LOGIN_ACTIVITY}?days=x"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 400);
    assert_eq!(app.lineworks.calls().len(), 2);

    app.lineworks.reply(
        403,
        br#"{"error":"missing_scope","scope":"audit.read","message":"x"}"#,
    );
    let got = call(&app, "GET", LOGIN_ACTIVITY, Some(t), None).await;
    assert_eq!(got.status, 403);
    assert_eq!(got.json()["scope"], json!("audit.read"));
    app.lineworks.reply(
        502,
        br#"{"error":"csv_columns_not_recognized","message":"audit csv columns not recognized"}"#,
    );
    let got = call(&app, "GET", LOGIN_ACTIVITY, Some(t), None).await;
    assert_eq!(
        (got.status, got.json()),
        (502, json!({"error": "csv_columns_not_recognized"}))
    );
    assert_eq!(
        app.logs(),
        vec![
            "notify: lineworks login activity failed: lineworks (status 403)".to_owned(),
            "notify: lineworks login activity failed: lineworks (status 502)".to_owned(),
        ]
    );
}

/// X-Tenant-ID が無い → 401 (DB にも lineworks-worker にも触らない)
#[tokio::test]
async fn lineworks_routes_require_tenant() {
    let app = app().await;
    let id = Uuid::new_v4();
    for (method, path) in [
        ("GET", LINE_CONFIG.to_owned()),
        ("POST", LINE_CONFIG.to_owned()),
        ("DELETE", LINE_CONFIG.to_owned()),
        ("GET", CHANNELS.to_owned()),
        ("DELETE", format!("{CHANNELS}/{id}")),
        ("POST", format!("{CHANNELS}/{id}/test-send")),
        ("GET", USERS.to_owned()),
        ("GET", LOGIN_ACTIVITY.to_owned()),
    ] {
        let got = call(&app, method, &path, None, None).await;
        assert_eq!(got.status, 401, "{method} {path}");
    }
    assert!(app.lineworks.calls().is_empty());
}

/// method・path・body・500 の `message` (`None` = 本文なし)・ログの段
type Case = (
    &'static str,
    String,
    Option<Value>,
    Option<&'static str>,
    &'static str,
);

/// DB の失敗 → 500 (line-config は本文なし、channel・users は rust と同じ語)。ログは固定の語と kind だけ
#[tokio::test]
async fn lineworks_db_failure_is_500_with_fixed_logs() {
    let t = tenant().await;
    let app = broken_app().await;
    app.lineworks.reply(200, b"[]");
    let id = Uuid::new_v4();
    let cases: [Case; 7] = [
        ("GET", LINE_CONFIG.to_owned(), None, None, "get line config"),
        (
            "POST",
            LINE_CONFIG.to_owned(),
            Some(line_config_body("x")),
            None,
            "upsert line config",
        ),
        (
            "DELETE",
            LINE_CONFIG.to_owned(),
            None,
            None,
            "delete line config",
        ),
        (
            "GET",
            CHANNELS.to_owned(),
            None,
            Some("list_failed"),
            "list lineworks channels",
        ),
        (
            "DELETE",
            format!("{CHANNELS}/{id}"),
            None,
            Some("delete_failed"),
            "delete lineworks channel",
        ),
        (
            "POST",
            format!("{CHANNELS}/{id}/test-send"),
            Some(json!({"text": "t"})),
            Some("get_failed"),
            "get lineworks channel",
        ),
        (
            "GET",
            USERS.to_owned(),
            None,
            Some("list_recipients_failed"),
            "list recipients",
        ),
    ];
    for (method, path, body, msg, what) in cases {
        app.logs.lock().unwrap().clear();
        let got = call(&app, method, &path, Some(t), body).await;
        assert_eq!(got.status, 500, "{method} {path}");
        match msg {
            Some(m) => assert_eq!(got.json(), json!({"error": "internal_error", "message": m})),
            None => assert!(got.body.is_empty()),
        }
        let logs = app.logs();
        assert_eq!(logs.len(), 1, "{method} {path}");
        assert!(
            logs[0].starts_with(&format!("notify: {what} failed: db (")),
            "{}",
            logs[0]
        );
        assert!(!logs[0].contains(&t.to_string()));
        assert!(!logs[0].contains(&id.to_string()));
    }
    // test-send は行を読めなかったので lineworks-worker を呼ばない (users の 1 回だけ)
    assert_eq!(app.lineworks.calls().len(), 1);
}
