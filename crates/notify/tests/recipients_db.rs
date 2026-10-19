//! 宛先・グループの口を、実 DB (`alc_api_rt` で接続) で通す。
//! 接続先は `NOTIFY_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。
//! 期待値は rust-alc-api の `crates/alc-notify/src/recipients.rs`・`groups.rs` と repo 層の SQL の意味。

mod support;

use notify::email_routing::ErMethod;
use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const RECIPIENTS: &str = "/api/notify/recipients";
const GROUPS: &str = "/api/notify/groups";

fn recipient_body(name: &str, lw: &str) -> Value {
    json!({"name": name, "provider": "lineworks", "lineworks_user_id": lw})
}

async fn create_recipient(app: &App, t: Uuid, name: &str, lw: &str) -> Value {
    let got = call(
        app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(recipient_body(name, lw)),
    )
    .await;
    assert_eq!(got.status, 201);
    got.json()
}

async fn create_group(app: &App, t: Uuid, name: &str) -> Value {
    let got = call(
        app,
        "POST",
        GROUPS,
        Some(t),
        Some(json!({"name": name, "description": "説明"})),
    )
    .await;
    assert_eq!(got.status, 201);
    got.json()
}

fn id(v: &Value) -> String {
    v["id"].as_str().unwrap().to_owned()
}

fn keys(v: &Value) -> Vec<&str> {
    let mut k: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    k.sort();
    k
}

fn names(v: &Value) -> Vec<&str> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect()
}

/// 作る → 一覧 (名前の順) → 1 件 → 直す (指定の無い項目は元のまま) → 消す。JSON の形は rust の `NotifyRecipient`
#[tokio::test]
async fn recipient_crud() {
    let t = tenant().await;
    let app = app().await;

    let got = call(
        &app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(json!({
            "name": "山田", "provider": "lineworks", "lineworks_user_id": "lw-yamada",
            "phone_number": "000", "email": "y@example.com"
        })),
    )
    .await;
    assert_eq!(got.status, 201);
    let yamada = got.json();
    assert_eq!(
        keys(&yamada),
        vec![
            "created_at",
            "email",
            "enabled",
            "id",
            "line_user_id",
            "lineworks_user_id",
            "name",
            "phone_number",
            "provider",
            "tenant_id",
            "updated_at"
        ]
    );
    assert_eq!(yamada["tenant_id"], json!(t.to_string()));
    assert_eq!(yamada["enabled"], json!(true));
    assert_eq!(yamada["line_user_id"], Value::Null);
    // chrono の serde と同じ RFC 3339 (UTC は Z)
    let created = yamada["created_at"].as_str().unwrap();
    assert!(created.ends_with('Z'), "{created}");
    chrono::DateTime::parse_from_rfc3339(created).unwrap();
    let line = call(
        &app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(json!({"name": "伊藤", "provider": "line", "line_user_id": "U1"})),
    )
    .await
    .json();

    let got = call(&app, "GET", RECIPIENTS, Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(names(&got.json()), vec!["伊藤", "山田"]);

    let path = format!("{RECIPIENTS}/{}", id(&yamada));
    let got = call(&app, "GET", &path, Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, yamada.clone()));

    let got = call(
        &app,
        "PUT",
        &path,
        Some(t),
        Some(json!({"name": "山田太郎", "enabled": false})),
    )
    .await;
    assert_eq!(got.status, 200);
    let updated = got.json();
    assert_eq!(updated["name"], json!("山田太郎"));
    assert_eq!(updated["enabled"], json!(false));
    assert_eq!(updated["email"], json!("y@example.com"));
    assert_eq!(updated["lineworks_user_id"], json!("lw-yamada"));
    // null は「指定なし」(COALESCE。消せない。rust と同じ)
    let got = call(&app, "PUT", &path, Some(t), Some(json!({"email": null}))).await;
    assert_eq!(got.json()["email"], json!("y@example.com"));

    let got = call(&app, "DELETE", &path, Some(t), None).await;
    assert_eq!((got.status, got.body.len()), (204, 0));
    let got = call(&app, "GET", &path, Some(t), None).await;
    assert_eq!((got.status, got.body.len()), (404, 0));
    // 無い行を消しても 204 (rust と同じ)
    let got = call(&app, "DELETE", &path, Some(t), None).await;
    assert_eq!(got.status, 204);
    let got = call(&app, "GET", RECIPIENTS, Some(t), None).await;
    assert_eq!(got.json(), json!([line]));
    assert!(app.logs.lock().unwrap().is_empty());
}

/// 無い行を直す → 500 (rust の `fetch_one` と同じ)。provider が許す語の外 → 400 `invalid_provider` (DB の CHECK に届かない)。
/// 500 は本文なし、ログは固定の語と kind だけ
#[tokio::test]
async fn recipient_update_missing_and_constraint() {
    let t = tenant().await;
    let app = app().await;
    let missing = Uuid::new_v4();
    let got = call(
        &app,
        "PUT",
        &format!("{RECIPIENTS}/{missing}"),
        Some(t),
        Some(json!({"name": "x"})),
    )
    .await;
    assert_eq!((got.status, got.body.len()), (500, 0));
    // provider の語が許す語の外 (検査で 400。DB には届かない)
    let got = call(
        &app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(json!({"name": "x", "provider": "mail", "lineworks_user_id": "lw-x"})),
    )
    .await;
    assert_eq!(got.status, 400);
    assert_eq!(got.json(), json!({"error": "invalid_provider"}));
    let logs = app.logs.lock().unwrap().clone();
    assert_eq!(
        logs,
        vec!["notify: update recipient failed: db (other)".to_owned()]
    );
    for l in &logs {
        assert!(!l.contains(&t.to_string()) && !l.contains(&missing.to_string()));
    }
}

/// bulk: lineworks_user_id で既存と突き合わせて直す / 作る / 飛ばす。グループへの追加は best-effort
#[tokio::test]
async fn recipient_bulk_upsert() {
    let t = tenant().await;
    let other = tenant().await;
    let app = app().await;
    let existing = create_recipient(&app, t, "旧名", "lw-1").await;
    // line の宛先 (lineworks_user_id 無し) は対応表に入らない
    call(
        &app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(json!({"name": "LINE", "provider": "line", "line_user_id": "U9"})),
    )
    .await;
    let g = create_group(&app, t, "班").await;
    let other_group = create_group(&app, other, "別").await;

    let got = call(
        &app,
        "POST",
        &format!("{RECIPIENTS}/bulk"),
        Some(t),
        Some(json!({
            "recipients": [
                {"name": "新名", "provider": "lineworks", "lineworks_user_id": "lw-1", "email": "a@example.com"},
                {"name": "追加", "provider": "lineworks", "lineworks_user_id": "lw-2"},
                {"name": "IDなし", "provider": "line", "line_user_id": "U2"},
                {"name": "重複", "provider": "lineworks", "lineworks_user_id": "lw-2"}
            ],
            "group_ids": [id(&g), id(&other_group)]
        })),
    )
    .await;
    assert_eq!(got.status, 200);
    let body = got.json();
    assert_eq!(
        keys(&body),
        vec!["created", "skipped", "touched_recipient_ids", "updated"]
    );
    assert_eq!(body["created"], json!(1));
    assert_eq!(body["updated"], json!(1));
    assert_eq!(
        body["skipped"],
        json!([
            {"index": 2, "reason": "missing_lineworks_user_id"},
            {"index": 3, "reason": "db_error"}
        ])
    );
    let touched = body["touched_recipient_ids"].as_array().unwrap().clone();
    assert_eq!(touched.len(), 2);
    assert_eq!(touched[0], existing["id"]);

    let got = call(
        &app,
        "GET",
        &format!("{RECIPIENTS}/{}", id(&existing)),
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(got["name"], json!("新名"));
    assert_eq!(got["email"], json!("a@example.com"));

    // 自分の tenant のグループには入り、別 tenant のグループへの追加は断られてログだけ
    let members = call(
        &app,
        "GET",
        &format!("{GROUPS}/{}/members", id(&g)),
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(names(&members), vec!["新名", "追加"]);
    assert_eq!(
        *app.logs.lock().unwrap(),
        vec![
            "notify: bulk upsert recipient failed: db (23505)".to_owned(),
            "notify: bulk add members failed: db (42501)".to_owned(),
        ]
    );

    // group_ids は省ける。空の recipients は全部 0
    let got = call(
        &app,
        "POST",
        &format!("{RECIPIENTS}/bulk"),
        Some(t),
        Some(json!({"recipients": []})),
    )
    .await;
    assert_eq!(
        got.json(),
        json!({"created": 0, "updated": 0, "skipped": [], "touched_recipient_ids": []})
    );
}

/// グループの CRUD とメンバー (追加は冪等・空の追加は 204・外す)。JSON の形は rust の `NotifyGroup`
#[tokio::test]
async fn group_crud_and_members() {
    let t = tenant().await;
    let app = app().await;
    let b = create_group(&app, t, "B班").await;
    assert_eq!(
        keys(&b),
        vec![
            "created_at",
            "description",
            "id",
            "name",
            "tenant_id",
            "updated_at"
        ]
    );
    assert_eq!(b["description"], json!("説明"));
    let got = call(&app, "POST", GROUPS, Some(t), Some(json!({"name": "A班"}))).await;
    assert_eq!(got.status, 201);
    assert_eq!(got.json()["description"], Value::Null);

    let got = call(&app, "GET", GROUPS, Some(t), None).await;
    assert_eq!(names(&got.json()), vec!["A班", "B班"]);

    let path = format!("{GROUPS}/{}", id(&b));
    let got = call(&app, "GET", &path, Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, b.clone()));
    let got = call(&app, "PUT", &path, Some(t), Some(json!({"name": "C班"}))).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["name"], json!("C班"));
    assert_eq!(got.json()["description"], json!("説明"));

    let r1 = create_recipient(&app, t, "鈴木", "lw-s").await;
    let r2 = create_recipient(&app, t, "佐藤", "lw-a").await;
    let members = format!("{path}/members");
    for ids in [json!([id(&r1), id(&r2)]), json!([id(&r1)]), json!([])] {
        let got = call(
            &app,
            "POST",
            &members,
            Some(t),
            Some(json!({"recipient_ids": ids})),
        )
        .await;
        assert_eq!((got.status, got.body.len()), (204, 0));
    }
    let got = call(&app, "GET", &members, Some(t), None).await;
    assert_eq!(got.status, 200);
    let list = got.json();
    assert_eq!(names(&list), vec!["佐藤", "鈴木"]);
    assert_eq!(list[0], r2);

    let got = call(
        &app,
        "DELETE",
        &format!("{members}/{}", id(&r2)),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    let got = call(&app, "GET", &members, Some(t), None).await;
    assert_eq!(names(&got.json()), vec!["鈴木"]);

    // 宛先を消すとメンバーからも消える (ON DELETE CASCADE)
    call(
        &app,
        "DELETE",
        &format!("{RECIPIENTS}/{}", id(&r1)),
        Some(t),
        None,
    )
    .await;
    let got = call(&app, "GET", &members, Some(t), None).await;
    assert_eq!(got.json(), json!([]));

    let got = call(&app, "DELETE", &path, Some(t), None).await;
    assert_eq!(got.status, 204);
    let got = call(&app, "GET", &path, Some(t), None).await;
    assert_eq!(got.status, 404);
    // 無いグループを直す → 500 (rust と同じ)。同じ名前を作る → 500 (UNIQUE)
    let got = call(&app, "PUT", &path, Some(t), Some(json!({"name": "x"}))).await;
    assert_eq!((got.status, got.body.len()), (500, 0));
    let got = call(&app, "POST", GROUPS, Some(t), Some(json!({"name": "A班"}))).await;
    assert_eq!(got.status, 500);
    assert_eq!(
        *app.logs.lock().unwrap(),
        vec![
            "notify: update group failed: db (other)".to_owned(),
            "notify: create group failed: db (23505)".to_owned(),
        ]
    );
}

/// 別 tenant の行は見えず、直せず、消せない (RLS と WHERE tenant_id)
#[tokio::test]
async fn other_tenant_is_invisible() {
    let t = tenant().await;
    let other = tenant().await;
    let app = app().await;
    let r = create_recipient(&app, t, "本人", "lw-me").await;
    let g = create_group(&app, t, "班").await;
    let members = format!("{GROUPS}/{}/members", id(&g));
    call(
        &app,
        "POST",
        &members,
        Some(t),
        Some(json!({"recipient_ids": [id(&r)]})),
    )
    .await;
    let r_path = format!("{RECIPIENTS}/{}", id(&r));
    let g_path = format!("{GROUPS}/{}", id(&g));

    let got = call(&app, "GET", RECIPIENTS, Some(other), None).await;
    assert_eq!((got.status, got.json()), (200, json!([])));
    let got = call(&app, "GET", GROUPS, Some(other), None).await;
    assert_eq!(got.json(), json!([]));
    assert_eq!(
        call(&app, "GET", &r_path, Some(other), None).await.status,
        404
    );
    assert_eq!(
        call(&app, "GET", &g_path, Some(other), None).await.status,
        404
    );
    let got = call(&app, "GET", &members, Some(other), None).await;
    assert_eq!(got.json(), json!([]));
    let got = call(
        &app,
        "PUT",
        &r_path,
        Some(other),
        Some(json!({"name": "乗っ取り"})),
    )
    .await;
    assert_eq!(got.status, 500);
    let got = call(
        &app,
        "PUT",
        &g_path,
        Some(other),
        Some(json!({"name": "乗っ取り"})),
    )
    .await;
    assert_eq!(got.status, 500);
    // 消す・外す・足すは 204 / 500 でも、行は残る
    assert_eq!(
        call(&app, "DELETE", &r_path, Some(other), None)
            .await
            .status,
        204
    );
    assert_eq!(
        call(&app, "DELETE", &g_path, Some(other), None)
            .await
            .status,
        204
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("{members}/{}", id(&r)),
            Some(other),
            None
        )
        .await
        .status,
        204
    );
    let got = call(
        &app,
        "POST",
        &members,
        Some(other),
        Some(json!({"recipient_ids": [id(&r)]})),
    )
    .await;
    // other から見て r は無い宛先 → 404 (group の RLS より先に宛先の tenant で断る)
    assert_eq!(got.status, 404);
    // 別 tenant の bulk は、t の lineworks_user_id と突き合わない (別の行を作る)
    let got = call(
        &app,
        "POST",
        &format!("{RECIPIENTS}/bulk"),
        Some(other),
        Some(json!({"recipients": [recipient_body("他社", "lw-me")]})),
    )
    .await;
    let body = got.json();
    assert_eq!((&body["created"], &body["updated"]), (&json!(1), &json!(0)));

    let got = call(&app, "GET", &r_path, Some(t), None).await;
    assert_eq!(got.json()["name"], json!("本人"));
    let got = call(&app, "GET", &g_path, Some(t), None).await;
    assert_eq!(got.json()["name"], json!("班"));
    let got = call(&app, "GET", &members, Some(t), None).await;
    assert_eq!(names(&got.json()), vec!["本人"]);
    let got = call(&app, "GET", RECIPIENTS, Some(t), None).await;
    assert_eq!(names(&got.json()), vec!["本人"]);
}

async fn member_rows(group: &Value) -> i64 {
    let g: Uuid = id(group).parse().unwrap();
    admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.notify_recipient_groups WHERE group_id = $1",
            &[&g],
        )
        .await
        .unwrap()
        .get(0)
}

/// 別 tenant の宛先・存在しない宛先は自分のグループに足せない: 404 (本文なし・ログなし) で、1 つでも混ざれば何も足さない。
/// rust はここを検査せず、RLS (group の tenant しか見ない) と FK (RLS を通らない) を抜けて別 tenant の宛先が入った
#[tokio::test]
async fn add_members_rejects_other_tenant_recipient() {
    let t = tenant().await;
    let other = tenant().await;
    let app = app().await;
    let mine = create_recipient(&app, t, "自社", "lw-mine").await;
    let theirs = create_recipient(&app, other, "他社", "lw-theirs").await;
    let g = create_group(&app, t, "班").await;
    let members = format!("{GROUPS}/{}/members", id(&g));

    for ids in [
        json!([id(&theirs)]),
        json!([id(&mine), id(&theirs)]),
        json!([id(&mine), Uuid::new_v4()]),
    ] {
        let got = call(
            &app,
            "POST",
            &members,
            Some(t),
            Some(json!({"recipient_ids": ids})),
        )
        .await;
        assert_eq!((got.status, got.body.len()), (404, 0));
        assert_eq!(member_rows(&g).await, 0, "行が増えた");
    }
    // 同じ id の重複は 1 つとして数える
    let got = call(
        &app,
        "POST",
        &members,
        Some(t),
        Some(json!({"recipient_ids": [id(&mine), id(&mine)]})),
    )
    .await;
    assert_eq!(got.status, 204);
    assert_eq!(member_rows(&g).await, 1);
    let got = call(&app, "GET", &members, Some(t), None).await;
    assert_eq!(names(&got.json()), vec!["自社"]);
    assert!(app.logs.lock().unwrap().is_empty());
}

/// X-Tenant-ID が無い・UUID でない → 401。読めない body・method・path は axum の既定 (rust と同じ)
#[tokio::test]
async fn tenant_header_and_rejections() {
    let t = tenant().await;
    let app = app().await;
    let paths = [
        ("GET", RECIPIENTS.to_owned()),
        ("POST", format!("{RECIPIENTS}/bulk")),
        ("GET", format!("{RECIPIENTS}/{}", Uuid::new_v4())),
        ("GET", GROUPS.to_owned()),
        ("GET", format!("{GROUPS}/{}/members", Uuid::new_v4())),
        (
            "DELETE",
            format!("{GROUPS}/{}/members/{}", Uuid::new_v4(), Uuid::new_v4()),
        ),
    ];
    for (method, path) in &paths {
        let got = call(&app, method, path, None, None).await;
        assert_eq!(got.status, 401, "{method} {path}");
    }
    let got = {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;
        app.router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(RECIPIENTS)
                    .header("X-Tenant-ID", "not-a-uuid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    };
    assert_eq!(got.as_u16(), 401);

    // 必須の項目が無い → 422、id が UUID でない → 400、bulk に GET → 405、知らない path → 404
    let got = call(
        &app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(json!({"name": "x"})),
    )
    .await;
    assert_eq!(got.status, 422);
    let got = call(&app, "POST", GROUPS, Some(t), Some(json!({}))).await;
    assert_eq!(got.status, 422);
    let got = call(
        &app,
        "POST",
        &format!("{GROUPS}/{}/members", Uuid::new_v4()),
        Some(t),
        Some(json!({})),
    )
    .await;
    assert_eq!(got.status, 422);
    let got = call(&app, "GET", &format!("{RECIPIENTS}/x"), Some(t), None).await;
    assert_eq!(got.status, 400);
    let got = call(&app, "GET", &format!("{RECIPIENTS}/bulk"), Some(t), None).await;
    assert_eq!(got.status, 405);
    let got = call(&app, "GET", "/api/notify/nothing", Some(t), None).await;
    assert_eq!(got.status, 404);
    assert!(app.logs.lock().unwrap().is_empty());
}

/// 接続が切れた DB: どの口も 500 (本文なし)。ログは固定の語と kind だけ
#[tokio::test]
async fn db_failure_is_500_with_fixed_logs() {
    let t = tenant().await;
    let app = broken_app().await;
    let r = Uuid::new_v4();
    let g = Uuid::new_v4();
    let body_r = recipient_body("x", "lw-x");
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        ("GET", RECIPIENTS.into(), None, "list recipients"),
        (
            "POST",
            RECIPIENTS.into(),
            Some(body_r.clone()),
            "create recipient",
        ),
        (
            "POST",
            format!("{RECIPIENTS}/bulk"),
            Some(json!({"recipients": [body_r]})),
            "bulk list recipients",
        ),
        ("GET", format!("{RECIPIENTS}/{r}"), None, "get recipient"),
        (
            "PUT",
            format!("{RECIPIENTS}/{r}"),
            Some(json!({})),
            "update recipient",
        ),
        (
            "DELETE",
            format!("{RECIPIENTS}/{r}"),
            None,
            "delete recipient",
        ),
        ("GET", GROUPS.into(), None, "list groups"),
        (
            "POST",
            GROUPS.into(),
            Some(json!({"name": "x"})),
            "create group",
        ),
        ("GET", format!("{GROUPS}/{g}"), None, "get group"),
        (
            "PUT",
            format!("{GROUPS}/{g}"),
            Some(json!({})),
            "update group",
        ),
        ("DELETE", format!("{GROUPS}/{g}"), None, "delete group"),
        (
            "GET",
            format!("{GROUPS}/{g}/members"),
            None,
            "list group members",
        ),
        (
            "POST",
            format!("{GROUPS}/{g}/members"),
            Some(json!({"recipient_ids": [r]})),
            "add group members",
        ),
        (
            "DELETE",
            format!("{GROUPS}/{g}/members/{r}"),
            None,
            "remove group member",
        ),
    ];
    for (method, path, body, what) in cases {
        app.logs.lock().unwrap().clear();
        let got = call(&app, method, &path, Some(t), body).await;
        assert_eq!((got.status, got.body.len()), (500, 0), "{method} {path}");
        let logs = app.logs.lock().unwrap().clone();
        assert_eq!(logs.len(), 1, "{method} {path}");
        assert!(
            logs[0].starts_with(&format!("notify: {what} failed: db (")),
            "{}",
            logs[0]
        );
        for id in [t, r, g] {
            assert!(!logs[0].contains(&id.to_string()));
        }
    }
}

// ---- Cloudflare Email Routing の送り先 (Refs ippoan/alc-app#436) ----
// Cloudflare は MockEmailRouting。送り先を作るのは admin だけ・メールアドレスが新しく入った / 変わったときだけ・
// 失敗しても保存の応答は変えない・消さない。email-status は 3 状態。

const EMAIL_STATUS: &str = "/api/notify/recipients/email-status";

/// auth-worker の identity のヘッダーで role だけ変える
fn as_role(t: Uuid, role: &str) -> Vec<(&'static str, String)> {
    let mut h = identity(t, Uuid::new_v4());
    h.iter_mut().find(|(k, _)| *k == "X-User-Role").unwrap().1 = role.to_owned();
    h
}

fn with_email(name: &str, lw: &str, email: &str) -> Value {
    json!({"name": name, "provider": "lineworks", "lineworks_user_id": lw, "email": email})
}

/// 送り先を作るのは admin の POST だけ。Cloudflare に渡すアドレスは前後の空白を削って小文字、DB の値はそのまま
#[tokio::test]
async fn email_routing_created_on_admin_post_only() {
    let t = tenant().await;
    let app = app().await;

    let admin = identity(t, Uuid::new_v4());
    let got = call_with(
        &app,
        "POST",
        RECIPIENTS,
        &admin,
        Some(with_email("山田", "lw-1", "  Yamada@Example.TEST ")),
    )
    .await;
    assert_eq!(got.status, 201);
    assert_eq!(got.json()["email"], "  Yamada@Example.TEST ");
    assert_eq!(app.email.created_emails(), vec!["yamada@example.test"]);
    let call0 = &app.email.calls()[0];
    assert_eq!(call0.method, ErMethod::Post);
    assert_eq!(call0.path, "/email/routing/addresses");

    // admin でない (role が違う・identity のヘッダーが無い) POST は、宛先は保存するが送り先は作らない
    for (n, headers) in [as_role(t, "member"), vec![("X-Tenant-ID", t.to_string())]]
        .into_iter()
        .enumerate()
    {
        let got = call_with(
            &app,
            "POST",
            RECIPIENTS,
            &headers,
            Some(with_email("佐藤", &format!("lw-s{n}"), "sato@example.test")),
        )
        .await;
        assert_eq!(got.status, 201);
    }
    assert_eq!(app.email.created_emails(), vec!["yamada@example.test"]);

    // メールアドレスが無い・空の POST でも作らない
    for body in [
        recipient_body("鈴木", "lw-3"),
        with_email("高橋", "lw-4", "   "),
    ] {
        let got = call_with(&app, "POST", RECIPIENTS, &admin, Some(body)).await;
        assert_eq!(got.status, 201);
    }
    assert_eq!(app.email.calls().len(), 1);
}

/// PUT: アドレスが変わらなければ(大文字小文字・空白の違いは同じとみなす)作らない。変わったら作る。送らなければ作らない。
/// admin でなければ作らない。行が無ければ 500 (rust と同じ)
#[tokio::test]
async fn email_routing_put_only_when_changed() {
    let t = tenant().await;
    let app = app().await;
    let admin = identity(t, Uuid::new_v4());
    let created = call(
        &app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(with_email("山田", "lw-1", "yamada@example.test")),
    )
    .await
    .json();
    let uri = format!("{RECIPIENTS}/{}", id(&created));
    assert!(app.email.calls().is_empty());

    for body in [
        json!({"email": " YAMADA@example.test"}),
        json!({"name": "山田 太郎"}),
    ] {
        let got = call_with(&app, "PUT", &uri, &admin, Some(body)).await;
        assert_eq!(got.status, 200);
    }
    assert!(app.email.calls().is_empty());

    // 変わった PUT でも admin でなければ作らない
    let got = call_with(
        &app,
        "PUT",
        &uri,
        &as_role(t, "member"),
        Some(json!({"email": "other@example.test"})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert!(app.email.calls().is_empty());

    let got = call_with(
        &app,
        "PUT",
        &uri,
        &admin,
        Some(json!({"email": "New@Example.test"})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["email"], "New@Example.test");
    assert_eq!(app.email.created_emails(), vec!["new@example.test"]);

    // 行が無い PUT は 500 のまま (送り先は作らない)
    let got = call_with(
        &app,
        "PUT",
        &format!("{RECIPIENTS}/{}", Uuid::new_v4()),
        &admin,
        Some(json!({"email": "none@example.test"})),
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(app.email.created_emails().len(), 1);
}

/// bulk: 新しい行のアドレスと、既存の行で変わったアドレスだけ作る (同じアドレスは 1 回)。変わらない行・admin でない呼び手は作らない
#[tokio::test]
async fn email_routing_bulk() {
    let t = tenant().await;
    let app = app().await;
    let admin = identity(t, Uuid::new_v4());

    let first = json!({"recipients": [
        with_email("山田", "lw-1", "yamada@example.test"),
        with_email("佐藤", "lw-2", "Sato@example.test"),
        with_email("重複", "lw-3", "sato@example.test"),
        recipient_body("鈴木", "lw-4"),
    ]});
    let got = call_with(
        &app,
        "POST",
        "/api/notify/recipients/bulk",
        &admin,
        Some(first),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["created"], 4);
    assert_eq!(
        app.email.created_emails(),
        vec!["yamada@example.test", "sato@example.test"]
    );

    // 2 回目: 山田は同じ(大文字になっただけ)、佐藤は変わった、鈴木は新しく入った
    let second = json!({"recipients": [
        with_email("山田", "lw-1", "YAMADA@example.test"),
        with_email("佐藤", "lw-2", "sato2@example.test"),
        with_email("鈴木", "lw-4", "suzuki@example.test"),
    ]});
    let got = call_with(
        &app,
        "POST",
        "/api/notify/recipients/bulk",
        &admin,
        Some(second),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["updated"], 3);
    assert_eq!(
        app.email.created_emails(),
        vec![
            "yamada@example.test",
            "sato@example.test",
            "sato2@example.test",
            "suzuki@example.test"
        ]
    );

    // admin でなければ作らない
    let third = json!({"recipients": [with_email("田中", "lw-5", "tanaka@example.test")]});
    let got = call_with(
        &app,
        "POST",
        "/api/notify/recipients/bulk",
        &as_role(t, "member"),
        Some(third),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(app.email.created_emails().len(), 4);
}

/// Cloudflare が失敗しても(2xx 以外・届かない)宛先の保存の応答は変わらない。既に在る応答はエラーにしない。
/// 未設定なら Cloudflare を呼ばない。ログは固定の語だけ(アドレス・上流の本文を出さない)
#[tokio::test]
async fn email_routing_failure_does_not_change_save() {
    let t = tenant().await;
    let app = app().await;
    let admin = identity(t, Uuid::new_v4());
    let post = |n: usize| with_email("山田", &format!("lw-{n}"), &format!("user{n}@example.test"));

    app.email
        .post_reply(500, r#"{"errors":[{"message":"secret upstream text"}]}"#);
    let got = call_with(&app, "POST", RECIPIENTS, &admin, Some(post(1))).await;
    assert_eq!(got.status, 201);
    app.email.post_unreachable("fetch");
    let got = call_with(&app, "POST", RECIPIENTS, &admin, Some(post(2))).await;
    assert_eq!(got.status, 201);
    // 重複 (既に在る) は成功として扱い、ログも出さない
    app.email.post_reply(
        400,
        r#"{"success":false,"errors":[{"code":1,"message":"Destination address already exists"}]}"#,
    );
    let got = call_with(&app, "POST", RECIPIENTS, &admin, Some(post(3))).await;
    assert_eq!(got.status, 201);

    assert_eq!(app.email.calls().len(), 3);
    assert_eq!(
        app.logs(),
        vec![
            "notify: email routing create failed (status 500)",
            "notify: email routing create failed (fetch)",
        ]
    );

    // 未設定: 保存は成功し、Cloudflare は呼ばれず、ログも出ない
    let unconfigured = build_with(
        rt_client().await,
        Options {
            email_routing_configured: false,
            ..Options::default()
        },
    );
    let got = call_with(&unconfigured, "POST", RECIPIENTS, &admin, Some(post(4))).await;
    assert_eq!(got.status, 201);
    assert!(unconfigured.email.calls().is_empty());
    assert!(unconfigured.logs().is_empty());
}

/// 宛先を消しても Cloudflare の送り先は消さない (消す口を持たない。呼び出しが 1 つも出ない)
#[tokio::test]
async fn email_routing_delete_keeps_destination() {
    let t = tenant().await;
    let app = app().await;
    let admin = identity(t, Uuid::new_v4());
    let created = call_with(
        &app,
        "POST",
        RECIPIENTS,
        &admin,
        Some(with_email("山田", "lw-1", "yamada@example.test")),
    )
    .await
    .json();
    assert_eq!(app.email.calls().len(), 1);

    let got = call_with(
        &app,
        "DELETE",
        &format!("{RECIPIENTS}/{}", id(&created)),
        &admin,
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    assert_eq!(app.email.calls().len(), 1);
    assert!(app.email.calls().iter().all(|c| c.method == ErMethod::Post));
}

/// email-status: 送り先が在って確認済み = verified、在るが未確認 = pending、無い = not_registered。
/// アドレスの無い宛先は載せない。大文字小文字・空白は無視して突き合わせ、一覧は全ページ辿る。
/// 偽物の応答は本物の形 (`result_info` は page・per_page・count・total_count だけで total_pages 無し。
/// ページ数は total_count から数える)。`email-status` は id として読まれない。既存の一覧の JSON は変わらない
#[tokio::test]
async fn email_status_three_states() {
    let t = tenant().await;
    let app = app().await;
    let a = create_recipient_with_email(&app, t, "a", "lw-a", "A@example.test").await;
    let b = create_recipient_with_email(&app, t, "b", "lw-b", " b@example.test").await;
    let c = create_recipient_with_email(&app, t, "c", "lw-c", "c@example.test").await;
    create_recipient(&app, t, "d", "lw-d").await;
    let before = call(&app, "GET", RECIPIENTS, Some(t), None).await.json();

    app.email.then_list(
        200,
        r#"{"success":true,"result":[{"id":"1","email":"a@example.test","verified":"2026-01-01T00:00:00Z"}],"result_info":{"page":1,"per_page":50,"count":1,"total_count":51}}"#,
    );
    app.email.then_list(
        200,
        r#"{"success":true,"result":[{"id":"2","email":"B@Example.test","verified":null},{"id":"9","email":"other@example.test","verified":null}],"result_info":{"page":2,"per_page":50,"count":1,"total_count":51}}"#,
    );
    let got = call(&app, "GET", EMAIL_STATUS, Some(t), None).await;
    assert_eq!(got.status, 200);
    let body = got.json();
    assert_eq!(body["configured"], true);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    let status_of = |rid: &str| {
        items.iter().find(|i| i["recipient_id"] == rid).unwrap()["email_status"].clone()
    };
    assert_eq!(status_of(&id(&a)), "verified");
    assert_eq!(status_of(&id(&b)), "pending");
    assert_eq!(status_of(&id(&c)), "not_registered");
    assert_eq!(keys(&items[0]), vec!["email_status", "recipient_id"]);

    // Cloudflare の一覧は 1 回だけ全ページ(2 リクエスト)
    let gets: Vec<_> = app
        .email
        .calls()
        .into_iter()
        .filter(|c| c.method == ErMethod::Get)
        .collect();
    assert_eq!(gets.len(), 2);
    assert_eq!(gets[0].path, "/email/routing/addresses?page=1&per_page=50");
    assert_eq!(gets[1].path, "/email/routing/addresses?page=2&per_page=50");

    // 既存の一覧は変わらない (email_status を足さない)
    let after = call(&app, "GET", RECIPIENTS, Some(t), None).await.json();
    assert_eq!(before, after);

    // 本文の無い成功 (result が null・result_info が無い) は空の一覧として扱う
    app.email
        .then_list(200, r#"{"success":true,"result":null}"#);
    let got = call(&app, "GET", EMAIL_STATUS, Some(t), None).await;
    assert_eq!(got.status, 200);
    assert!(got.json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .all(|i| i["email_status"] == "not_registered"));

    // tenant が無ければ 401 (ほかの口と同じ)
    assert_eq!(
        call(&app, "GET", EMAIL_STATUS, None, None).await.status,
        401
    );
}

async fn create_recipient_with_email(
    app: &App,
    t: Uuid,
    name: &str,
    lw: &str,
    email: &str,
) -> Value {
    let got = call(
        app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(with_email(name, lw, email)),
    )
    .await;
    assert_eq!(got.status, 201);
    got.json()
}

/// 未設定なら 200 `{"configured": false, "items": []}` で Cloudflare を呼ばない
#[tokio::test]
async fn email_status_unconfigured() {
    let t = tenant().await;
    let app = build_with(
        rt_client().await,
        Options {
            email_routing_configured: false,
            ..Options::default()
        },
    );
    create_recipient_with_email(&app, t, "a", "lw-a", "a@example.test").await;
    let got = call(&app, "GET", EMAIL_STATUS, Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"configured": false, "items": []}));
    assert!(app.email.calls().is_empty());
}

/// 一覧の取得に失敗したら 502 (本文は固定の語)。DB が切れていれば 500。ログは固定の語と status か label だけ
#[tokio::test]
async fn email_status_failures() {
    let t = tenant().await;
    let app = app().await;
    create_recipient_with_email(&app, t, "a", "lw-a", "secret-addr@example.test").await;

    app.email
        .then_list(500, r#"{"errors":[{"message":"secret upstream text"}]}"#);
    app.email.then_list_unreachable("fetch");
    app.email.then_list(200, "not json");
    for _ in 0..3 {
        let got = call(&app, "GET", EMAIL_STATUS, Some(t), None).await;
        assert_eq!(got.status, 502);
        assert_eq!(got.json(), json!({"error": "upstream_error"}));
    }
    assert_eq!(
        app.logs(),
        vec![
            "notify: email routing list failed (status 500)",
            "notify: email routing list failed (fetch)",
            "notify: email routing list failed (body)",
        ]
    );

    // DB が切れていれば 500 (Cloudflare は呼ばない)
    let broken = broken_app().await;
    let got = call(&broken, "GET", EMAIL_STATUS, Some(t), None).await;
    assert_eq!(got.status, 500);
    assert!(broken.email.calls().is_empty());
    assert!(broken.logs()[0].starts_with("notify: email status list recipients failed: db ("));

    // admin の PUT で保存前の行を読む DB が切れていれば 500 (rust の PUT の 500 と同じ)
    let got = call_with(
        &broken,
        "PUT",
        &format!("{RECIPIENTS}/{}", Uuid::new_v4()),
        &identity(t, Uuid::new_v4()),
        Some(json!({"email": "x@example.test"})),
    )
    .await;
    assert_eq!(got.status, 500);
    assert!(broken.email.calls().is_empty());
}

// ---- 確認メールの再送 (Refs ippoan/alc-app#436) ----
// Cloudflare に再送の API が無いので、未確認の送り先に限り消して作り直す。確認済みは消さない。
// 偽物の一覧の応答は本物の形 (result_info に total_pages 無し・id あり・verified は文字列か null)。

fn resend_uri(rid: &str) -> String {
    format!("{RECIPIENTS}/{rid}/resend-verification")
}

/// 本物の形の一覧の応答 (1 件)
fn list_one(dest_id: &str, email: &str, verified: Option<&str>) -> String {
    json!({
        "success": true,
        "result": [{"id": dest_id, "email": email, "verified": verified}],
        "result_info": {"page": 1, "per_page": 50, "count": 1, "total_count": 1},
    })
    .to_string()
}

/// admin でなければ 403 (ヘッダー無しも)、宛先が無い・他の tenant の宛先は 404、メールアドレスが無い・空は 400、
/// Cloudflare が未設定なら 503。どれも Cloudflare を呼ばない
#[tokio::test]
async fn resend_preconditions() {
    let t = tenant().await;
    let other = tenant().await;
    let app = app().await;
    let with = create_recipient_with_email(&app, t, "a", "lw-a", "a@example.test").await;
    let none = create_recipient(&app, t, "b", "lw-b").await;
    let blank = create_recipient_with_email(&app, t, "c", "lw-c", "   ").await;
    let foreign = create_recipient_with_email(&app, other, "d", "lw-d", "d@example.test").await;
    let admin = identity(t, Uuid::new_v4());
    let calls_before = app.email.calls().len();

    for headers in [as_role(t, "member"), vec![("X-Tenant-ID", t.to_string())]] {
        let got = call_with(&app, "POST", &resend_uri(&id(&with)), &headers, None).await;
        assert_eq!(got.status, 403);
        assert_eq!(got.json(), json!({"error": "forbidden"}));
    }
    let got = call_with(
        &app,
        "POST",
        &resend_uri(&Uuid::new_v4().to_string()),
        &admin,
        None,
    )
    .await;
    assert_eq!(got.status, 404);
    // 他の tenant の宛先は見えない
    let got = call_with(&app, "POST", &resend_uri(&id(&foreign)), &admin, None).await;
    assert_eq!(got.status, 404);
    for r in [&none, &blank] {
        let got = call_with(&app, "POST", &resend_uri(&id(r)), &admin, None).await;
        assert_eq!(got.status, 400);
        assert_eq!(got.json(), json!({"error": "no_email"}));
    }
    assert_eq!(app.email.calls().len(), calls_before);

    let unconfigured = build_with(
        rt_client().await,
        Options {
            email_routing_configured: false,
            ..Options::default()
        },
    );
    let got = call_with(&unconfigured, "POST", &resend_uri(&id(&with)), &admin, None).await;
    assert_eq!(got.status, 503);
    assert_eq!(got.json(), json!({"error": "not_configured"}));
    assert!(unconfigured.email.calls().is_empty());
    assert!(unconfigured.logs().is_empty());

    // DB が切れていれば 500 (Cloudflare は呼ばない)
    let broken = broken_app().await;
    let got = call_with(&broken, "POST", &resend_uri(&id(&with)), &admin, None).await;
    assert_eq!(got.status, 500);
    assert!(broken.email.calls().is_empty());
    assert!(broken.logs()[0].starts_with("notify: resend verification get recipient failed: db ("));
}

/// 送り先が無ければ作る (POST 1 回。DELETE はしない)。アドレスは正規化して渡す
#[tokio::test]
async fn resend_creates_when_missing() {
    let t = tenant().await;
    let app = app().await;
    let r = create_recipient_with_email(&app, t, "a", "lw-a", " A@Example.TEST").await;
    let admin = identity(t, Uuid::new_v4());
    // 保存の POST (member なので作られない) の後に数える
    assert!(app.email.calls().is_empty());

    app.email
        .then_list(200, &list_one("zz", "other@example.test", None));
    let got = call_with(&app, "POST", &resend_uri(&id(&r)), &admin, None).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"status": "created"}));
    assert_eq!(app.email.methods(), vec![ErMethod::Get, ErMethod::Post]);
    assert_eq!(app.email.created_emails(), vec!["a@example.test"]);
}

/// 確認済みなら消さない: 409 で DELETE も POST も呼ばない
#[tokio::test]
async fn resend_keeps_verified() {
    let t = tenant().await;
    let app = app().await;
    let r = create_recipient_with_email(&app, t, "a", "lw-a", "a@example.test").await;
    app.email.then_list(
        200,
        &list_one("aa11", "A@example.test", Some("2026-01-01T00:00:00Z")),
    );
    let got = call_with(
        &app,
        "POST",
        &resend_uri(&id(&r)),
        &identity(t, Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(got.status, 409);
    assert_eq!(got.json(), json!({"error": "already_verified"}));
    assert_eq!(app.email.methods(), vec![ErMethod::Get]);
    assert!(app.logs().is_empty());
}

/// 未確認なら、その ID の送り先を DELETE してから POST で作り直す
#[tokio::test]
async fn resend_recreates_pending() {
    let t = tenant().await;
    let app = app().await;
    let r = create_recipient_with_email(&app, t, "a", "lw-a", "a@example.test").await;
    app.email
        .then_list(200, &list_one("aa11", "a@example.test", None));
    let got = call_with(
        &app,
        "POST",
        &resend_uri(&id(&r)),
        &identity(t, Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"status": "resent"}));
    assert_eq!(
        app.email.methods(),
        vec![ErMethod::Get, ErMethod::Delete, ErMethod::Post]
    );
    let calls = app.email.calls();
    assert_eq!(calls[1].path, "/email/routing/addresses/aa11");
    assert_eq!(app.email.created_emails(), vec!["a@example.test"]);
    assert!(app.logs().is_empty());

    // 作り直しの POST が「既に在る」でも成功として扱う
    app.email
        .then_list(200, &list_one("aa11", "a@example.test", None));
    app.email.post_reply(
        400,
        r#"{"success":false,"errors":[{"code":1,"message":"Destination address already exists"}]}"#,
    );
    let got = call_with(
        &app,
        "POST",
        &resend_uri(&id(&r)),
        &identity(t, Uuid::new_v4()),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    assert!(app.logs().is_empty());
}

/// Cloudflare の失敗は 502 (本文は固定の語)。ログは固定の語と status か label だけ。
/// 消した後に作るのが失敗したら、送り先が消えたままなので `recreate` の語で見分ける
#[tokio::test]
async fn resend_upstream_failures() {
    let t = tenant().await;
    let app = app().await;
    let r =
        create_recipient_with_email(&app, t, "a", "lw-secret", "secret-addr@example.test").await;
    let admin = identity(t, Uuid::new_v4());
    let uri = resend_uri(&id(&r));
    let upstream = json!({"error": "upstream_error"});
    let pending = list_one("aa11", "secret-addr@example.test", None);

    // 一覧の失敗 (2xx 以外・届かない)
    app.email
        .then_list(500, r#"{"errors":[{"message":"secret upstream text"}]}"#);
    app.email.then_list_unreachable("fetch");
    for _ in 0..2 {
        let got = call_with(&app, "POST", &uri, &admin, None).await;
        assert_eq!(got.status, 502);
        assert_eq!(got.json(), upstream);
    }
    assert_eq!(app.email.methods(), vec![ErMethod::Get, ErMethod::Get]);

    // 送り先が無いときの作成の失敗
    app.email.then_list(200, r#"{"result":[]}"#);
    app.email.post_reply(500, "secret upstream text");
    let got = call_with(&app, "POST", &uri, &admin, None).await;
    assert_eq!(got.status, 502);
    assert_eq!(got.json(), upstream);

    // DELETE の失敗 (2xx 以外・届かない): 作り直しはしない
    let before = app.email.calls().len();
    app.email.then_list(200, &pending);
    app.email.delete_reply(500, "secret upstream text");
    let got = call_with(&app, "POST", &uri, &admin, None).await;
    assert_eq!(got.status, 502);
    assert_eq!(got.json(), upstream);
    app.email.then_list(200, &pending);
    app.email.delete_unreachable("fetch");
    let got = call_with(&app, "POST", &uri, &admin, None).await;
    assert_eq!(got.status, 502);
    let methods: Vec<_> = app.email.methods().split_off(before);
    assert_eq!(
        methods,
        vec![
            ErMethod::Get,
            ErMethod::Delete,
            ErMethod::Get,
            ErMethod::Delete
        ]
    );

    // 消した後の作成の失敗 (2xx 以外・届かない)
    app.email.delete_reply(200, "{}");
    app.email.then_list(200, &pending);
    app.email.post_reply(500, "secret upstream text");
    let got = call_with(&app, "POST", &uri, &admin, None).await;
    assert_eq!(got.status, 502);
    app.email.then_list(200, &pending);
    app.email.post_unreachable("fetch");
    let got = call_with(&app, "POST", &uri, &admin, None).await;
    assert_eq!(got.status, 502);
    assert_eq!(got.json(), upstream);

    assert_eq!(
        app.logs(),
        vec![
            "notify: email routing list failed (status 500)",
            "notify: email routing list failed (fetch)",
            "notify: email routing create failed (status 500)",
            "notify: email routing delete failed (status 500)",
            "notify: email routing delete failed (fetch)",
            "notify: email routing recreate failed (status 500)",
            "notify: email routing recreate failed (fetch)",
        ]
    );
}

// ---- provider 'email' と provider ごとの必須 (Refs ippoan/alc-app#436) ----
// migration 178 で provider に 'email' が入った。DB の CHECK 違反を 500 にしないため、POST / PUT は先に 400 の固定の語で断る。
// PUT は既存の行と合成して検査する (送らなかった項目は COALESCE で既存の値のまま)。

fn error_of(got: &Got) -> Value {
    got.json()["error"].clone()
}

/// POST: provider が許す語の外は `invalid_provider`、provider ごとの必須 (空・空白だけも空) が無ければ 400。何も保存しない
#[tokio::test]
async fn recipient_post_requires_per_provider_field() {
    let t = tenant().await;
    let app = app().await;
    let cases = [
        (
            json!({"name": "x", "provider": "sms", "email": "a@example.test"}),
            "invalid_provider",
        ),
        (
            json!({"name": "x", "provider": "", "lineworks_user_id": "lw"}),
            "invalid_provider",
        ),
        (
            json!({"name": "x", "provider": "line"}),
            "line_user_id_required",
        ),
        (
            json!({"name": "x", "provider": "line", "line_user_id": ""}),
            "line_user_id_required",
        ),
        (
            json!({"name": "x", "provider": "line", "line_user_id": "  "}),
            "line_user_id_required",
        ),
        // 別の provider の ID があっても、自分の provider の ID が無ければ断る
        (
            json!({"name": "x", "provider": "line", "lineworks_user_id": "lw"}),
            "line_user_id_required",
        ),
        (
            json!({"name": "x", "provider": "lineworks"}),
            "lineworks_user_id_required",
        ),
        (
            json!({"name": "x", "provider": "lineworks", "lineworks_user_id": ""}),
            "lineworks_user_id_required",
        ),
        (
            json!({"name": "x", "provider": "lineworks", "lineworks_user_id": " \t"}),
            "lineworks_user_id_required",
        ),
        (
            json!({"name": "x", "provider": "lineworks", "line_user_id": "U"}),
            "lineworks_user_id_required",
        ),
        (json!({"name": "x", "provider": "email"}), "email_required"),
        (
            json!({"name": "x", "provider": "email", "email": ""}),
            "email_required",
        ),
        (
            json!({"name": "x", "provider": "email", "email": "   "}),
            "email_required",
        ),
        (
            json!({"name": "x", "provider": "email", "lineworks_user_id": "lw"}),
            "email_required",
        ),
    ];
    for (body, want) in cases {
        let got = call(&app, "POST", RECIPIENTS, Some(t), Some(body.clone())).await;
        assert_eq!(got.status, 400, "{body}");
        assert_eq!(error_of(&got), want, "{body}");
    }
    let got = call(&app, "GET", RECIPIENTS, Some(t), None).await;
    assert_eq!(got.json(), json!([]));
    assert!(app.logs.lock().unwrap().is_empty());
    assert!(app.email.calls().is_empty());
}

/// provider 'email' の宛先: メールアドレスだけで作れる (201)。ID の列は null。一覧・1 件で読める。admin なら送り先を作る
#[tokio::test]
async fn email_provider_recipient_create_and_update() {
    let t = tenant().await;
    let app = app().await;
    let admin = identity(t, Uuid::new_v4());
    let got = call_with(
        &app,
        "POST",
        RECIPIENTS,
        &admin,
        Some(json!({"name": "メール太郎", "provider": "email", "email": "Taro@Example.test"})),
    )
    .await;
    assert_eq!(got.status, 201);
    let r = got.json();
    assert_eq!(r["provider"], "email");
    assert_eq!(r["email"], "Taro@Example.test");
    assert_eq!(r["lineworks_user_id"], Value::Null);
    assert_eq!(r["line_user_id"], Value::Null);
    assert_eq!(app.email.created_emails(), vec!["taro@example.test"]);
    let path = format!("{RECIPIENTS}/{}", id(&r));

    // 名前だけ直す (provider・email は既存のまま) → 200
    let got = call_with(
        &app,
        "PUT",
        &path,
        &admin,
        Some(json!({"name": "メール次郎"})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["email"], "Taro@Example.test");
    // email を空白にする → 400 (既存の値ではなく入力を見る)。行は変わらない
    for blank in ["", "   "] {
        let got = call_with(&app, "PUT", &path, &admin, Some(json!({"email": blank}))).await;
        assert_eq!((got.status, error_of(&got)), (400, json!("email_required")));
    }
    let got = call_with(&app, "GET", &path, &admin, None).await;
    assert_eq!(got.json()["email"], "Taro@Example.test");
    // 別のアドレスに変える → 200、送り先を作る
    let got = call_with(
        &app,
        "PUT",
        &path,
        &admin,
        Some(json!({"email": "jiro@example.test"})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(
        app.email.created_emails(),
        vec!["taro@example.test", "jiro@example.test"]
    );
    // lineworks に変えるなら lineworks_user_id が要る。送れば通る
    let got = call_with(
        &app,
        "PUT",
        &path,
        &admin,
        Some(json!({"provider": "lineworks"})),
    )
    .await;
    assert_eq!(
        (got.status, error_of(&got)),
        (400, json!("lineworks_user_id_required"))
    );
    let got = call_with(
        &app,
        "PUT",
        &path,
        &admin,
        Some(json!({"provider": "lineworks", "lineworks_user_id": "lw-j"})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["provider"], "lineworks");
    assert!(app.logs.lock().unwrap().is_empty());
}

/// PUT は既存の行と合成して検査する: provider だけ送っても既存の項目で足りれば通り、足りなければ 400。語の外は `invalid_provider`
#[tokio::test]
async fn recipient_put_validates_merged_row() {
    let t = tenant().await;
    let app = app().await;
    // lineworks の宛先 (email あり) と (email なし)
    let with = create_recipient_with_email(&app, t, "あり", "lw-a", "a@example.test").await;
    let without = create_recipient(&app, t, "なし", "lw-b").await;
    let (with_path, without_path) = (
        format!("{RECIPIENTS}/{}", id(&with)),
        format!("{RECIPIENTS}/{}", id(&without)),
    );

    // provider だけ送る: email が既存の行は通る
    let got = call(
        &app,
        "PUT",
        &with_path,
        Some(t),
        Some(json!({"provider": "email"})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["provider"], "email");
    // email が無い行は 400 (行は lineworks のまま)
    let got = call(
        &app,
        "PUT",
        &without_path,
        Some(t),
        Some(json!({"provider": "email"})),
    )
    .await;
    assert_eq!((got.status, error_of(&got)), (400, json!("email_required")));
    let got = call(&app, "GET", &without_path, Some(t), None).await;
    assert_eq!(got.json()["provider"], "lineworks");
    // line の ID が無い行を line に
    let got = call(
        &app,
        "PUT",
        &without_path,
        Some(t),
        Some(json!({"provider": "line"})),
    )
    .await;
    assert_eq!(
        (got.status, error_of(&got)),
        (400, json!("line_user_id_required"))
    );
    let got = call(
        &app,
        "PUT",
        &without_path,
        Some(t),
        Some(json!({"provider": "line", "line_user_id": "U-b"})),
    )
    .await;
    assert_eq!(got.status, 200);
    // provider の語の外
    let got = call(
        &app,
        "PUT",
        &without_path,
        Some(t),
        Some(json!({"provider": "sms"})),
    )
    .await;
    assert_eq!(
        (got.status, error_of(&got)),
        (400, json!("invalid_provider"))
    );
    // lineworks の行の lineworks_user_id を空にする
    let got = call(
        &app,
        "PUT",
        &with_path,
        Some(t),
        Some(json!({"provider": "lineworks"})),
    )
    .await;
    assert_eq!(got.status, 200);
    let got = call(
        &app,
        "PUT",
        &with_path,
        Some(t),
        Some(json!({"lineworks_user_id": " "})),
    )
    .await;
    assert_eq!(
        (got.status, error_of(&got)),
        (400, json!("lineworks_user_id_required"))
    );
    assert!(app.logs.lock().unwrap().is_empty());
}

/// admin でなくても email の宛先は作れる・直せる。送り先は作らない (任意のアドレスに確認メールが飛ぶため)
#[tokio::test]
async fn email_provider_non_admin_saves_without_destination() {
    let t = tenant().await;
    let app = app().await;
    let member = as_role(t, "member");
    let got = call_with(
        &app,
        "POST",
        RECIPIENTS,
        &member,
        Some(json!({"name": "メール", "provider": "email", "email": "m@example.test"})),
    )
    .await;
    assert_eq!(got.status, 201);
    let path = format!("{RECIPIENTS}/{}", id(&got.json()));
    let got = call_with(
        &app,
        "PUT",
        &path,
        &member,
        Some(json!({"email": "n@example.test"})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["email"], "n@example.test");
    assert!(app.email.calls().is_empty());
}
