//! 宛先・グループの口を、実 DB (`alc_api_rt` で接続) で通す。
//! 接続先は `NOTIFY_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。
//! 期待値は rust-alc-api の `crates/alc-notify/src/recipients.rs`・`groups.rs` と repo 層の SQL の意味。

mod support;

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

/// 無い行を直す → 500 (rust の `fetch_one` と同じ)。DB の制約違反 → 500。どちらも本文なし、ログは固定の語と kind だけ
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
    // provider の CHECK 違反
    let got = call(
        &app,
        "POST",
        RECIPIENTS,
        Some(t),
        Some(json!({"name": "x", "provider": "mail", "lineworks_user_id": "lw-x"})),
    )
    .await;
    assert_eq!((got.status, got.body.len()), (500, 0));
    let logs = app.logs.lock().unwrap().clone();
    assert_eq!(
        logs,
        vec![
            "notify: update recipient failed: db (other)".to_owned(),
            "notify: create recipient failed: db (23514)".to_owned(),
        ]
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
    assert_eq!(got.status, 500);
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
