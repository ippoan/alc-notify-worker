//! 配信の口 (distribute・test-distribute) を、実 DB (`alc_api_rt` で接続) と、lineworks-worker・LINE・viewer の
//! モック (tests/support の `MockLineworks`・`MockHttp`) で通す。
//! 接続先は `NOTIFY_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。
//! 期待値は rust-alc-api の `crates/alc-notify/src/distribute.rs`・`clients/line.rs`・`viewer_register.rs`。

mod support;

use std::sync::OnceLock;

use alc_worker_crypto::secret::encrypt_secret;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use rsa::RsaPrivateKey;
use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const TEST_DISTRIBUTE: &str = "/api/notify/test-distribute";

fn distribute_path(doc: Uuid) -> String {
    format!("/api/notify/documents/{doc}/distribute")
}

/// テスト専用の RSA 鍵の PEM (生成は 1 回だけ。ソースに鍵の文字列を置かない)
fn test_pem() -> &'static str {
    static PEM: OnceLock<String> = OnceLock::new();
    PEM.get_or_init(|| {
        RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 1024)
            .unwrap()
            .to_pkcs8_pem(LineEnding::LF)
            .unwrap()
            .to_string()
    })
}

/// 宛先を 1 つ作る (superuser で)
async fn recipient(
    t: Uuid,
    name: &str,
    provider: &str,
    lineworks_user_id: Option<&str>,
    line_user_id: Option<&str>,
    enabled: bool,
) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_recipients (id, tenant_id, name, provider, lineworks_user_id, \
             line_user_id, enabled) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[&id, &t, &name, &provider, &lineworks_user_id, &line_user_id, &enabled],
        )
        .await
        .unwrap();
    id
}

/// CHECK (provider IN ('lineworks', 'line', 'email')) の外の宛先 (rust に在る分岐を通すため)。制約を外して 1 行入れ、
/// 同じ transaction で `NOT VALID` で戻す (ほかの接続から制約が無い時は見えない。以後の行は今までどおり検査される)
async fn unknown_provider_recipient(t: Uuid) {
    admin()
        .await
        .batch_execute(&format!(
            "BEGIN; \
             ALTER TABLE alc_api.notify_recipients DROP CONSTRAINT notify_recipients_provider_check; \
             INSERT INTO alc_api.notify_recipients (tenant_id, name, provider, lineworks_user_id) VALUES ('{t}', 'a', 'sms', 'lw-a'); \
             ALTER TABLE alc_api.notify_recipients ADD CONSTRAINT notify_recipients_provider_check \
               CHECK (provider IN ('lineworks', 'line', 'email')) NOT VALID; \
             COMMIT;"
        ))
        .await
        .unwrap();
}

async fn lw(t: Uuid, name: &str) -> Uuid {
    recipient(
        t,
        name,
        "lineworks",
        Some(&format!("lw-{name}")),
        None,
        true,
    )
    .await
}

async fn line(t: Uuid, name: &str) -> Uuid {
    recipient(t, name, "line", None, Some(&format!("U-{name}")), true).await
}

async fn group(t: Uuid, members: &[Uuid]) -> Uuid {
    let id = Uuid::new_v4();
    let su = admin().await;
    su.execute(
        "INSERT INTO alc_api.notify_groups (id, tenant_id, name) VALUES ($1, $2, 'g')",
        &[&id, &t],
    )
    .await
    .unwrap();
    for m in members {
        su.execute(
            "INSERT INTO alc_api.notify_recipient_groups (group_id, recipient_id) VALUES ($1, $2)",
            &[&id, m],
        )
        .await
        .unwrap();
    }
    id
}

/// 文書の作り方
struct Doc {
    file_name: Option<&'static str>,
    redaction_status: &'static str,
    extracted_data: Option<Value>,
    redacted_r2_key: Option<&'static str>,
}

impl Default for Doc {
    fn default() -> Self {
        Self {
            file_name: Some("memo.txt"),
            redaction_status: "skipped",
            extracted_data: None,
            redacted_r2_key: None,
        }
    }
}

/// 配車手配票の PDF (画像も送る)
fn haisou() -> Doc {
    Doc {
        file_name: Some("haisou.pdf"),
        redaction_status: "completed",
        extracted_data: Some(json!({"logistics": {"loading_place": "東京"}})),
        redacted_r2_key: Some("t/redacted.jpg"),
    }
}

async fn document(t: Uuid, d: Doc) -> Uuid {
    let id = Uuid::new_v4();
    let data = d.extracted_data.map(|v| v.to_string());
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_documents (id, tenant_id, source_type, source_sender, source_subject, \
             r2_key, file_name, file_size_bytes, redaction_status, extracted_data, redacted_r2_key) \
             VALUES ($1, $2, 'email', 'from@example.com', '件名', 't/orig.pdf', $3, 2048, $4, $5::text::jsonb, $6)",
            &[&id, &t, &d.file_name, &d.redaction_status, &data, &d.redacted_r2_key],
        )
        .await
        .unwrap();
    id
}

async fn user(t: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.users (id, tenant_id, google_sub, email, name) VALUES ($1, $2, $3, 'u@example.com', 'u')",
            &[&id, &t, &format!("sub-{id}")],
        )
        .await
        .unwrap();
    id
}

/// LINE の設定 (key_id と private_key の暗号文は渡したまま)
async fn line_config_row(t: Uuid, key_id: Option<&str>, private_key_encrypted: Option<String>) {
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_line_configs (tenant_id, name, channel_id, channel_secret_encrypted, \
             channel_access_token_encrypted, key_id, private_key_encrypted) VALUES ($1, 'line', 'ch-1', 'x', 'x', $2, $3)",
            &[&t, &key_id, &private_key_encrypted],
        )
        .await
        .unwrap();
}

/// 使える LINE の設定 (private_key は [`TEST_SSO_KEY`] で暗号化)
async fn good_line_config(t: Uuid) {
    let enc = encrypt_secret(test_pem(), TEST_SSO_KEY).unwrap();
    line_config_row(t, Some("kid-1"), Some(enc)).await;
}

/// 文書の notify_deliveries の 1 行 (宛先の名前の順)
#[derive(Debug)]
struct Delivery {
    recipient_id: Uuid,
    status: String,
    error_message: Option<String>,
    attempt: i32,
    sent: bool,
    read_token: Uuid,
    triggered_by: Option<Uuid>,
    /// expire_at - created_at (秒)
    expire_secs: f64,
}

async fn deliveries(doc: Uuid) -> Vec<Delivery> {
    admin()
        .await
        .query(
            "SELECT d.recipient_id, d.status, d.error_message, d.attempt, d.sent_at IS NOT NULL, d.read_token, \
             d.triggered_by_user_id, EXTRACT(EPOCH FROM d.expire_at - d.created_at)::float8 \
             FROM alc_api.notify_deliveries d JOIN alc_api.notify_recipients r ON r.id = d.recipient_id \
             WHERE d.document_id = $1 ORDER BY r.name",
            &[&doc],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| Delivery {
            recipient_id: r.get(0),
            status: r.get(1),
            error_message: r.get(2),
            attempt: r.get(3),
            sent: r.get(4),
            read_token: r.get(5),
            triggered_by: r.get(6),
            expire_secs: r.get(7),
        })
        .collect()
}

/// (distribution_status, distributed_at が在るか)
async fn doc_status(doc: Uuid) -> (String, bool) {
    let r = admin()
        .await
        .query_one(
            "SELECT distribution_status, distributed_at IS NOT NULL FROM alc_api.notify_documents WHERE id = $1",
            &[&doc],
        )
        .await
        .unwrap();
    (r.get(0), r.get(1))
}

fn days(d: f64) -> f64 {
    d * 86_400.0
}

fn body_json(body: &[u8]) -> Value {
    serde_json::from_slice(body).unwrap()
}

/// lineworks-worker に送った本文
fn lw_bodies(app: &App) -> Vec<Value> {
    app.lineworks
        .calls()
        .iter()
        .map(|c| serde_json::from_str(c.body.as_deref().unwrap()).unwrap())
        .collect()
}

fn line_token_ok(app: &App) {
    app.http.on(
        LINE_TOKEN_URL,
        200,
        r#"{"access_token":"tok","expires_in":86400,"token_type":"Bearer"}"#,
    );
}

/// 文書が無い・別 tenant → 404、黒塗りが済んでいない → 400、宛先 0 件 → 200 の語、tenant が無い → 401
#[tokio::test]
async fn distribute_rejections() {
    let (t, other) = (tenant().await, tenant().await);
    let app = app().await;
    let missing = call(
        &app,
        "POST",
        &distribute_path(Uuid::new_v4()),
        Some(t),
        None,
    )
    .await;
    assert_eq!(missing.status, 404);
    assert!(missing.body.is_empty());
    let others = document(other, Doc::default()).await;
    lw(other, "o").await;
    assert_eq!(
        call(&app, "POST", &distribute_path(others), Some(t), None)
            .await
            .status,
        404
    );
    assert_eq!(
        call(&app, "POST", &distribute_path(others), None, None)
            .await
            .status,
        401
    );
    assert!(deliveries(others).await.is_empty());

    lw(t, "a").await;
    for status in ["pending", "processing", "failed"] {
        let doc = document(
            t,
            Doc {
                redaction_status: status,
                ..Doc::default()
            },
        )
        .await;
        app.logs.lock().unwrap().clear();
        let got = call(&app, "POST", &distribute_path(doc), Some(t), None).await;
        assert_eq!(got.status, 400, "{status}");
        assert!(got.body.is_empty());
        assert_eq!(
            app.logs(),
            vec!["notify: distribute refused: redaction not done"]
        );
        assert!(deliveries(doc).await.is_empty());
        assert_eq!(doc_status(doc).await, ("pending".to_owned(), false));
    }
    assert!(app.lineworks.calls().is_empty());

    // 宛先 0 件 (enabled が無い) は 200 の語で、文書は pending のまま
    let empty = tenant().await;
    recipient(empty, "off", "lineworks", Some("lw-off"), None, false).await;
    let doc = document(empty, Doc::default()).await;
    let got = call(&app, "POST", &distribute_path(doc), Some(empty), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"message": "No enabled recipients"}));
    assert_eq!(doc_status(doc).await, ("pending".to_owned(), false));
    assert!(app.http.calls().is_empty());
}

/// 宛先の選び方: group_id (enabled なメンバー) > recipient_ids (enabled のうち指定のもの、別 tenant は入らない) > 全 enabled
#[tokio::test]
async fn distribute_target_selection() {
    let (t, other) = (tenant().await, tenant().await);
    let a = lw(t, "a").await;
    let b = lw(t, "b").await;
    let off = recipient(t, "c-off", "lineworks", Some("lw-c"), None, false).await;
    let foreign = lw(other, "d").await;
    let g = group(t, &[a, off]).await;
    let foreign_group = group(other, &[foreign]).await;
    let app = app().await;

    let cases: Vec<(Option<Value>, Vec<Uuid>)> = vec![
        (Some(json!({"target": {"group_id": g}})), vec![a]),
        (Some(json!({"target": {"group_id": foreign_group}})), vec![]),
        (
            Some(json!({"target": {"recipient_ids": [b, off, foreign]}})),
            vec![b],
        ),
        (
            Some(json!({"target": {"all": true, "recipient_ids": []}})),
            vec![a, b],
        ),
        (Some(json!({})), vec![a, b]),
        (None, vec![a, b]),
    ];
    for (body, want) in cases {
        let doc = document(t, Doc::default()).await;
        let got = call(&app, "POST", &distribute_path(doc), Some(t), body.clone()).await;
        assert_eq!(got.status, 200, "{body:?}");
        let rows = deliveries(doc).await;
        let ids: Vec<Uuid> = rows.iter().map(|d| d.recipient_id).collect();
        assert_eq!(ids, want, "{body:?}");
        if want.is_empty() {
            assert_eq!(got.json(), json!({"message": "No enabled recipients"}));
        } else {
            let n = want.len();
            assert_eq!(got.json(), json!({"sent": n, "failed": 0, "total": n}));
        }
    }
}

/// provider 'email' の宛先は文書配信の対象外: 全員・グループ・指定のどれでも delivery 行も送信も作らない
#[tokio::test]
async fn distribute_skips_email_recipients() {
    let t = tenant().await;
    let a = lw(t, "a").await;
    let mail = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_recipients (id, tenant_id, name, provider, email) \
             VALUES ($1, $2, 'mail', 'email', 'm@example.test')",
            &[&mail, &t],
        )
        .await
        .unwrap();
    let g = group(t, &[a, mail]).await;
    let app = app().await;

    let cases: Vec<(Value, Vec<Uuid>)> = vec![
        (json!({}), vec![a]),
        (json!({"target": {"group_id": g}}), vec![a]),
        (json!({"target": {"recipient_ids": [a, mail]}}), vec![a]),
        (json!({"target": {"recipient_ids": [mail]}}), vec![]),
    ];
    let mut sends = 0;
    for (body, want) in cases {
        let doc = document(t, Doc::default()).await;
        let got = call(
            &app,
            "POST",
            &distribute_path(doc),
            Some(t),
            Some(body.clone()),
        )
        .await;
        assert_eq!(got.status, 200, "{body}");
        let ids: Vec<Uuid> = deliveries(doc)
            .await
            .iter()
            .map(|d| d.recipient_id)
            .collect();
        assert_eq!(ids, want, "{body}");
        sends += want.len();
        assert_eq!(app.lineworks.calls().len(), sends, "{body}");
    }

    // email の宛先しかいない tenant: 配る相手がいない
    let only = tenant().await;
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_recipients (tenant_id, name, provider, email) \
             VALUES ($1, 'mail', 'email', 'm@example.test')",
            &[&only],
        )
        .await
        .unwrap();
    let doc = document(only, Doc::default()).await;
    let got = call(&app, "POST", &distribute_path(doc), Some(only), None).await;
    assert_eq!(got.json(), json!({"message": "No enabled recipients"}));
    assert!(deliveries(doc).await.is_empty());
    assert_eq!(app.lineworks.calls().len(), sends);
}

/// LINE WORKS 宛の text だけの配信: deliveries・viewer の登録・lineworks-worker の本文 (bot_config_id 無し)・
/// 配信した人・閲覧期限・distribution_status
#[tokio::test]
async fn distribute_lineworks_text() {
    let t = tenant().await;
    let r = lw(t, "a").await;
    let u = user(t).await;
    let doc = document(
        t,
        Doc {
            redacted_r2_key: Some("t/redacted.pdf"),
            ..Doc::default()
        },
    )
    .await;
    let app = app().await;
    let got = call_with(
        &app,
        "POST",
        &distribute_path(doc),
        &identity(t, u),
        Some(json!({"retention_days": 30})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"sent": 1, "failed": 0, "total": 1}));

    let rows = deliveries(doc).await;
    assert_eq!(rows.len(), 1);
    let d = &rows[0];
    assert_eq!(
        (
            d.status.as_str(),
            d.attempt,
            d.sent,
            d.error_message.as_deref()
        ),
        ("sent", 1, true, None)
    );
    assert_eq!(d.triggered_by, Some(u));
    assert!(
        (d.expire_secs - days(30.0)).abs() < 60.0,
        "{}",
        d.expire_secs
    );
    assert_eq!(doc_status(doc).await, ("completed".to_owned(), true));

    let calls = app.lineworks.calls();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].post);
    assert_eq!(calls[0].path, "/api/internal/lineworks/messages");
    assert_eq!(calls[0].tenant_id, t);
    let view_url = format!("{FRONTEND_URL}/v/{}", d.read_token);
    assert_eq!(
        lw_bodies(&app)[0],
        json!({
            "to": {"user_id": "lw-a"},
            "content": {
                "type": "text",
                "text": format!("📄 memo.txt\n\n新しいドキュメントが届きました\n\n▶ 詳細を見る: {view_url}"),
            },
        })
    );

    // viewer の KV への登録 (r2_key は黒塗り済みを優先、期限は今 + retention_days)
    let reg = app.http.calls_to(REGISTER_URL);
    assert_eq!(reg.len(), 1);
    assert_eq!(
        reg[0].headers,
        vec![("x-notify-internal-secret", VIEWER_SECRET.to_owned())]
    );
    let body = body_json(&reg[0].body);
    let expire = DateTime::<Utc>::from_timestamp(NOW as i64, 0).unwrap() + Duration::days(30);
    assert_eq!(body["token"], d.read_token.to_string());
    assert_eq!(body["tenant_id"], t.to_string());
    assert_eq!(body["r2_key"], "t/redacted.pdf");
    assert_eq!(body["document_id"], doc.to_string());
    assert_eq!(body["recipient_id"], r.to_string());
    assert_eq!(body["file_name"], "memo.txt");
    assert_eq!(body["file_size_bytes"], 2048);
    assert_eq!(body["source_subject"], "件名");
    assert_eq!(body["source_sender"], "from@example.com");
    assert!(body["source_received_at"].is_string());
    assert_eq!(body["expire_at"], expire.to_rfc3339());
    assert_eq!(app.http.calls().len(), 1);
    assert!(app.logs().is_empty(), "{:?}", app.logs());

    // 呼び手が分からない・期限の既定は 7 日 (行は migration の既定のまま)。範囲の外は 1..90 に丸める
    for (retention, want_days) in [
        (None, 7.0),
        (Some(0), 1.0),
        (Some(-5), 1.0),
        (Some(1000), 90.0),
    ] {
        let doc = document(t, Doc::default()).await;
        let body = retention.map(|r: i64| json!({"retention_days": r}));
        let got = call(&app, "POST", &distribute_path(doc), Some(t), body).await;
        assert_eq!(got.status, 200);
        let rows = deliveries(doc).await;
        assert_eq!(rows[0].triggered_by, None);
        assert!(
            (rows[0].expire_secs - days(want_days)).abs() < 60.0,
            "{retention:?}: {}",
            rows[0].expire_secs
        );
        // r2_key は黒塗り済みが無ければ原本
        let reg = app.http.calls_to(REGISTER_URL);
        assert_eq!(body_json(&reg.last().unwrap().body)["r2_key"], "t/orig.pdf");
    }
    // 配信した人は分かるが期限は既定 → triggered_by だけ書く
    let doc = document(t, Doc::default()).await;
    let got = call_with(&app, "POST", &distribute_path(doc), &identity(t, u), None).await;
    assert_eq!(got.status, 200);
    let rows = deliveries(doc).await;
    assert_eq!(rows[0].triggered_by, Some(u));
    assert!((rows[0].expire_secs - days(7.0)).abs() < 60.0);
}

/// 配車手配票の PDF は text (「▶ 詳細」行なし) の後に画像 (`.jpg`)。画像の失敗は成功扱い、text の失敗・届かないは failed
#[tokio::test]
async fn distribute_lineworks_image_and_failures() {
    let t = tenant().await;
    lw(t, "a").await;
    lw(t, "b").await;
    lw(t, "c").await;
    let doc = document(t, haisou()).await;
    let app = app().await;
    // a: text 204 → 画像 500 / b: text 500 / c: 届かない
    app.lineworks.then_reply(204);
    app.lineworks.then_reply(500);
    app.lineworks.then_reply(500);
    app.lineworks.unreachable("fetch");
    let got = call(&app, "POST", &distribute_path(doc), Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"sent": 1, "failed": 2, "total": 3}));

    let rows = deliveries(doc).await;
    assert_eq!(rows[0].status, "sent");
    assert_eq!(
        (
            rows[1].status.as_str(),
            rows[1].error_message.as_deref(),
            rows[1].attempt,
            rows[1].sent
        ),
        ("failed", Some("LINE WORKS: send failed"), 1, false)
    );
    assert_eq!(
        (rows[2].status.as_str(), rows[2].error_message.as_deref()),
        ("failed", Some("LINE WORKS: unreachable"))
    );
    assert_eq!(doc_status(doc).await, ("completed".to_owned(), true));

    let bodies = lw_bodies(&app);
    assert_eq!(bodies.len(), 4, "a の text・画像、b の text、c の text");
    assert_eq!(
        bodies[0],
        json!({"to": {"user_id": "lw-a"}, "content": {"type": "text", "text": "📄 haisou.pdf\n📍 積地: 東京"}})
    );
    let image = format!(
        "{FRONTEND_URL}/api/notify/v/{}/image.jpg",
        rows[0].read_token
    );
    assert_eq!(
        bodies[1],
        json!({"to": {"user_id": "lw-a"}, "content": {"type": "image", "original_url": image, "preview_url": image}})
    );
    assert!(bodies.iter().all(|b| b.get("bot_config_id").is_none()));
    assert_eq!(bodies[2]["to"]["user_id"], "lw-b");
    assert_eq!(bodies[3]["to"]["user_id"], "lw-c");
    // ログは固定の語だけ (宛先・tenant を出さない)
    assert_eq!(
        app.logs(),
        vec![
            "notify: deliver image failed (lineworks)",
            "notify: deliver failed (LINE WORKS: send failed)",
            "notify: deliver failed (LINE WORKS: unreachable)",
        ]
    );
}

/// LINE 宛: 鍵を復号して JWT (kid・claim) で token を 1 回だけ取り、text → 画像を push。画像の失敗は成功扱い、
/// text の失敗は failed
#[tokio::test]
async fn distribute_line() {
    let t = tenant().await;
    good_line_config(t).await;
    line(t, "a").await;
    line(t, "b").await;
    line(t, "c").await;
    let doc = document(t, haisou()).await;
    let app = app().await;
    line_token_ok(&app);
    // a: 画像だけ 400 / c: text が 400
    app.http
        .on_body(LINE_PUSH_URL, r#""type":"image"}],"to":"U-a""#, 400);
    app.http.on_body(LINE_PUSH_URL, r#""to":"U-c""#, 400);
    let got = call(&app, "POST", &distribute_path(doc), Some(t), None).await;
    assert_eq!(got.json(), json!({"sent": 2, "failed": 1, "total": 3}));
    let rows = deliveries(doc).await;
    assert_eq!(
        rows.iter().map(|d| d.status.as_str()).collect::<Vec<_>>(),
        ["sent", "sent", "failed"]
    );
    assert_eq!(rows[2].error_message.as_deref(), Some("LINE: send failed"));

    // token は isolate の cache で 1 回だけ。JWT は kid と LINE の claim
    let token_calls = app.http.calls_to(LINE_TOKEN_URL);
    assert_eq!(token_calls.len(), 1);
    assert_eq!(app.line_tokens.len(), 1);
    let form = String::from_utf8(token_calls[0].body.clone()).unwrap();
    let jwt = form.split("client_assertion=").nth(1).unwrap();
    let part = |i: usize| -> Value {
        serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(jwt.split('.').nth(i).unwrap())
                .unwrap(),
        )
        .unwrap()
    };
    assert_eq!(
        part(0),
        json!({"alg": "RS256", "typ": "JWT", "kid": "kid-1"})
    );
    assert_eq!(
        part(1),
        json!({
            "iss": "ch-1", "sub": "ch-1", "aud": "https://api.line.me/",
            "exp": NOW + 1800, "token_exp": 2_592_000, "token_type": "Bearer",
        })
    );

    let pushes = app.http.calls_to(LINE_PUSH_URL);
    assert_eq!(pushes.len(), 5, "a・b は text と画像、c は text だけ");
    assert!(pushes
        .iter()
        .all(|p| p.headers == vec![("Authorization", "Bearer tok".to_owned())]));
    let image = format!(
        "{FRONTEND_URL}/api/notify/v/{}/image.jpg",
        rows[0].read_token
    );
    assert_eq!(
        body_json(&pushes[0].body),
        json!({"to": "U-a", "messages": [{"type": "text", "text": "📄 haisou.pdf\n📍 積地: 東京"}]})
    );
    assert_eq!(
        body_json(&pushes[1].body),
        json!({"to": "U-a", "messages": [{"type": "image", "originalContentUrl": image, "previewImageUrl": image}]})
    );
    assert_eq!(
        app.logs(),
        vec![
            "notify: deliver image failed (line)",
            "notify: deliver failed (LINE: send failed)",
        ]
    );
    assert!(app.lineworks.calls().is_empty());
}

/// 宛先ごとの失敗の語 (固定) が error_message に残る
#[tokio::test]
async fn distribute_failure_reasons() {
    let app = app().await;
    // (tenant の準備, 宛先, 期待する語)
    async fn reason_for(app: &App, t: Uuid) -> Option<String> {
        let doc = document(t, Doc::default()).await;
        let got = call(app, "POST", &distribute_path(doc), Some(t), None).await;
        assert_eq!(got.json(), json!({"sent": 0, "failed": 1, "total": 1}));
        deliveries(doc).await.remove(0).error_message
    }

    let t = tenant().await;
    line(t, "a").await;
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("LINE config not found")
    );

    let t = tenant().await;
    line(t, "a").await;
    line_config_row(t, None, Some("x".into())).await;
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("LINE config missing key_id")
    );

    let t = tenant().await;
    line(t, "a").await;
    line_config_row(t, Some("k"), None).await;
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("LINE config missing private_key")
    );

    let t = tenant().await;
    line(t, "a").await;
    line_config_row(t, Some("k"), Some("not-a-ciphertext".into())).await;
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("LINE config: decrypt private_key failed")
    );

    // 復号はできたが PEM でない → JWT を組めない
    let t = tenant().await;
    line(t, "a").await;
    line_config_row(
        t,
        Some("k"),
        Some(encrypt_secret("not a pem", TEST_SSO_KEY).unwrap()),
    )
    .await;
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("LINE: JWT error")
    );

    // token の口が断る・届かない
    let t = tenant().await;
    line(t, "a").await;
    good_line_config(t).await;
    app.http
        .on(LINE_TOKEN_URL, 400, r#"{"error":"invalid_client"}"#);
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("LINE: token issue failed")
    );
    app.http.fail(LINE_TOKEN_URL);
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("LINE: unreachable")
    );

    let t = tenant().await;
    recipient(t, "a", "line", Some("lw-a"), None, true).await;
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("No line_user_id")
    );

    let t = tenant().await;
    recipient(t, "a", "lineworks", None, Some("U-a"), true).await;
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("No lineworks_user_id")
    );

    let t = tenant().await;
    unknown_provider_recipient(t).await;
    assert_eq!(
        reason_for(&app, t).await.as_deref(),
        Some("Unknown provider")
    );

    // SSO_ENCRYPTION_KEY が取れない
    let t = tenant().await;
    line(t, "a").await;
    good_line_config(t).await;
    let no_key = build_with_key(rt_client().await, None);
    assert_eq!(
        reason_for(&no_key, t).await.as_deref(),
        Some("Encryption key not set")
    );
    assert!(no_key.http.calls_to(LINE_TOKEN_URL).is_empty());
    // ログに宛先・tenant を出さない
    assert!(app
        .logs()
        .iter()
        .all(|l| l.starts_with("notify: deliver failed (") && !l.contains(&t.to_string())));
}

/// viewer: NOTIFY_FRONTEND_URL か INTERNAL_SHARED_SECRET が無ければ登録しない (URL は既定)。登録の失敗 (302 など) は
/// ログだけで配信は続ける
#[tokio::test]
async fn distribute_viewer_registration_is_best_effort() {
    let t = tenant().await;
    lw(t, "a").await;

    let no_frontend = build_with(
        rt_client().await,
        Options {
            frontend_url: None,
            ..Options::default()
        },
    );
    let doc = document(t, Doc::default()).await;
    let got = call(&no_frontend, "POST", &distribute_path(doc), Some(t), None).await;
    assert_eq!(got.json()["sent"], 1);
    assert!(no_frontend.http.calls().is_empty());
    let token = deliveries(doc).await[0].read_token;
    assert!(lw_bodies(&no_frontend)[0]["content"]["text"]
        .as_str()
        .unwrap()
        .ends_with(&format!(
            "▶ 詳細を見る: https://notify.example.com/v/{token}"
        )));

    let no_secret = build_with(
        rt_client().await,
        Options {
            viewer_secret: None,
            ..Options::default()
        },
    );
    let doc = document(t, Doc::default()).await;
    call(&no_secret, "POST", &distribute_path(doc), Some(t), None).await;
    assert!(no_secret.http.calls().is_empty());
    assert_eq!(no_secret.lineworks.calls().len(), 1);

    // CF Access の 302・届かない は失敗のログだけ
    let app = app().await;
    app.http.on(REGISTER_URL, 302, "");
    let doc = document(t, Doc::default()).await;
    let got = call(&app, "POST", &distribute_path(doc), Some(t), None).await;
    assert_eq!(got.json(), json!({"sent": 1, "failed": 0, "total": 1}));
    app.http.fail(REGISTER_URL);
    let doc = document(t, Doc::default()).await;
    let got = call(&app, "POST", &distribute_path(doc), Some(t), None).await;
    assert_eq!(got.json()["sent"], 1);
    assert_eq!(
        app.logs(),
        vec![
            "notify: register view failed",
            "notify: register view failed"
        ]
    );
}

/// test-distribute: 指定のうち enabled な宛先へ text だけ。DB に行を作らない。空の指定は 400
#[tokio::test]
async fn test_distribute_sends_text_only() {
    let (t, other) = (tenant().await, tenant().await);
    let a = lw(t, "a").await;
    let off = recipient(t, "b-off", "lineworks", Some("lw-b"), None, false).await;
    lw(t, "c").await;
    let d = lw(t, "d").await;
    let l = line(t, "f").await;
    good_line_config(t).await;
    let foreign = lw(other, "e").await;
    let app = app().await;
    line_token_ok(&app);

    let empty = call(
        &app,
        "POST",
        TEST_DISTRIBUTE,
        Some(t),
        Some(json!({"message": "hi", "recipient_ids": []})),
    )
    .await;
    assert_eq!(empty.status, 400);
    assert!(empty.body.is_empty());

    app.lineworks.then_reply(204);
    app.lineworks.then_reply(500);
    let got = call(
        &app,
        "POST",
        TEST_DISTRIBUTE,
        Some(t),
        Some(json!({"message": "hello", "recipient_ids": [a, off, d, l, foreign]})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"sent": 2, "failed": 1, "total": 3}));
    assert_eq!(
        lw_bodies(&app),
        vec![
            json!({"to": {"user_id": "lw-a"}, "content": {"type": "text", "text": "hello"}}),
            json!({"to": {"user_id": "lw-d"}, "content": {"type": "text", "text": "hello"}}),
        ]
    );
    // LINE は text だけ (viewer の登録も画像もしない)
    let pushes = app.http.calls_to(LINE_PUSH_URL);
    assert_eq!(pushes.len(), 1);
    assert_eq!(
        body_json(&pushes[0].body),
        json!({"to": "U-f", "messages": [{"type": "text", "text": "hello"}]})
    );
    assert!(app.http.calls_to(REGISTER_URL).is_empty());
    assert_eq!(
        app.logs(),
        vec!["notify: test deliver failed (LINE WORKS: send failed)"]
    );
    let n: i64 = admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.notify_deliveries WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
    assert_eq!(
        call(
            &app,
            "POST",
            TEST_DISTRIBUTE,
            None,
            Some(json!({"message": "x", "recipient_ids": [a]}))
        )
        .await
        .status,
        401
    );
}

/// DB の失敗: 最初の読みは 500 (本文なし)。配信の途中で切れたら、その後の書き込みはログだけで応答は返す
#[tokio::test]
async fn distribute_db_failures() {
    let t = tenant().await;
    let a = lw(t, "a").await;
    let broken = broken_app().await;
    let doc = document(t, Doc::default()).await;
    let got = call(&broken, "POST", &distribute_path(doc), Some(t), None).await;
    assert_eq!(got.status, 500);
    assert!(got.body.is_empty());
    let got = call(
        &broken,
        "POST",
        TEST_DISTRIBUTE,
        Some(t),
        Some(json!({"message": "x", "recipient_ids": [a]})),
    )
    .await;
    assert_eq!(got.status, 500);
    let logs = broken.logs();
    assert_eq!(logs.len(), 2);
    assert!(logs[0].starts_with("notify: get document failed: db ("));
    assert!(logs[1].starts_with("notify: list recipients failed: db ("));

    // LINE WORKS 宛: viewer の登録の間に DB が切れる → 送信は済み、sent の記録と completed はログだけ
    let mut pg = rt_client().await;
    let pid = backend_pid(&mut pg).await;
    let app = build(pg);
    app.http.kill_db_on_next_post(pid);
    let got = call(&app, "POST", &distribute_path(doc), Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"sent": 1, "failed": 0, "total": 1}));
    let logs = app.logs();
    assert_eq!(logs.len(), 2, "{logs:?}");
    assert!(logs[0].starts_with("notify: mark delivery sent failed: db ("));
    assert!(logs[1].starts_with("notify: set distribution completed failed: db ("));
    assert_eq!(deliveries(doc).await[0].status, "pending");
    assert_eq!(doc_status(doc).await, ("in_progress".to_owned(), false));

    // LINE 宛: 鍵を読む前に切れる → failed (理由は記録できずログだけ)
    let t = tenant().await;
    line(t, "a").await;
    good_line_config(t).await;
    let doc = document(t, Doc::default()).await;
    let mut pg = rt_client().await;
    let pid = backend_pid(&mut pg).await;
    let app = build(pg);
    app.http.kill_db_on_next_post(pid);
    let got = call(&app, "POST", &distribute_path(doc), Some(t), None).await;
    assert_eq!(got.json(), json!({"sent": 0, "failed": 1, "total": 1}));
    let logs = app.logs();
    assert_eq!(logs.len(), 4, "{logs:?}");
    assert!(logs[0].starts_with("notify: get line config failed: db ("));
    assert_eq!(logs[1], "notify: deliver failed (LINE config: db error)");
    assert!(logs[2].starts_with("notify: fail delivery failed: db ("));
    assert!(logs[3].starts_with("notify: set distribution completed failed: db ("));
    assert!(logs
        .iter()
        .all(|l| !l.contains(&t.to_string()) && !l.contains(&doc.to_string())));
}
