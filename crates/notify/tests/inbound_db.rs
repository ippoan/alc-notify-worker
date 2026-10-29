//! LINE・LINE WORKS の webhook の受け口 (W8b) を、実 DB (`alc_api_rt` で接続) と LINE のモック (tests/support の `MockHttp`) で通す。
//! 接続先は `NOTIFY_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。
//! 期待値は rust-alc-api の `crates/alc-notify/src/line_webhook.rs`・`lineworks_channels.rs` (internal の 2 本)。
//! どの口も tenant のヘッダーを付けずに打つ (auth-worker が付けない)。例外は LINE WORKS の tenant 付きの 2 本 (`/tenant/…`)。

mod support;

use alc_worker_crypto::secret::encrypt_secret;
use axum::body::Body;
use axum::http::Request;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use rsa::RsaPrivateKey;
use serde_json::{json, Value};
use sha2::Sha256;
use support::*;
use tower::ServiceExt;
use uuid::Uuid;

const LINE_WEBHOOK: &str = "/api/internal/notify/line/webhook";
const BOT_SECRET: &str = "/api/internal/lineworks/bot-secret";
const EVENT: &str = "/api/internal/lineworks/event";
const TENANT_BOT_SECRET: &str = "/api/internal/lineworks/tenant/bot-secret";
const TENANT_EVENT: &str = "/api/internal/lineworks/tenant/event";

fn test_pem() -> &'static str {
    static PEM: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PEM.get_or_init(|| {
        RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 1024)
            .unwrap()
            .to_pkcs8_pem(LineEnding::LF)
            .unwrap()
            .to_string()
    })
}

fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    STANDARD.encode(mac.finalize().into_bytes())
}

/// LINE の設定 (channel secret は乱数。暗号文は [`TEST_SSO_KEY`] で作る。`key_id`・`private_key_encrypted` は渡したまま)
async fn line_config(
    t: Uuid,
    key_id: Option<&str>,
    private_key_encrypted: Option<String>,
) -> String {
    let secret = format!("channel-secret-{}", Uuid::new_v4());
    let enc = encrypt_secret(&secret, TEST_SSO_KEY).unwrap();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_line_configs (tenant_id, name, channel_id, channel_secret_encrypted, \
             channel_access_token_encrypted, key_id, private_key_encrypted) VALUES ($1, 'line', 'ch-1', $2, 'x', $3, $4)",
            &[&t, &enc, &key_id, &private_key_encrypted],
        )
        .await
        .unwrap();
    secret
}

/// 使える LINE の設定。channel secret を返す
async fn good_line_config(t: Uuid) -> String {
    let pk = encrypt_secret(test_pem(), TEST_SSO_KEY).unwrap();
    line_config(t, Some("kid-1"), Some(pk)).await
}

/// raw body と署名で LINE の webhook を打つ (`signature` が `None` ならヘッダーを付けない)
async fn line_webhook(app: &App, body: &[u8], signature: Option<String>) -> Got {
    let mut b = Request::builder()
        .method("POST")
        .uri(LINE_WEBHOOK)
        .header("content-type", "application/json");
    if let Some(s) = signature {
        b = b.header("x-line-signature", s);
    }
    let resp = app
        .router
        .clone()
        .oneshot(b.body(Body::from(body.to_vec())).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    Got {
        status,
        headers,
        body,
    }
}

fn follow(user_id: &str) -> Value {
    json!({"type": "follow", "source": {"type": "user", "userId": user_id}, "replyToken": "r"})
}

fn token_ok(app: &App) {
    app.http.on(
        LINE_TOKEN_URL,
        200,
        r#"{"access_token":"line-tok","expires_in":2592000}"#,
    );
}

/// tenant の LINE の宛先 (名前の順): (名前, line_user_id, provider)
async fn line_recipients(t: Uuid) -> Vec<(String, String, String)> {
    admin()
        .await
        .query(
            "SELECT name, line_user_id, provider FROM alc_api.notify_recipients \
             WHERE tenant_id = $1 ORDER BY name",
            &[&t],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

#[tokio::test]
async fn line_follow_creates_and_renames_recipient() {
    let t = tenant().await;
    let secret = good_line_config(t).await;
    let app = app().await;
    token_ok(&app);
    app.http.on(
        &format!("{LINE_PROFILE_URL}Ufollow1"),
        200,
        r#"{"displayName":"山田 太郎"}"#,
    );
    // follow 以外・userId の無い follow は何もしない
    let body = serde_json::to_vec(&json!({
        "destination": "Ubot",
        "events": [
            {"type": "message", "source": {"type": "user", "userId": "Uother"}},
            {"type": "follow"},
            {"type": "follow", "source": {"type": "group"}},
            follow("Ufollow1"),
        ],
    }))
    .unwrap();
    let got = line_webhook(&app, &body, Some(sign(&secret, &body))).await;
    assert_eq!(got.status, 200);
    assert!(got.body.is_empty());
    assert_eq!(
        line_recipients(t).await,
        vec![("山田 太郎".into(), "Ufollow1".into(), "line".into())]
    );
    let profile = app.http.calls_to(LINE_PROFILE_URL);
    assert_eq!(profile.len(), 1);
    assert_eq!(
        profile[0].headers,
        vec![("Authorization", "Bearer line-tok".to_owned())]
    );

    // 同じ利用者の 2 回目の follow は名前を更新する (行は増えない)
    app.http.on(
        &format!("{LINE_PROFILE_URL}Ufollow1"),
        200,
        r#"{"displayName":"山田"}"#,
    );
    let body = serde_json::to_vec(&json!({"events": [follow("Ufollow1")]})).unwrap();
    let got = line_webhook(&app, &body, Some(sign(&secret, &body))).await;
    assert_eq!(got.status, 200);
    assert_eq!(
        line_recipients(t).await,
        vec![("山田".into(), "Ufollow1".into(), "line".into())]
    );
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

/// profile が引けなければ `LINE User <先頭 8 文字>` (token が取れないときも)
#[tokio::test]
async fn line_follow_falls_back_to_short_name() {
    let t = tenant().await;
    let secret = good_line_config(t).await;
    let app = app().await;
    token_ok(&app);
    app.http.on(LINE_PROFILE_URL, 500, "");
    let body = serde_json::to_vec(&json!({"events": [follow("Uabcdefgh123")]})).unwrap();
    let got = line_webhook(&app, &body, Some(sign(&secret, &body))).await;
    assert_eq!(got.status, 200);

    let app2 = app_with_failing_token().await;
    let body = serde_json::to_vec(&json!({"events": [follow("U7")]})).unwrap();
    let got = line_webhook(&app2, &body, Some(sign(&secret, &body))).await;
    assert_eq!(got.status, 200);
    assert!(app2.http.calls_to(LINE_PROFILE_URL).is_empty());

    assert_eq!(
        line_recipients(t).await,
        vec![
            ("LINE User U7".into(), "U7".into(), "line".into()),
            (
                "LINE User Uabcdefg".into(),
                "Uabcdefgh123".into(),
                "line".into()
            ),
        ]
    );
    assert_eq!(app.logs(), vec!["notify: line webhook profile failed"]);
    assert_eq!(app2.logs(), vec!["notify: line webhook profile failed"]);
}

async fn app_with_failing_token() -> App {
    let app = app().await;
    app.http
        .on(LINE_TOKEN_URL, 400, r#"{"error":"invalid_client"}"#);
    app
}

#[tokio::test]
async fn line_rejects_unsigned_bad_and_unmatched() {
    let t = tenant().await;
    let secret = good_line_config(t).await;
    let app = app().await;
    let body = serde_json::to_vec(&json!({"events": [follow("Ux")]})).unwrap();
    // 署名なし → 400
    assert_eq!(line_webhook(&app, &body, None).await.status, 400);
    // LINE の webhook の JSON でない → 400 (署名が合っていても)
    let bad = b"{\"events\": 1}";
    assert_eq!(
        line_webhook(&app, bad, Some(sign(&secret, bad)))
            .await
            .status,
        400
    );
    // どの設定の channel secret にも合わない → 401
    let got = line_webhook(&app, &body, Some(sign("not-a-channel-secret", &body))).await;
    assert_eq!(got.status, 401);
    assert!(got.body.is_empty());
    assert_eq!(
        app.logs(),
        vec!["notify: line webhook: no matching line config"]
    );
    assert!(line_recipients(t).await.is_empty());
    assert!(app.http.calls().is_empty());
}

/// 署名が合った設定が使えない (key_id / private_key が無い・復号できない) → 500。鍵が無い worker → 500
#[tokio::test]
async fn line_unusable_config_is_500() {
    let body = serde_json::to_vec(&json!({"events": [follow("Ux")]})).unwrap();

    let pk = encrypt_secret(test_pem(), TEST_SSO_KEY).unwrap();
    let no_key_id = line_config(tenant().await, None, Some(pk)).await;
    let no_private_key = line_config(tenant().await, Some("kid"), None).await;
    let undecryptable =
        line_config(tenant().await, Some("kid"), Some("bm90LWNpcGhlcg==".into())).await;

    let app = app().await;
    for secret in [&no_key_id, &no_private_key] {
        assert_eq!(
            line_webhook(&app, &body, Some(sign(secret, &body)))
                .await
                .status,
            500
        );
    }
    assert_eq!(
        line_webhook(&app, &body, Some(sign(&undecryptable, &body)))
            .await
            .status,
        500
    );
    assert_eq!(
        app.logs(),
        vec![
            "notify: line webhook: line config has no key_id / private_key",
            "notify: line webhook: line config has no key_id / private_key",
            "notify: line webhook: decrypt private_key failed",
        ]
    );

    let app = build_with_key(rt_client().await, None);
    assert_eq!(
        line_webhook(&app, &body, Some(sign(&no_key_id, &body)))
            .await
            .status,
        500
    );
    assert_eq!(
        app.logs(),
        vec!["notify: line webhook: no SSO_ENCRYPTION_KEY"]
    );
}

#[tokio::test]
async fn line_db_failures() {
    let t = tenant().await;
    let secret = good_line_config(t).await;
    let body = serde_json::to_vec(&json!({"events": [follow("Udbfail")]})).unwrap();
    // 設定が読めない → 500
    let app = broken_app().await;
    assert_eq!(
        line_webhook(&app, &body, Some(sign(&secret, &body)))
            .await
            .status,
        500
    );
    assert_eq!(app.logs().len(), 1);
    assert!(app.logs()[0].starts_with("notify: list line configs failed: db ("));

    // 宛先が書けなくても 200 (rust と同じ。ログだけ)。token の POST の間に DB を落とす
    let mut pg = rt_client().await;
    let pid = backend_pid(&mut pg).await;
    let app = build(pg);
    token_ok(&app);
    app.http.on(LINE_PROFILE_URL, 200, r#"{"displayName":"x"}"#);
    app.http.kill_db_on_next_post(pid);
    assert_eq!(
        line_webhook(&app, &body, Some(sign(&secret, &body)))
            .await
            .status,
        200
    );
    assert_eq!(app.logs().len(), 1);
    assert!(app.logs()[0].starts_with("notify: upsert line recipient failed: db ("));
    assert!(line_recipients(t).await.is_empty());
}

// ---------- LINE WORKS ----------

/// bot_configs の行 (bot_id は乱数)。(行 id, bot_id)
async fn bot(t: Uuid, secret: Option<&str>, enabled: bool) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let bot_id = format!("bot-{id}");
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.bot_configs (id, tenant_id, name, client_id, client_secret_encrypted, \
             service_account, bot_id, bot_secret_encrypted, enabled) VALUES ($1, $2, 'bot', 'c', 'x', 's', $3, $4, $5)",
            &[&id, &t, &bot_id, &secret, &enabled],
        )
        .await
        .unwrap();
    (id, bot_id)
}

/// bot の lineworks_channels: (channel_id, channel_type, title, active, tenant_id)
async fn channels(bot: Uuid) -> Vec<(String, Option<String>, Option<String>, bool, Uuid)> {
    admin()
        .await
        .query(
            "SELECT channel_id, channel_type, title, active, tenant_id FROM alc_api.lineworks_channels \
             WHERE bot_config_id = $1 ORDER BY channel_id",
            &[&bot],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect()
}

#[tokio::test]
async fn bot_secret_returns_encrypted_secret() {
    let t = tenant().await;
    let (_, with_secret) = bot(t, Some("enc-secret"), true).await;
    let (_, without_secret) = bot(t, None, true).await;
    let (_, disabled) = bot(t, Some("enc"), false).await;
    let app = app().await;

    let got = call(
        &app,
        "GET",
        &format!("{BOT_SECRET}/{with_secret}"),
        None,
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"bot_secret_encrypted": "enc-secret"}));

    let got = call(
        &app,
        "GET",
        &format!("{BOT_SECRET}/{without_secret}"),
        None,
        None,
    )
    .await;
    assert_eq!(got.status, 404);
    assert_eq!(got.json(), json!({"error": "bot_secret_not_configured"}));

    for missing in [disabled, "bot-unknown".to_owned()] {
        let got = call(&app, "GET", &format!("{BOT_SECRET}/{missing}"), None, None).await;
        assert_eq!(got.status, 404);
        assert_eq!(got.json(), json!({"error": "bot_not_found"}));
    }

    let broken = broken_app().await;
    let got = call(
        &broken,
        "GET",
        &format!("{BOT_SECRET}/{with_secret}"),
        None,
        None,
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(
        got.json(),
        json!({"error": "internal_error", "message": "lookup_failed"})
    );
    assert!(broken.logs()[0].starts_with("notify: lookup lineworks bot failed: db ("));
}

#[tokio::test]
async fn event_joins_and_leaves_channels() {
    let t = tenant().await;
    let (bot_row, bot_id) = bot(t, Some("enc"), true).await;
    let app = app().await;
    let send = |v: Value| call(&app, "POST", EVENT, None, Some(v));

    let got = send(
        json!({"bot_id": bot_id, "event_type": "joined", "channel_id": "ch-a",
        "channel_type": "group", "title": "配車"}),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"ok": true}));
    assert_eq!(
        send(json!({"bot_id": bot_id, "event_type": "join", "channel_id": "ch-b"}))
            .await
            .status,
        200
    );
    assert_eq!(
        channels(bot_row).await,
        vec![
            (
                "ch-a".into(),
                Some("group".into()),
                Some("配車".into()),
                true,
                t
            ),
            ("ch-b".into(), None, None, true, t),
        ]
    );

    // 外れる (leave / left)。channel_id なし・知らない種類は何もしない
    for (ev, ch) in [("leave", "ch-a"), ("left", "ch-b")] {
        let got = send(json!({"bot_id": bot_id, "event_type": ev, "channel_id": ch})).await;
        assert_eq!(got.json(), json!({"ok": true}));
    }
    for v in [
        json!({"bot_id": bot_id, "event_type": "joined"}),
        json!({"bot_id": bot_id, "event_type": "message", "channel_id": "ch-c"}),
    ] {
        assert_eq!(send(v).await.json(), json!({"ok": true}));
    }
    assert_eq!(
        channels(bot_row).await,
        vec![
            (
                "ch-a".into(),
                Some("group".into()),
                Some("配車".into()),
                false,
                t
            ),
            ("ch-b".into(), None, None, false, t),
        ]
    );

    // 招き直し: 種類・題が無ければ前の値を残す
    send(json!({"bot_id": bot_id, "event_type": "joined", "channel_id": "ch-a"})).await;
    assert_eq!(
        channels(bot_row).await[0],
        (
            "ch-a".into(),
            Some("group".into()),
            Some("配車".into()),
            true,
            t
        )
    );

    // 知らない bot → 404
    let got =
        send(json!({"bot_id": "bot-unknown", "event_type": "joined", "channel_id": "x"})).await;
    assert_eq!(got.status, 404);
    assert_eq!(got.json(), json!({"error": "bot_not_found"}));
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

/// 読めない・書けない → 500。書く段の失敗は、lineworks_channels に、決めた channel_id でだけ例外を投げる trigger を張って起こす
#[tokio::test]
async fn event_db_failures() {
    let t = tenant().await;
    let (_, bot_id) = bot(t, Some("enc"), true).await;
    let broken = broken_app().await;
    let got = call(
        &broken,
        "POST",
        EVENT,
        None,
        Some(json!({"bot_id": bot_id, "event_type": "joined", "channel_id": "x"})),
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(
        got.json(),
        json!({"error": "internal_error", "message": "lookup_failed"})
    );

    admin()
        .await
        .batch_execute(
            "CREATE OR REPLACE FUNCTION alc_api.notify_test_fail_channel() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN IF NEW.channel_id = 'notify-test-fail' THEN RAISE EXCEPTION 'notify test'; END IF; RETURN NEW; END $$; \
             DROP TRIGGER IF EXISTS notify_test_fail_channel ON alc_api.lineworks_channels; \
             CREATE TRIGGER notify_test_fail_channel BEFORE INSERT OR UPDATE ON alc_api.lineworks_channels \
             FOR EACH ROW EXECUTE FUNCTION alc_api.notify_test_fail_channel();",
        )
        .await
        .unwrap();
    let app = app().await;
    let send = |ev: &str| {
        call(
            &app,
            "POST",
            EVENT,
            None,
            Some(json!({"bot_id": bot_id, "event_type": ev, "channel_id": "notify-test-fail"})),
        )
    };
    let joined = send("joined").await;
    // 外す UPDATE は行が在って初めて trigger が動く。superuser で行を作っておく
    admin()
        .await
        .execute(
            "ALTER TABLE alc_api.lineworks_channels DISABLE TRIGGER notify_test_fail_channel",
            &[],
        )
        .await
        .unwrap();
    send("joined").await;
    admin()
        .await
        .execute(
            "ALTER TABLE alc_api.lineworks_channels ENABLE TRIGGER notify_test_fail_channel",
            &[],
        )
        .await
        .unwrap();
    let left = send("left").await;
    admin()
        .await
        .batch_execute("DROP TRIGGER notify_test_fail_channel ON alc_api.lineworks_channels")
        .await
        .unwrap();

    assert_eq!(joined.status, 500);
    assert_eq!(
        joined.json(),
        json!({"error": "internal_error", "message": "upsert_failed"})
    );
    assert_eq!(left.status, 500);
    assert_eq!(
        left.json(),
        json!({"error": "internal_error", "message": "mark_left_failed"})
    );
    let logs = app.logs();
    assert_eq!(logs.len(), 2, "{logs:?}");
    assert!(logs[0].starts_with("notify: upsert lineworks channel failed: db ("));
    assert!(logs[1].starts_with("notify: mark lineworks channel left failed: db ("));
}

/// tenant 付きの bot-secret: その tenant の bot だけを引く (別の tenant の bot は 404)。ヘッダーが無ければ 401
#[tokio::test]
async fn tenant_bot_secret_is_scoped_to_the_tenant() {
    let (t, other) = (tenant().await, tenant().await);
    let (_, with_secret) = bot(t, Some("enc-secret"), true).await;
    let (_, without_secret) = bot(t, None, true).await;
    let (_, disabled) = bot(t, Some("enc"), false).await;
    let app = app().await;
    let get = |tenant: Option<Uuid>, bot_id: &str| {
        let (app, uri) = (&app, format!("{TENANT_BOT_SECRET}/{bot_id}"));
        async move { call(app, "GET", &uri, tenant, None).await }
    };

    let got = get(Some(t), &with_secret).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"bot_secret_encrypted": "enc-secret"}));

    let got = get(Some(t), &without_secret).await;
    assert_eq!(got.status, 404);
    assert_eq!(got.json(), json!({"error": "bot_secret_not_configured"}));

    for (tenant, bot_id) in [
        (other, with_secret.as_str()),
        (t, disabled.as_str()),
        (t, "bot-unknown"),
    ] {
        let got = get(Some(tenant), bot_id).await;
        assert_eq!(got.status, 404);
        assert_eq!(got.json(), json!({"error": "bot_not_found"}));
    }
    assert_eq!(get(None, &with_secret).await.status, 401);
    assert!(app.logs().is_empty(), "{:?}", app.logs());

    let broken = broken_app().await;
    let got = call(
        &broken,
        "GET",
        &format!("{TENANT_BOT_SECRET}/{with_secret}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(
        got.json(),
        json!({"error": "internal_error", "message": "lookup_failed"})
    );
    assert!(broken.logs()[0].starts_with("notify: lookup lineworks bot failed: db ("));
}

/// tenant 付きの event: その tenant の bot のトークルームだけを足し・外す。別の tenant からは 404 で何も書かない
#[tokio::test]
async fn tenant_event_joins_within_the_tenant() {
    let (t, other) = (tenant().await, tenant().await);
    let (bot_row, bot_id) = bot(t, Some("enc"), true).await;
    let app = app().await;
    let send = |tenant: Option<Uuid>, v: Value| call(&app, "POST", TENANT_EVENT, tenant, Some(v));

    let got = send(
        Some(t),
        json!({"bot_id": bot_id, "event_type": "joined", "channel_id": "ch-a", "title": "配車"}),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), json!({"ok": true}));
    assert_eq!(
        channels(bot_row).await,
        vec![("ch-a".into(), None, Some("配車".into()), true, t)]
    );

    let got = send(
        Some(other),
        json!({"bot_id": bot_id, "event_type": "left", "channel_id": "ch-a"}),
    )
    .await;
    assert_eq!(got.status, 404);
    assert_eq!(got.json(), json!({"error": "bot_not_found"}));
    let got = send(
        None,
        json!({"bot_id": bot_id, "event_type": "left", "channel_id": "ch-a"}),
    )
    .await;
    assert_eq!(got.status, 401);
    assert!(
        channels(bot_row).await[0].3,
        "別の tenant・ヘッダーなしでは外れない"
    );

    let got = send(
        Some(t),
        json!({"bot_id": bot_id, "event_type": "left", "channel_id": "ch-a"}),
    )
    .await;
    assert_eq!(got.json(), json!({"ok": true}));
    assert!(!channels(bot_row).await[0].3);
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

/// 受け口は tenant のヘッダーを要らない。ほかの口は今までどおり要る
#[tokio::test]
async fn inbound_routes_are_outside_the_tenant_layer() {
    let app = app().await;
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{BOT_SECRET}/bot-unknown"),
            None,
            None
        )
        .await
        .status,
        404
    );
    assert_eq!(
        call(&app, "GET", "/api/notify/recipients", None, None)
            .await
            .status,
        401
    );
}

/// 招待のイベントに名前が無ければ、lineworks-worker の channel-info (そのトークルームの bot・その tenant) で引いて入れる。
/// イベントに名前があれば引かない。引けなければ名前なしのまま足し、ログは固定の語と status / label だけ
#[tokio::test]
async fn joined_without_title_fetches_the_channel_name() {
    let t = tenant().await;
    let (bot_row, bot_id) = bot(t, Some("enc"), true).await;
    let app = app().await;
    let send = |v: Value| call(&app, "POST", TENANT_EVENT, Some(t), Some(v));

    app.lineworks
        .reply(200, r#"{"title":"勤怠通知"}"#.as_bytes());
    let got = send(json!({"bot_id": bot_id, "event_type": "joined", "channel_id": "ch a/1"})).await;
    assert_eq!(got.json(), json!({"ok": true}));
    let calls = app.lineworks.calls();
    assert_eq!(calls.len(), 1);
    assert!(!calls[0].post);
    assert_eq!(calls[0].tenant_id, t);
    assert_eq!(
        calls[0].path,
        format!(
            "/api/internal/lineworks/channel-info?channel_id=ch%20a%2F1&bot_config_id={bot_row}"
        )
    );

    // 名前があれば引かない
    send(json!({"bot_id": bot_id, "event_type": "join", "channel_id": "ch-b", "title": "配車"}))
        .await;
    assert_eq!(app.lineworks.calls().len(), 1);
    // 引けない: 上流の失敗・届かない・名前が無い応答
    app.lineworks.reply(502, br#"{"error":"upstream_error"}"#);
    send(json!({"bot_id": bot_id, "event_type": "joined", "channel_id": "ch-c"})).await;
    app.lineworks.unreachable("fetch");
    send(json!({"bot_id": bot_id, "event_type": "joined", "channel_id": "ch-d"})).await;
    app.lineworks.reply(200, br#"{"title":null}"#);
    send(json!({"bot_id": bot_id, "event_type": "joined", "channel_id": "ch-e"})).await;

    let titles: Vec<(String, Option<String>)> = channels(bot_row)
        .await
        .into_iter()
        .map(|(c, _, title, _, _)| (c, title))
        .collect();
    assert_eq!(
        titles,
        vec![
            ("ch a/1".into(), Some("勤怠通知".into())),
            ("ch-b".into(), Some("配車".into())),
            ("ch-c".into(), None),
            ("ch-d".into(), None),
            ("ch-e".into(), None),
        ]
    );
    assert_eq!(
        app.logs(),
        vec![
            "notify: lineworks channel title failed: status 502".to_owned(),
            "notify: lineworks channel title failed: fetch".to_owned(),
        ]
    );
}
