//! 文書とメールの閲覧の口 (W8c-1) を、実 DB (`alc_api_rt` で接続) と R2 のモック (tests/support の `MockStore`) で通す。
//! 接続先は `NOTIFY_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。
//! 期待値は rust-alc-api の `crates/alc-notify/src/documents.rs`・`email_documents.rs`・`repo/notify_deliveries.rs`。

mod support;

use std::sync::atomic::Ordering;

use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const DOCUMENTS: &str = "/api/notify/documents";
const EMAILS: &str = "/api/notify/emails";

/// 文書の行の差し替え (既定は黒塗り済みの .jpg を持つ PDF の文書)
struct Doc {
    r2_key: &'static str,
    redacted_r2_key: Option<&'static str>,
    file_name: Option<&'static str>,
    email_message_id: Option<Uuid>,
    distribution_status: &'static str,
    file_size_bytes: Option<i64>,
    /// `created_at` を今から何秒前にするか (並びを決める)
    age_secs: i64,
}

impl Default for Doc {
    fn default() -> Self {
        Self {
            r2_key: "t/orig.pdf",
            redacted_r2_key: Some("t/redacted.jpg"),
            file_name: Some("配車 \"票\".pdf"),
            email_message_id: None,
            distribution_status: "pending",
            file_size_bytes: Some(2048),
            age_secs: 0,
        }
    }
}

/// 文書を 1 つ作る (superuser で。抽出・黒塗りの列も埋める)
async fn document(t: Uuid, d: Doc) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_documents (id, tenant_id, source_type, source_sender, source_subject, \
             r2_key, file_name, file_size_bytes, extracted_title, extracted_date, extracted_summary, \
             extracted_phone_numbers, extracted_data, extraction_status, distribution_status, redacted_r2_key, \
             redactions_applied, redaction_status, redact_total_ms, email_message_id, source_body_text, \
             source_received_at, created_at) \
             VALUES ($1, $2, 'email', 'from@example.com', '件名', $3, $4, $5, '見積', DATE '2026-10-01', '要約', \
             ARRAY['03-0000-0000'], '{\"logistics\": {\"loading_place\": \"東京\"}}'::jsonb, 'completed', $6, $7, \
             2, 'completed', 120, $8, '本文', TIMESTAMPTZ '2026-10-01 09:00:00+00', \
             NOW() - make_interval(secs => $9::float8))",
            &[
                &id,
                &t,
                &d.r2_key,
                &d.file_name,
                &d.file_size_bytes,
                &d.distribution_status,
                &d.redacted_r2_key,
                &d.email_message_id,
                &(d.age_secs as f64),
            ],
        )
        .await
        .unwrap();
    id
}

/// 宛先 (名前は `name`) と、`by_user` なら送った人の付いた配信の記録を作る
async fn delivery(t: Uuid, doc: Uuid, name: &str, by_user: bool) {
    let su = admin().await;
    let rid = Uuid::new_v4();
    su.execute(
        "INSERT INTO alc_api.notify_recipients (id, tenant_id, name, provider, lineworks_user_id) \
         VALUES ($1, $2, $3, 'lineworks', $4)",
        &[&rid, &t, &name, &format!("lw-{rid}")],
    )
    .await
    .unwrap();
    let user = if by_user {
        let uid = Uuid::new_v4();
        su.execute(
            "INSERT INTO alc_api.users (id, tenant_id, google_sub, email, name) \
             VALUES ($1, $2, $3, 'sender@example.com', '送信者')",
            &[&uid, &t, &format!("sub-{uid}")],
        )
        .await
        .unwrap();
        Some(uid)
    } else {
        None
    };
    su.execute(
        "INSERT INTO alc_api.notify_deliveries (tenant_id, document_id, recipient_id, provider, status, \
         triggered_by_user_id) VALUES ($1, $2, $3, 'lineworks', 'sent', $4)",
        &[&t, &doc, &rid, &user],
    )
    .await
    .unwrap();
}

fn ids(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|d| d["id"].as_str().unwrap().to_owned())
        .collect()
}

fn header<'a>(got: &'a Got, name: &str) -> &'a str {
    got.headers.get(name).unwrap().to_str().unwrap()
}

/// 一覧は tenant の文書だけを新しい順に。JSON は rust の `NotifyDocument` の全列
#[tokio::test]
async fn list_documents_is_newest_first_with_all_columns() {
    let t = tenant().await;
    let other = tenant().await;
    let old = document(
        t,
        Doc {
            age_secs: 60,
            ..Doc::default()
        },
    )
    .await;
    let new = document(t, Doc::default()).await;
    document(other, Doc::default()).await;
    let app = app().await;

    let got = call(&app, "GET", DOCUMENTS, Some(t), None).await;
    assert_eq!(got.status, 200);
    let body = got.json();
    assert_eq!(ids(&body), [new.to_string(), old.to_string()]);
    let doc = &body[0];
    let mut keys: Vec<&str> = doc
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    keys.sort();
    let mut want = vec![
        "id",
        "tenant_id",
        "source_type",
        "source_sender",
        "source_subject",
        "r2_key",
        "file_name",
        "file_size_bytes",
        "extracted_title",
        "extracted_date",
        "extracted_summary",
        "extracted_phone_numbers",
        "extracted_data",
        "extraction_status",
        "extraction_error",
        "distribution_status",
        "distributed_at",
        "redacted_r2_key",
        "redacted_at",
        "redactions_applied",
        "redaction_status",
        "redaction_error",
        "redact_dl_ms",
        "redact_llm_ms",
        "redact_render_ms",
        "redact_upload_ms",
        "redact_total_ms",
        "created_at",
        "updated_at",
    ];
    want.sort();
    assert_eq!(keys, want);
    assert_eq!(doc["tenant_id"], t.to_string());
    assert_eq!(doc["extracted_date"], "2026-10-01");
    assert_eq!(doc["extracted_phone_numbers"], json!(["03-0000-0000"]));
    assert_eq!(
        doc["extracted_data"],
        json!({"logistics": {"loading_place": "東京"}})
    );
    assert_eq!(doc["redacted_r2_key"], "t/redacted.jpg");
    assert_eq!(doc["redactions_applied"], 2);
    assert_eq!(doc["redact_total_ms"], 120);
    assert_eq!(doc["redact_dl_ms"], Value::Null);

    let got = call(
        &app,
        "GET",
        &format!("{DOCUMENTS}?limit=1&offset=1"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(ids(&got.json()), [old.to_string()]);

    // tenant のヘッダーが無ければ 401 (tenant の layer)
    assert_eq!(call(&app, "GET", DOCUMENTS, None, None).await.status, 401);
}

/// 1 件は `{document, deliveries}`。配信の記録に宛先の名前と送った人。ほかの tenant の文書・無い文書は 404
#[tokio::test]
async fn get_document_with_deliveries() {
    let t = tenant().await;
    let other = tenant().await;
    let doc = document(t, Doc::default()).await;
    delivery(t, doc, "山田", true).await;
    delivery(t, doc, "佐藤", false).await;
    let empty = document(t, Doc::default()).await;
    let foreign = document(other, Doc::default()).await;
    let app = app().await;

    let got = call(&app, "GET", &format!("{DOCUMENTS}/{doc}"), Some(t), None).await;
    assert_eq!(got.status, 200);
    let body = got.json();
    assert_eq!(body["document"]["id"], doc.to_string());
    let deliveries = body["deliveries"].as_array().unwrap();
    assert_eq!(deliveries.len(), 2);
    let mut names: Vec<&str> = deliveries
        .iter()
        .map(|d| d["recipient_name"].as_str().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["佐藤", "山田"]);
    let by = deliveries
        .iter()
        .find(|d| d["recipient_name"] == "山田")
        .unwrap();
    assert_eq!(by["triggered_by_name"], "送信者");
    assert_eq!(by["triggered_by_email"], "sender@example.com");
    assert_eq!(by["document_id"], doc.to_string());
    assert_eq!(by["status"], "sent");
    assert_eq!(by["attempt"], 0);
    let anon = deliveries
        .iter()
        .find(|d| d["recipient_name"] == "佐藤")
        .unwrap();
    assert_eq!(anon["triggered_by_user_id"], Value::Null);
    assert_eq!(anon["triggered_by_name"], Value::Null);

    let got = call(&app, "GET", &format!("{DOCUMENTS}/{empty}"), Some(t), None).await;
    assert_eq!(got.json()["deliveries"], json!([]));

    for id in [foreign, Uuid::new_v4()] {
        let got = call(&app, "GET", &format!("{DOCUMENTS}/{id}"), Some(t), None).await;
        assert_eq!(got.status, 404);
    }
    let got = call(
        &app,
        "GET",
        &format!("{DOCUMENTS}/not-a-uuid"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 400);
}

/// preview は黒塗り済み (無ければ原本)、`original=true` なら原本。Content-Type は key の拡張子、inline・nosniff
#[tokio::test]
async fn preview_picks_redacted_or_original() {
    let t = tenant().await;
    let doc = document(t, Doc::default()).await;
    let plain = document(
        t,
        Doc {
            r2_key: "t/plain.PDF",
            redacted_r2_key: None,
            file_name: None,
            ..Doc::default()
        },
    )
    .await;
    let app = app().await;
    app.files.insert("t/orig.pdf", b"%PDF-orig");
    app.files.insert("t/redacted.jpg", b"jpeg");
    app.files.insert("t/plain.PDF", b"%PDF-plain");

    let got = call(
        &app,
        "GET",
        &format!("{DOCUMENTS}/{doc}/preview"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.body, b"jpeg");
    assert_eq!(header(&got, "content-type"), "image/jpeg");
    assert_eq!(
        header(&got, "content-disposition"),
        "inline; filename*=UTF-8''%E9%85%8D%E8%BB%8A%20%22%E7%A5%A8%22.pdf; filename=\"__ ___.pdf\""
    );
    assert_eq!(header(&got, "x-content-type-options"), "nosniff");

    for q in ["original=true", "original=false"] {
        let got = call(
            &app,
            "GET",
            &format!("{DOCUMENTS}/{doc}/preview?{q}"),
            Some(t),
            None,
        )
        .await;
        let want: &[u8] = if q == "original=true" {
            b"%PDF-orig"
        } else {
            b"jpeg"
        };
        assert_eq!(got.body, want, "{q}");
    }

    let got = call(
        &app,
        "GET",
        &format!("{DOCUMENTS}/{plain}/preview"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.body, b"%PDF-plain");
    assert_eq!(header(&got, "content-type"), "application/pdf");
    assert_eq!(
        header(&got, "content-disposition"),
        "inline; filename*=UTF-8''attachment; filename=\"attachment\""
    );
}

/// inline に開くと危ない型 (svg・txt) は rust と違い octet-stream の添付にする
#[tokio::test]
async fn preview_of_unsafe_type_is_attachment() {
    let t = tenant().await;
    let app = app().await;
    for key in ["t/x.svg", "t/x.txt"] {
        let doc = document(
            t,
            Doc {
                r2_key: key,
                redacted_r2_key: None,
                file_name: Some("x"),
                ..Doc::default()
            },
        )
        .await;
        app.files.insert(key, b"<svg/>");
        let got = call(
            &app,
            "GET",
            &format!("{DOCUMENTS}/{doc}/preview"),
            Some(t),
            None,
        )
        .await;
        assert_eq!(got.status, 200, "{key}");
        assert_eq!(header(&got, "content-type"), "application/octet-stream");
        assert_eq!(
            header(&got, "content-disposition"),
            "attachment; filename*=UTF-8''x; filename=\"x\""
        );
        assert_eq!(header(&got, "x-content-type-options"), "nosniff");
    }
}

/// download は原本を octet-stream の添付で
#[tokio::test]
async fn download_returns_original_as_attachment() {
    let t = tenant().await;
    let doc = document(t, Doc::default()).await;
    let app = app().await;
    app.files.insert("t/orig.pdf", b"%PDF-orig");
    app.files.insert("t/redacted.jpg", b"jpeg");

    let got = call(
        &app,
        "GET",
        &format!("{DOCUMENTS}/{doc}/download"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.body, b"%PDF-orig");
    assert_eq!(header(&got, "content-type"), "application/octet-stream");
    assert_eq!(
        header(&got, "content-disposition"),
        "attachment; filename*=UTF-8''%E9%85%8D%E8%BB%8A%20%22%E7%A5%A8%22.pdf; filename=\"__ ___.pdf\""
    );
}

/// 文書が無ければ 404 (R2 は見ない)、R2 の binding が無ければ preview は 503・download は 500 (rust と同じ)、
/// R2 に無い・読めないなら 500。ログは固定の語だけ
#[tokio::test]
async fn file_routes_failures() {
    let t = tenant().await;
    let other = tenant().await;
    let doc = document(t, Doc::default()).await;
    let foreign = document(other, Doc::default()).await;
    let app = app().await;
    for route in ["preview", "download"] {
        for id in [foreign, Uuid::new_v4()] {
            let got = call(
                &app,
                "GET",
                &format!("{DOCUMENTS}/{id}/{route}"),
                Some(t),
                None,
            )
            .await;
            assert_eq!(got.status, 404, "{route}");
        }
        // R2 に無い
        let got = call(
            &app,
            "GET",
            &format!("{DOCUMENTS}/{doc}/{route}"),
            Some(t),
            None,
        )
        .await;
        assert_eq!(got.status, 500, "{route}");
    }
    app.files.insert("t/orig.pdf", b"x");
    app.files.insert("t/redacted.jpg", b"x");
    app.files.fail_get.store(true, Ordering::SeqCst);
    let got = call(
        &app,
        "GET",
        &format!("{DOCUMENTS}/{doc}/download"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(
        app.logs(),
        [
            "notify: preview failed: r2 (missing)",
            "notify: download failed: r2 (missing)",
            "notify: download failed: r2 (get)",
        ]
    );

    let no_r2 = build_with(
        rt_client().await,
        Options {
            files_configured: false,
            ..Options::default()
        },
    );
    let got = call(
        &no_r2,
        "GET",
        &format!("{DOCUMENTS}/{doc}/preview"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 503);
    let got = call(
        &no_r2,
        "GET",
        &format!("{DOCUMENTS}/{doc}/download"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(
        no_r2.logs(),
        [
            "notify: preview failed: r2 (not configured)",
            "notify: download failed: r2 (not configured)",
        ]
    );
}

/// メールの一覧は email_message_id ごとに新しい順。配信の状態は rust と同じ CASE。メールでない文書・ほかの tenant は入らない
#[tokio::test]
async fn list_emails_groups_by_message() {
    let t = tenant().await;
    let other = tenant().await;
    let mails: Vec<(Uuid, &[&str])> = vec![
        (Uuid::new_v4(), &["completed", "completed"]),
        (Uuid::new_v4(), &["completed", "in_progress", "failed"]),
        (Uuid::new_v4(), &["pending", "failed"]),
        (Uuid::new_v4(), &["completed", "pending"]),
    ];
    for (i, (m, statuses)) in mails.iter().enumerate() {
        for s in statuses.iter() {
            document(
                t,
                Doc {
                    email_message_id: Some(*m),
                    distribution_status: s,
                    // 1 通目が一番新しい
                    age_secs: (i as i64) * 60,
                    file_size_bytes: Some(100),
                    ..Doc::default()
                },
            )
            .await;
        }
    }
    document(t, Doc::default()).await;
    document(
        other,
        Doc {
            email_message_id: Some(Uuid::new_v4()),
            ..Doc::default()
        },
    )
    .await;
    let app = app().await;

    let got = call(&app, "GET", EMAILS, Some(t), None).await;
    assert_eq!(got.status, 200);
    let body = got.json();
    let rows = body.as_array().unwrap();
    let got_ids: Vec<&str> = rows
        .iter()
        .map(|r| r["email_message_id"].as_str().unwrap())
        .collect();
    let want_ids: Vec<String> = mails.iter().map(|(m, _)| m.to_string()).collect();
    assert_eq!(got_ids, want_ids);
    let statuses: Vec<&str> = rows
        .iter()
        .map(|r| r["distribution_status"].as_str().unwrap())
        .collect();
    assert_eq!(statuses, ["completed", "in_progress", "failed", "pending"]);
    assert_eq!(rows[1]["attachment_count"], 3);
    assert_eq!(rows[1]["total_size_bytes"], 300);
    assert_eq!(rows[0]["source_sender"], "from@example.com");
    assert_eq!(rows[0]["source_subject"], "件名");
    assert_eq!(rows[0]["source_received_at"], "2026-10-01T09:00:00Z");
}

/// メールの 1 件は見出しと添付 (古い順)。添付が無い・ほかの tenant のメールは 404
#[tokio::test]
async fn get_email_returns_header_and_documents() {
    let t = tenant().await;
    let other = tenant().await;
    let m = Uuid::new_v4();
    let first = document(
        t,
        Doc {
            email_message_id: Some(m),
            age_secs: 60,
            file_name: Some("b.pdf"),
            ..Doc::default()
        },
    )
    .await;
    let second = document(
        t,
        Doc {
            email_message_id: Some(m),
            file_name: Some("a.pdf"),
            ..Doc::default()
        },
    )
    .await;
    let foreign = Uuid::new_v4();
    document(
        other,
        Doc {
            email_message_id: Some(foreign),
            ..Doc::default()
        },
    )
    .await;
    let app = app().await;

    let got = call(&app, "GET", &format!("{EMAILS}/{m}"), Some(t), None).await;
    assert_eq!(got.status, 200);
    let body = got.json();
    assert_eq!(body["email_message_id"], m.to_string());
    assert_eq!(body["source_body_text"], "本文");
    assert_eq!(body["source_sender"], "from@example.com");
    assert_eq!(body["source_received_at"], "2026-10-01T09:00:00Z");
    assert_eq!(
        ids(&body["documents"]),
        [first.to_string(), second.to_string()]
    );
    let d = &body["documents"][0];
    assert_eq!(d["file_name"], "b.pdf");
    assert_eq!(d["r2_key"], "t/orig.pdf");
    assert_eq!(d["extraction_status"], "completed");
    assert_eq!(d["distribution_status"], "pending");
    assert_eq!(d["file_size_bytes"], 2048);

    for id in [foreign, Uuid::new_v4()] {
        let got = call(&app, "GET", &format!("{EMAILS}/{id}"), Some(t), None).await;
        assert_eq!(got.status, 404);
    }
}

/// DB の失敗はどの口も本文なしの 500。ログは固定の語と kind だけ
#[tokio::test]
async fn db_failures_are_500() {
    let t = tenant().await;
    let id = Uuid::new_v4();
    let paths = [
        (DOCUMENTS.to_owned(), "list_documents"),
        (format!("{DOCUMENTS}/{id}"), "get_document"),
        (format!("{DOCUMENTS}/{id}/preview"), "preview"),
        (format!("{DOCUMENTS}/{id}/download"), "download"),
        (EMAILS.to_owned(), "list_emails"),
        (format!("{EMAILS}/{id}"), "get_email"),
    ];
    for (path, what) in paths {
        let app = broken_app().await;
        let got = call(&app, "GET", &path, Some(t), None).await;
        assert_eq!(got.status, 500, "{path}");
        assert!(got.body.is_empty(), "{path}");
        let logs = app.logs();
        assert_eq!(logs.len(), 1, "{path}");
        assert!(
            logs[0].starts_with(&format!("notify: {what} failed: db (")),
            "{path}: {logs:?}"
        );
    }
}
