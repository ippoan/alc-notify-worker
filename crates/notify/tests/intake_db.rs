//! 文書の upload・メールの取り込み (ingest)・削除 (W8c-2 PR-3b) を、実 DB (`alc_api_rt` で接続) と R2・queue のモック
//! (tests/support の `MockStore`・`MockQueue`) で通す。接続先は `NOTIFY_TEST_ADMIN_DATABASE_URL` (無ければ失敗する)。
//! 期待値は rust-alc-api の `crates/alc-notify/src/documents.rs` の upload・`ingest.rs`・`email_documents.rs` の delete。
//!
//! 行の作成を落とすのに、ファイル名が `__fail_insert` で始まる行の INSERT を例外にする trigger を使う (ほかの行には掛からない)。

mod support;

use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::http::Request;
use base64::Engine;
use notify::jobs::{Job, JobKind};
use serde_json::{json, Value};
use support::*;
use tokio::sync::OnceCell;
use tower::ServiceExt;
use uuid::Uuid;

const UPLOAD: &str = "/api/notify/documents/upload";
const INGEST: &str = "/api/notify/ingest";
const BOUNDARY: &str = "notify-test-boundary";

async fn insert_trigger() {
    static ONCE: OnceCell<()> = OnceCell::const_new();
    ONCE.get_or_init(|| async {
        prepare().await;
        admin()
            .await
            .batch_execute(
                "CREATE OR REPLACE FUNCTION alc_api.test_fail_insert() RETURNS trigger LANGUAGE plpgsql AS $$ \
                 BEGIN \
                   IF NEW.file_name LIKE '\\_\\_fail\\_insert%' THEN RAISE EXCEPTION 'test failure'; END IF; \
                   RETURN NEW; \
                 END $$; \
                 DROP TRIGGER IF EXISTS test_fail_insert ON alc_api.notify_documents; \
                 CREATE TRIGGER test_fail_insert BEFORE INSERT ON alc_api.notify_documents \
                 FOR EACH ROW EXECUTE FUNCTION alc_api.test_fail_insert();",
            )
            .await
            .unwrap();
    })
    .await;
}

/// multipart の 1 部分 (`file` = (ファイル名, Content-Type)。`None` はテキストの欄)
fn part(file: Option<(&str, Option<&str>)>, body: &[u8]) -> Vec<u8> {
    let mut out = format!("--{BOUNDARY}\r\n").into_bytes();
    match file {
        Some((name, ct)) => {
            out.extend(
                format!("Content-Disposition: form-data; name=\"files\"; filename=\"{name}\"\r\n")
                    .bytes(),
            );
            if let Some(ct) = ct {
                out.extend(format!("Content-Type: {ct}\r\n").bytes());
            }
        }
        None => out.extend(b"Content-Disposition: form-data; name=\"note\"\r\n"),
    }
    out.extend(b"\r\n");
    out.extend(body);
    out.extend(b"\r\n");
    out
}

fn multipart(parts: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out: Vec<u8> = parts.concat();
    out.extend(format!("--{BOUNDARY}--\r\n").bytes());
    out
}

/// 生の本文で 1 リクエストを打つ
async fn raw(
    app: &App,
    uri: &str,
    headers: &[(&str, String)],
    content_type: &str,
    body: Vec<u8>,
) -> Got {
    let mut b = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type);
    for (k, v) in headers {
        b = b.header(*k, v);
    }
    let resp = app
        .router
        .clone()
        .oneshot(b.body(Body::from(body)).unwrap())
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

async fn upload(app: &App, tenant: Option<Uuid>, body: Vec<u8>) -> Got {
    let headers: Vec<(&str, String)> = tenant
        .map(|t| ("X-Tenant-ID", t.to_string()))
        .into_iter()
        .collect();
    raw(
        app,
        UPLOAD,
        &headers,
        &format!("multipart/form-data; boundary={BOUNDARY}"),
        body,
    )
    .await
}

/// 文書の行 (superuser で読む)
async fn doc_row(id: Uuid) -> Value {
    let r = admin()
        .await
        .query_one(
            "SELECT json_build_object('tenant_id', tenant_id, 'source_type', source_type, 'source_sender', source_sender, \
             'source_subject', source_subject, 'r2_key', r2_key, 'file_name', file_name, 'file_size_bytes', file_size_bytes, \
             'email_message_id', email_message_id, 'source_body_text', source_body_text, \
             'received', source_received_at IS NOT NULL, 'received_at', source_received_at, \
             'extraction_status', extraction_status, 'distribution_status', distribution_status, \
             'redaction_status', redaction_status)::text \
             FROM alc_api.notify_documents WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap();
    serde_json::from_str(&r.get::<_, String>(0)).unwrap()
}

async fn short_id(t: Uuid) -> String {
    admin()
        .await
        .query_one("SELECT short_id FROM alc_api.tenants WHERE id = $1", &[&t])
        .await
        .unwrap()
        .get(0)
}

fn ids(v: &Value) -> Vec<Uuid> {
    v["document_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d.as_str().unwrap().parse().unwrap())
        .collect()
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

// ---- upload ----

/// ファイルの部分だけを R2 に置いて行を作る (テキストの欄は無視)。PDF は黒塗りと抽出、ほかは抽出だけを積む
#[tokio::test]
async fn upload_stores_files_and_enqueues() {
    let t = tenant().await;
    let app = app().await;
    let body = multipart(vec![
        part(None, b"memo"),
        part(Some(("配車/票.PDF", Some("application/pdf"))), b"%PDF-1"),
        part(Some(("photo.png", None)), b"PNG"),
    ]);

    let got = upload(&app, Some(t), body).await;
    assert_eq!(got.status, 201);
    let v = got.json();
    assert_eq!(v["count"], 2);
    let ids = ids(&v);

    let pdf = doc_row(ids[0]).await;
    let key = pdf["r2_key"].as_str().unwrap().to_owned();
    let prefix = format!("{t}/manual/");
    assert!(
        key.starts_with(&prefix) && key.ends_with("/配車_票.PDF"),
        "{key}"
    );
    let batch = &key[prefix.len()..key.len() - "/配車_票.PDF".len()];
    assert!(batch.parse::<Uuid>().is_ok());
    assert_eq!(pdf["tenant_id"], t.to_string());
    assert_eq!(pdf["source_type"], "manual");
    assert_eq!(pdf["file_name"], "配車/票.PDF");
    assert_eq!(pdf["file_size_bytes"], 6);
    assert_eq!(pdf["received"], true);
    for col in [
        "extraction_status",
        "distribution_status",
        "redaction_status",
    ] {
        assert_eq!(pdf[col], "pending", "{col}");
    }
    assert_eq!(pdf["email_message_id"], Value::Null);
    let png = doc_row(ids[1]).await;
    assert_eq!(png["r2_key"], format!("{t}/manual/{batch}/photo.png"));

    // R2 は Content-Type 付き (無ければ octet-stream)
    assert_eq!(
        *app.files.puts.lock().unwrap(),
        [
            (key.clone(), "application/pdf".to_owned()),
            (
                format!("{t}/manual/{batch}/photo.png"),
                "application/octet-stream".to_owned()
            ),
        ]
    );
    assert_eq!(app.files.objects.lock().unwrap()[&key], b"%PDF-1");
    assert_eq!(
        app.jobs.sent(),
        [
            (Job::new(JobKind::Redact, t, ids[0]), 0),
            (Job::new(JobKind::Extract, t, ids[0]), 0),
            (Job::new(JobKind::Extract, t, ids[1]), 0),
        ]
    );
}

#[tokio::test]
async fn upload_rejects_bad_requests() {
    let t = tenant().await;
    let app = app().await;
    let pdf = || part(Some(("a.pdf", Some("application/pdf"))), b"%PDF");

    // 許していない拡張子・ファイルが 0 件・multipart が読めない・途中で切れた
    let cases: Vec<(&str, Vec<u8>, u16)> = vec![
        (
            "exe",
            multipart(vec![pdf(), part(Some(("a.exe", None)), b"MZ")]),
            400,
        ),
        ("no files", multipart(vec![part(None, b"x")]), 400),
        ("broken", b"not multipart".to_vec(), 400),
        (
            "truncated",
            [part(Some(("a.pdf", None)), b"%PDF")].concat(),
            400,
        ),
        ("21 files", multipart((0..21).map(|_| pdf()).collect()), 413),
        // 本文の上限 (20 MiB) を超えると読めずに 400 (rust と同じ)
        (
            "over body limit",
            multipart(vec![part(Some(("a.pdf", None)), &vec![b'x'; 21 << 20])]),
            400,
        ),
    ];
    for (label, body, want) in cases {
        assert_eq!(upload(&app, Some(t), body).await.status, want, "{label}");
    }
    // tenant のヘッダー無しは 401
    assert_eq!(upload(&app, None, multipart(vec![pdf()])).await.status, 401);
    assert!(app.files.puts.lock().unwrap().is_empty());
    assert!(app.jobs.sent().is_empty());
}

#[tokio::test]
async fn upload_failures_are_500() {
    let t = tenant().await;
    let body = || multipart(vec![part(Some(("a.pdf", None)), b"%PDF")]);

    // R2 の binding が無い
    let app = build_with(
        rt_client().await,
        Options {
            files_configured: false,
            ..Options::default()
        },
    );
    assert_eq!(upload(&app, Some(t), body()).await.status, 500);
    assert!(app
        .logs()
        .contains(&"notify: upload failed: storage".to_owned()));

    // R2 に置けない (行も作らない)
    let app = support::app().await;
    app.files.fail_put.store(true, Ordering::SeqCst);
    assert_eq!(upload(&app, Some(t), body()).await.status, 500);
    assert!(app
        .logs()
        .contains(&"notify: upload failed: r2 put".to_owned()));
    assert!(app.jobs.sent().is_empty());

    // DB に繋がらない (batch の id が作れない)
    let app = broken_app().await;
    assert_eq!(upload(&app, Some(t), body()).await.status, 500);
    assert!(app
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: upload failed: db (")));

    // 行を作れない
    insert_trigger().await;
    let app = support::app().await;
    let body = multipart(vec![part(Some(("__fail_insert.pdf", None)), b"%PDF")]);
    assert_eq!(upload(&app, Some(t), body).await.status, 500);
    assert!(app.jobs.sent().is_empty());

    // queue に積めなくても 201 (行は pending のまま)
    let app = support::app().await;
    app.jobs.fail.store(true, Ordering::SeqCst);
    let got = upload(
        &app,
        Some(t),
        multipart(vec![part(Some(("b.pdf", None)), b"%PDF")]),
    )
    .await;
    assert_eq!(got.status, 201);
    assert_eq!(
        doc_row(ids(&got.json())[0]).await["redaction_status"],
        "pending"
    );
    assert_eq!(
        app.logs()
            .iter()
            .filter(|l| *l == "notify: upload enqueue failed: queue")
            .count(),
        2
    );
}

// ---- ingest ----

/// short_id で tenant を引き、添付を R2 に置いて行を作る。tenant のヘッダーは要らない (付いていても見ない)
#[tokio::test]
async fn ingest_stores_attachments_and_enqueues() {
    let t = tenant().await;
    let other = tenant().await;
    let app = app().await;
    let body = json!({
        "tenant_short_id": format!(" {} ", short_id(t).await),
        "from": "fax@example.com",
        "subject": "配車依頼",
        "body_text": "本文",
        "body_html": "<p>本文</p>",
        "received_at": "2026-10-01T09:00:00Z",
        "attachments": [
            {"filename": "../依頼.pdf", "content_type": "application/pdf", "size": 4, "content_base64": b64(b"%PDF")},
            {"filename": "", "content_type": "text/plain", "content_base64": b64(b"hi")}
        ]
    });
    let got = call(&app, "POST", INGEST, Some(other), Some(body)).await;
    assert_eq!(got.status, 201);
    let v = got.json();
    assert_eq!(v["count"], 2);
    let mid: Uuid = v["email_message_id"].as_str().unwrap().parse().unwrap();
    let ids = ids(&v);

    let row = doc_row(ids[0]).await;
    assert_eq!(row["tenant_id"], t.to_string());
    assert_eq!(row["source_type"], "email");
    assert_eq!(row["source_sender"], "fax@example.com");
    assert_eq!(row["source_subject"], "配車依頼");
    assert_eq!(row["source_body_text"], "本文");
    assert_eq!(row["email_message_id"], mid.to_string());
    assert!(row["received_at"]
        .as_str()
        .unwrap()
        .starts_with("2026-10-01T09:00:00"));
    assert_eq!(row["r2_key"], format!("{t}/email/{mid}/__依頼.pdf"));
    assert_eq!(row["file_name"], "../依頼.pdf");
    assert_eq!(row["file_size_bytes"], 4);
    assert_eq!(row["extraction_status"], "pending");
    assert_eq!(
        doc_row(ids[1]).await["r2_key"],
        format!("{t}/email/{mid}/attachment")
    );
    assert_eq!(
        *app.files.puts.lock().unwrap(),
        [
            (
                format!("{t}/email/{mid}/__依頼.pdf"),
                "application/pdf".to_owned()
            ),
            (
                format!("{t}/email/{mid}/attachment"),
                "text/plain".to_owned()
            ),
        ]
    );
    assert_eq!(
        app.jobs.sent(),
        [
            (Job::new(JobKind::Redact, t, ids[0]), 0),
            (Job::new(JobKind::Extract, t, ids[0]), 0),
            (Job::new(JobKind::Extract, t, ids[1]), 0),
        ]
    );

    // received_at が無ければ今。tenant のヘッダー無しでも届く
    let body = json!({
        "tenant_short_id": short_id(t).await,
        "attachments": [{"filename": "a.pdf", "content_type": "application/pdf", "content_base64": b64(b"%PDF")}]
    });
    let got = call(&app, "POST", INGEST, None, Some(body)).await;
    assert_eq!(got.status, 201);
    let row = doc_row(ids_of(&got)[0]).await;
    assert_eq!(row["received"], true);
    assert_eq!(row["source_sender"], Value::Null);
}

fn ids_of(got: &Got) -> Vec<Uuid> {
    ids(&got.json())
}

#[tokio::test]
async fn ingest_rejects_bad_requests() {
    let t = tenant().await;
    let sid = short_id(t).await;
    let app = app().await;
    let att = |s: &str| json!({"filename": "a.pdf", "content_type": "application/pdf", "content_base64": s});
    let cases: Vec<(&str, Value, u16)> = vec![
        (
            "empty short_id",
            json!({"tenant_short_id": "  ", "attachments": [att("JVBERg==")]}),
            400,
        ),
        (
            "unknown short_id",
            json!({"tenant_short_id": "zzzzzzzz", "attachments": [att("JVBERg==")]}),
            404,
        ),
        ("no attachments", json!({"tenant_short_id": sid}), 400),
        (
            "21 attachments",
            json!({"tenant_short_id": sid, "attachments": vec![att("JVBERg=="); 21]}),
            413,
        ),
        (
            "bad base64",
            json!({"tenant_short_id": sid, "attachments": [att("!!!")]}),
            400,
        ),
        (
            "no short_id",
            json!({"attachments": [att("JVBERg==")]}),
            422,
        ),
    ];
    for (label, body, want) in cases {
        assert_eq!(
            call(&app, "POST", INGEST, None, Some(body)).await.status,
            want,
            "{label}"
        );
    }
    // 本文の上限 (20 MiB) を超えると 413 (rust と同じ)
    let big = format!(
        r#"{{"tenant_short_id":"{sid}","attachments":[{{"filename":"a.pdf","content_type":"x","content_base64":"{}"}}]}}"#,
        "A".repeat(21 << 20)
    );
    assert_eq!(
        raw(&app, INGEST, &[], "application/json", big.into_bytes())
            .await
            .status,
        413
    );
    assert!(app.files.puts.lock().unwrap().is_empty());

    // R2 の binding が無い
    let nofiles = build_with(
        rt_client().await,
        Options {
            files_configured: false,
            ..Options::default()
        },
    );
    let body = json!({"tenant_short_id": sid, "attachments": [att("JVBERg==")]});
    assert_eq!(
        call(&nofiles, "POST", INGEST, None, Some(body.clone()))
            .await
            .status,
        500
    );
    assert!(nofiles
        .logs()
        .contains(&"notify: ingest failed: storage".to_owned()));

    // DB に繋がらない
    let broken = broken_app().await;
    assert_eq!(
        call(&broken, "POST", INGEST, None, Some(body)).await.status,
        500
    );
    assert!(broken
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: ingest tenant failed: db (")));

    // 行を作れない
    insert_trigger().await;
    let body = json!({"tenant_short_id": sid, "attachments": [
        {"filename": "__fail_insert.pdf", "content_type": "application/pdf", "content_base64": "JVBERg=="}
    ]});
    assert_eq!(
        call(&app, "POST", INGEST, None, Some(body)).await.status,
        500
    );
    assert!(app
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: ingest failed: db (")));
    assert!(app.jobs.sent().is_empty());
}

// ---- DELETE ----

async fn document(t: Uuid, key: &str) -> Uuid {
    let id = Uuid::new_v4();
    let su = admin().await;
    su.execute(
        "INSERT INTO alc_api.notify_documents (id, tenant_id, source_type, r2_key, file_name) \
         VALUES ($1, $2, 'manual', $3, 'a.pdf')",
        &[&id, &t, &key],
    )
    .await
    .unwrap();
    let rid = Uuid::new_v4();
    su.execute(
        "INSERT INTO alc_api.notify_recipients (id, tenant_id, name, provider, lineworks_user_id) \
         VALUES ($1, $2, '宛先', 'lineworks', $3)",
        &[&rid, &t, &format!("lw-{rid}")],
    )
    .await
    .unwrap();
    su.execute(
        "INSERT INTO alc_api.notify_deliveries (tenant_id, document_id, recipient_id, provider) \
         VALUES ($1, $2, $3, 'lineworks')",
        &[&t, &id, &rid],
    )
    .await
    .unwrap();
    id
}

async fn count(sql: &str, id: Uuid) -> i64 {
    admin().await.query_one(sql, &[&id]).await.unwrap().get(0)
}

/// 行を消し (配信の記録は CASCADE)、R2 の原本も消す。204
#[tokio::test]
async fn delete_removes_row_and_file() {
    let t = tenant().await;
    let other = tenant().await;
    let id = document(t, "t/del.pdf").await;
    let app = app().await;
    app.files.insert("t/del.pdf", b"%PDF");
    let path = format!("/api/notify/documents/{id}");

    // 別の tenant・無い文書は 404 (消さない)
    assert_eq!(
        call(&app, "DELETE", &path, Some(other), None).await.status,
        404
    );
    let missing = format!("/api/notify/documents/{}", Uuid::new_v4());
    assert_eq!(
        call(&app, "DELETE", &missing, Some(t), None).await.status,
        404
    );
    assert_eq!(call(&app, "DELETE", &path, None, None).await.status, 401);

    let got = call(&app, "DELETE", &path, Some(t), None).await;
    assert_eq!(got.status, 204);
    assert!(got.body.is_empty());
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.notify_documents WHERE id = $1",
            id
        )
        .await,
        0
    );
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.notify_deliveries WHERE document_id = $1",
            id
        )
        .await,
        0
    );
    assert_eq!(*app.files.deletes.lock().unwrap(), ["t/del.pdf"]);
    assert!(app.files.objects.lock().unwrap().is_empty());
    // 消した後は 404
    assert_eq!(call(&app, "DELETE", &path, Some(t), None).await.status, 404);
}

/// R2 は best-effort (消せなくても・binding が無くても 204)。DB の失敗は 500
#[tokio::test]
async fn delete_r2_is_best_effort() {
    let t = tenant().await;
    let app = app().await;
    app.files.fail_delete.store(true, Ordering::SeqCst);
    let id = document(t, "t/keep.pdf").await;
    let got = call(
        &app,
        "DELETE",
        &format!("/api/notify/documents/{id}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    assert!(app
        .logs()
        .contains(&"notify: delete_document r2 delete failed".to_owned()));

    let nofiles = build_with(
        rt_client().await,
        Options {
            files_configured: false,
            ..Options::default()
        },
    );
    let id = document(t, "t/nofiles.pdf").await;
    let got = call(
        &nofiles,
        "DELETE",
        &format!("/api/notify/documents/{id}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    assert!(nofiles.files.deletes.lock().unwrap().is_empty());

    let broken = broken_app().await;
    let got = call(
        &broken,
        "DELETE",
        &format!("/api/notify/documents/{id}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 500);
    assert!(broken
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: delete_document failed: db (")));
}
