//! 抽出・黒塗りの背景処理 (Queues の consumer の中身) と recompute の口 (W8c-2) を、実 DB (`alc_api_rt` で接続) と
//! R2・alc-redact (auth-worker の RPC)・realtime-bus・queue のモックで通す。接続先は `NOTIFY_TEST_ADMIN_DATABASE_URL`
//! (無ければ失敗する。tests/support)。期待値は rust-alc-api の `crates/alc-notify/src/background_extract.rs`・
//! `background_redaction.rs`・`documents.rs` の recompute。
//!
//! DB の UPDATE を 1 行だけ落とすのに、`source_subject` が `__fail:<状態>` の文書で、更新後の状態が `<状態>` になる UPDATE を
//! 例外にする trigger を使う (ほかのテストの行には掛からない)。

mod support;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use notify::background::{
    Background, Broadcaster, Outcome, RedactApi, RedactReply, RedactUnreachable, MAX_ATTEMPTS,
};
use notify::jobs::{Job, JobKind};
use notify::store::{FileStore, SharedStore, StoreError};
use serde_json::{json, Value};
use support::*;
use tokio::sync::OnceCell;
use uuid::Uuid;

const PDF: &[u8] = b"%PDF-1.5 test";
const JPEG: &[u8] = b"\xFF\xD8\xFF test jpeg";

/// `source_subject` が `__fail:<状態>` の行を、`<状態>` へ更新する UPDATE で落とす trigger (1 回だけ作る)
async fn fail_trigger() {
    static ONCE: OnceCell<()> = OnceCell::const_new();
    ONCE.get_or_init(|| async {
        prepare().await;
        admin()
            .await
            .batch_execute(
                "CREATE OR REPLACE FUNCTION alc_api.test_fail_update() RETURNS trigger LANGUAGE plpgsql AS $$ \
                 BEGIN \
                   IF OLD.source_subject LIKE '__fail:%' AND \
                      (NEW.redaction_status = substr(OLD.source_subject, 8) OR \
                       NEW.extraction_status = substr(OLD.source_subject, 8)) THEN \
                     RAISE EXCEPTION 'test failure'; \
                   END IF; \
                   RETURN NEW; \
                 END $$; \
                 DROP TRIGGER IF EXISTS test_fail_update ON alc_api.notify_documents; \
                 CREATE TRIGGER test_fail_update BEFORE UPDATE ON alc_api.notify_documents \
                 FOR EACH ROW EXECUTE FUNCTION alc_api.test_fail_update();",
            )
            .await
            .unwrap();
    })
    .await;
}

struct Doc {
    file_name: &'static str,
    extraction_status: &'static str,
    redaction_status: &'static str,
    /// `__fail:<状態>` で、その状態への UPDATE を落とす
    subject: &'static str,
    extracted_data: Option<Value>,
}

impl Default for Doc {
    fn default() -> Self {
        Self {
            file_name: "配車.PDF",
            extraction_status: "pending",
            redaction_status: "pending",
            subject: "件名",
            extracted_data: Some(json!({"logistics": {"notes": "old"}, "other": 1})),
        }
    }
}

/// 文書を 1 つ作る (superuser で)。r2_key は `<tenant>/manual/<id>/x.pdf`
async fn document(t: Uuid, d: Doc) -> (Uuid, String) {
    fail_trigger().await;
    let id = Uuid::new_v4();
    let key = format!("{t}/manual/{id}/x.pdf");
    let data = d.extracted_data.map(|v| v.to_string());
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_documents (id, tenant_id, source_type, source_subject, r2_key, file_name, \
             extracted_data, extraction_status, redaction_status, extraction_error, redaction_error) \
             VALUES ($1, $2, 'manual', $3, $4, $5, $6::text::jsonb, $7, $8, 'old error', 'old error')",
            &[
                &id,
                &t,
                &d.subject,
                &key,
                &d.file_name,
                &data,
                &d.extraction_status,
                &d.redaction_status,
            ],
        )
        .await
        .unwrap();
    (id, key)
}

/// 文書の状態の列 (superuser で読む)
#[derive(Debug)]
struct Row {
    extraction_status: String,
    extraction_error: Option<String>,
    extracted_data: Option<Value>,
    redaction_status: String,
    redaction_error: Option<String>,
    redacted_r2_key: Option<String>,
    redactions_applied: Option<i32>,
    timing: [Option<i32>; 5],
}

async fn row(id: Uuid) -> Row {
    let r = admin()
        .await
        .query_one(
            "SELECT extraction_status, extraction_error, extracted_data::text, redaction_status, redaction_error, \
             redacted_r2_key, redactions_applied, redact_dl_ms, redact_llm_ms, redact_render_ms, redact_upload_ms, \
             redact_total_ms FROM alc_api.notify_documents WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap();
    Row {
        extraction_status: r.get(0),
        extraction_error: r.get(1),
        extracted_data: r
            .get::<_, Option<String>>(2)
            .map(|s| serde_json::from_str(&s).unwrap()),
        redaction_status: r.get(3),
        redaction_error: r.get(4),
        redacted_r2_key: r.get(5),
        redactions_applied: r.get(6),
        timing: [r.get(7), r.get(8), r.get(9), r.get(10), r.get(11)],
    }
}

/// R2 の代わり。`kill_on_get` で読みの間に DB の backend を落とす
#[derive(Default)]
struct Store {
    objects: StdMutex<std::collections::HashMap<String, Vec<u8>>>,
    puts: StdMutex<Vec<(String, String, Vec<u8>)>>,
    fail_get: AtomicBool,
    fail_put: AtomicBool,
    kill_on_get: StdMutex<Option<i32>>,
}

impl FileStore for Store {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        let kill = self.kill_on_get.lock().unwrap().take();
        Box::pin(async move {
            if let Some(pid) = kill {
                terminate(pid).await;
            }
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(StoreError("get"));
            }
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or(StoreError("missing"))
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if self.fail_put.load(Ordering::SeqCst) {
                return Err(StoreError("put"));
            }
            self.puts
                .lock()
                .unwrap()
                .push((key.to_owned(), content_type.to_owned(), bytes));
            Ok(())
        })
    }

    fn delete<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}

/// alc-redact (auth-worker の RPC) の代わり。応答を順に返す (尽きたら 200 の既定)
#[derive(Default)]
struct Redact {
    replies: StdMutex<VecDeque<Result<RedactReply, RedactUnreachable>>>,
    /// (method, pdf, self_company)
    calls: StdMutex<Vec<(&'static str, Vec<u8>, String)>>,
}

impl Redact {
    fn then(&self, status: u16, body: &str, headers: &[(&str, &str)]) {
        self.replies.lock().unwrap().push_back(Ok(RedactReply {
            status,
            body: body.as_bytes().to_vec(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }));
    }

    fn then_code(&self, status: u16, code: &str) {
        self.then(
            status,
            &json!({"error": code, "detail": format!("{code} detail")}).to_string(),
            &[],
        );
    }

    fn unreachable(&self) {
        self.replies
            .lock()
            .unwrap()
            .push_back(Err(RedactUnreachable));
    }

    fn calls(&self) -> Vec<(&'static str, Vec<u8>, String)> {
        self.calls.lock().unwrap().clone()
    }

    fn next(&self, default: &[u8]) -> Result<RedactReply, RedactUnreachable> {
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(RedactReply {
                status: 200,
                body: default.to_vec(),
                headers: Vec::new(),
            }))
    }
}

impl RedactApi for Redact {
    fn redact(&self, pdf: Vec<u8>) -> BoxFuture<'_, Result<RedactReply, RedactUnreachable>> {
        self.calls
            .lock()
            .unwrap()
            .push(("redact", pdf, String::new()));
        let r = self.next(JPEG);
        Box::pin(async move { r })
    }

    fn extract(
        &self,
        pdf: Vec<u8>,
        self_company: String,
    ) -> BoxFuture<'_, Result<RedactReply, RedactUnreachable>> {
        self.calls
            .lock()
            .unwrap()
            .push(("extract", pdf, self_company));
        let r = self.next(br#"{"logistics":{}}"#);
        Box::pin(async move { r })
    }
}

/// realtime-bus の代わり (送った JSON を残す)
struct Bus {
    sent: StdMutex<Vec<Value>>,
    reply: StdMutex<Result<u16, ()>>,
}

impl Broadcaster for Bus {
    fn broadcast(&self, body: Vec<u8>) -> BoxFuture<'_, Result<u16, ()>> {
        self.sent
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&body).unwrap());
        let r = *self.reply.lock().unwrap();
        Box::pin(async move { r })
    }
}

struct Bg {
    bg: Background,
    store: Arc<Store>,
    redact: Arc<Redact>,
    bus: Arc<Bus>,
    logs: Arc<StdMutex<Vec<String>>>,
}

impl Bg {
    fn logs(&self) -> Vec<String> {
        self.logs.lock().unwrap().clone()
    }

    fn sent(&self) -> Vec<Value> {
        self.bus.sent.lock().unwrap().clone()
    }

    /// `key` に原本を置く
    fn put_pdf(&self, key: &str) {
        self.store
            .objects
            .lock()
            .unwrap()
            .insert(key.to_owned(), PDF.to_vec());
    }

    async fn run(&self, kind: JobKind, t: Uuid, id: Uuid) -> Outcome {
        self.bg.run(Job::new(kind, t, id)).await
    }

    async fn run_last(&self, kind: JobKind, t: Uuid, id: Uuid) -> Outcome {
        self.bg
            .run(Job {
                attempt: MAX_ATTEMPTS - 1,
                ..Job::new(kind, t, id)
            })
            .await
    }
}

/// `files` = R2 の binding が在るか、`bus` = realtime-bus の binding と secret が在るか
async fn bg_with(pg: alc_worker_db::PgClient, files: bool, bus: bool) -> Bg {
    let store = Arc::new(Store::default());
    let redact = Arc::new(Redact::default());
    let b = Arc::new(Bus {
        sent: StdMutex::new(Vec::new()),
        reply: StdMutex::new(Ok(200)),
    });
    let logs = Arc::new(StdMutex::new(Vec::new()));
    let sink = logs.clone();
    // 呼ぶたびに 10 ms 進む時計
    let clock = Arc::new(AtomicU64::new(1_000));
    let bg = Background {
        pg: Arc::new(Mutex::new(pg)),
        files: files.then(|| store.clone() as SharedStore),
        redact: redact.clone(),
        bus: bus.then(|| b.clone() as Arc<dyn Broadcaster>),
        log: Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
        now_ms: Arc::new(move || clock.fetch_add(10, Ordering::SeqCst)),
    };
    Bg {
        bg,
        store,
        redact,
        bus: b,
        logs,
    }
}

async fn bg() -> Bg {
    bg_with(rt_client().await, true, true).await
}

fn requeued(o: &Outcome, attempt: u32, delay: u32) -> bool {
    matches!(o, Outcome::Requeue { job, delay_secs } if job.attempt == attempt && *delay_secs == delay)
}

async fn set_tenant_name(t: Uuid, name: &str) {
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenants SET name = $1 WHERE id = $2",
            &[&name, &t],
        )
        .await
        .unwrap();
}

// ---- recompute の口 ----

#[tokio::test]
async fn redact_recompute_resets_and_enqueues() {
    let t = tenant().await;
    let (id, _) = document(
        t,
        Doc {
            redaction_status: "failed",
            ..Doc::default()
        },
    )
    .await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.notify_documents SET redacted_r2_key = 'k', redactions_applied = 2, redact_total_ms = 5 \
             WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap();
    let app = app().await;
    let path = format!("/api/notify/documents/{id}/redact-recompute");

    let got = call(&app, "POST", &path, Some(t), None).await;
    assert_eq!(got.status, 202);
    assert!(got.body.is_empty());
    let r = row(id).await;
    assert_eq!(r.redaction_status, "pending");
    assert_eq!(
        (
            r.redaction_error,
            r.redacted_r2_key,
            r.redactions_applied,
            r.timing[4]
        ),
        (None, None, None, None)
    );
    assert_eq!(app.jobs.sent(), [(Job::new(JobKind::Redact, t, id), 0)]);

    // 別の tenant・無い文書は 404 (積まない)
    let other = tenant().await;
    assert_eq!(
        call(&app, "POST", &path, Some(other), None).await.status,
        404
    );
    let missing = format!("/api/notify/documents/{}/redact-recompute", Uuid::new_v4());
    assert_eq!(
        call(&app, "POST", &missing, Some(t), None).await.status,
        404
    );
    // tenant のヘッダー無しは 401
    assert_eq!(call(&app, "POST", &path, None, None).await.status, 401);
    assert_eq!(app.jobs.sent().len(), 1);

    // 積めなければ 500
    app.jobs.fail.store(true, Ordering::SeqCst);
    assert_eq!(call(&app, "POST", &path, Some(t), None).await.status, 500);
    assert!(app
        .logs()
        .contains(&"notify: redact_recompute failed: queue".to_owned()));
}

#[tokio::test]
async fn extract_recompute_resets_and_enqueues() {
    let t = tenant().await;
    let (id, _) = document(
        t,
        Doc {
            extraction_status: "failed",
            ..Doc::default()
        },
    )
    .await;
    let app = app().await;
    let path = format!("/api/notify/documents/{id}/extract-recompute");

    assert_eq!(call(&app, "POST", &path, Some(t), None).await.status, 202);
    let r = row(id).await;
    assert_eq!(
        (r.extraction_status.as_str(), r.extraction_error),
        ("pending", None)
    );
    assert_eq!(app.jobs.sent(), [(Job::new(JobKind::Extract, t, id), 0)]);

    let missing = format!("/api/notify/documents/{}/extract-recompute", Uuid::new_v4());
    assert_eq!(
        call(&app, "POST", &missing, Some(t), None).await.status,
        404
    );

    // 戻せなくても積む (rust と同じ)
    let (stuck, _) = document(
        t,
        Doc {
            extraction_status: "failed",
            subject: "__fail:pending",
            ..Doc::default()
        },
    )
    .await;
    let path = format!("/api/notify/documents/{stuck}/extract-recompute");
    assert_eq!(call(&app, "POST", &path, Some(t), None).await.status, 202);
    assert_eq!(row(stuck).await.extraction_status, "failed");
    assert_eq!(app.jobs.sent().len(), 2);
    assert!(app
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: reset_extraction failed: db (")));

    // 黒塗りは戻せなければ 500 (積まない)
    let (stuck, _) = document(
        t,
        Doc {
            subject: "__fail:pending",
            redaction_status: "failed",
            extraction_status: "completed",
            ..Doc::default()
        },
    )
    .await;
    let path = format!("/api/notify/documents/{stuck}/redact-recompute");
    assert_eq!(call(&app, "POST", &path, Some(t), None).await.status, 500);
    assert_eq!(app.jobs.sent().len(), 2);
}

#[tokio::test]
async fn recompute_db_failure_is_500() {
    let app = broken_app().await;
    let t = Uuid::new_v4();
    for p in ["redact-recompute", "extract-recompute"] {
        let path = format!("/api/notify/documents/{}/{p}", Uuid::new_v4());
        assert_eq!(call(&app, "POST", &path, Some(t), None).await.status, 500);
    }
    let logs = app.logs();
    assert!(logs
        .iter()
        .any(|l| l.starts_with("notify: redact_recompute failed: db (")));
    assert!(logs
        .iter()
        .any(|l| l.starts_with("notify: extract_recompute failed: db (")));
    assert!(app.jobs.sent().is_empty());
}

// ---- 抽出 ----

/// PDF を alc-redact の /extract に送り、logistics を差し替えて completed (他のキーは残す)。自社名を渡す
#[tokio::test]
async fn extract_completes_with_logistics_and_self_company() {
    let t = tenant().await;
    set_tenant_name(t, "テスト運輸").await;
    let (id, key) = document(t, Doc::default()).await;
    let b = bg().await;
    b.put_pdf(&key);
    b.redact.then(
        200,
        r#"{"logistics":{"loading_place":"イオン","contact_phone":"03"}}"#,
        &[],
    );

    assert_eq!(b.run(JobKind::Extract, t, id).await, Outcome::Ack);
    let r = row(id).await;
    assert_eq!(r.extraction_status, "completed");
    assert_eq!(
        r.extracted_data,
        Some(json!({"logistics": {"loading_place": "イオン", "contact_phone": "03"}, "other": 1}))
    );
    assert_eq!(
        b.redact.calls(),
        [("extract", PDF.to_vec(), "テスト運輸".to_owned())]
    );
    // 抽出は push しない (rust と同じ)
    assert!(b.sent().is_empty());

    // completed になった後の再配送は何もしない
    assert_eq!(b.run(JobKind::Extract, t, id).await, Outcome::Ack);
    assert_eq!(b.redact.calls().len(), 1);
}

/// 抽出するものが無い (PDF 以外・alc-redact の not_pdf・gemini_key_missing・空の logistics) は logistics を消して completed
#[tokio::test]
async fn extract_without_logistics_completes() {
    let t = tenant().await;
    let b = bg().await;

    let (id, _) = document(
        t,
        Doc {
            file_name: "写真.jpg",
            ..Doc::default()
        },
    )
    .await;
    assert_eq!(b.run(JobKind::Extract, t, id).await, Outcome::Ack);
    let r = row(id).await;
    assert_eq!(r.extraction_status, "completed");
    assert_eq!(r.extracted_data, Some(json!({"other": 1})));
    assert!(b.redact.calls().is_empty());

    for code in ["not_pdf", "gemini_key_missing"] {
        let (id, key) = document(t, Doc::default()).await;
        b.put_pdf(&key);
        b.redact.then_code(503, code);
        assert_eq!(b.run(JobKind::Extract, t, id).await, Outcome::Ack, "{code}");
        let r = row(id).await;
        assert_eq!(r.extraction_status, "completed", "{code}");
        assert_eq!(r.extracted_data, Some(json!({"other": 1})), "{code}");
    }

    // 空の logistics・extracted_data が無い文書
    let (id, key) = document(
        t,
        Doc {
            extracted_data: None,
            ..Doc::default()
        },
    )
    .await;
    b.put_pdf(&key);
    b.redact.then(200, r#"{"logistics":{"notes":null}}"#, &[]);
    assert_eq!(b.run(JobKind::Extract, t, id).await, Outcome::Ack);
    assert_eq!(row(id).await.extracted_data, Some(json!({})));
    // 自社名は tenants.name
    assert_eq!(
        b.redact.calls().last().unwrap().2,
        format!("notify-test-{t}")
    );
}

/// 失敗を書いて ack する (Gemini を呼び直さない)。接頭辞は rust と同じ
#[tokio::test]
async fn extract_failures_are_written_and_acked() {
    let t = tenant().await;
    let b = bg().await;
    let cases: Vec<(u16, &str, &str)> = vec![
        (
            502,
            r#"{"error":"gemini","detail":"gemini status 402: pay"}"#,
            "gemini: gemini status 402: pay",
        ),
        (
            502,
            r#"{"error":"parse","detail":"logistics json parse: x"}"#,
            "parse: logistics json parse: x",
        ),
        (
            400,
            r#"{"error":"bad_request","detail":"empty body"}"#,
            "gemini: empty body",
        ),
        (403, "forbidden", "gemini: upstream status 403"),
        (200, "not json", "parse: invalid redact service response"),
        (
            200,
            r#"{"logistics":"x"}"#,
            "parse: invalid redact service response",
        ),
    ];
    for (status, body, want) in cases {
        let (id, key) = document(t, Doc::default()).await;
        b.put_pdf(&key);
        b.redact.then(status, body, &[]);
        assert_eq!(b.run(JobKind::Extract, t, id).await, Outcome::Ack, "{want}");
        let r = row(id).await;
        assert_eq!(r.extraction_status, "failed", "{want}");
        assert_eq!(r.extraction_error.as_deref(), Some(want));
        // 前の logistics はそのまま
        assert_eq!(
            r.extracted_data.unwrap()["logistics"],
            json!({"notes": "old"})
        );
    }
    assert!(b
        .logs()
        .contains(&"notify: extract failed: gemini".to_owned()));

    // R2 に無い
    let (id, _) = document(t, Doc::default()).await;
    assert_eq!(b.run(JobKind::Extract, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.extraction_error.as_deref(),
        Some("r2 download: missing")
    );

    // R2 の binding が無い
    let nofiles = bg_with(rt_client().await, false, true).await;
    let (id, _) = document(t, Doc::default()).await;
    assert_eq!(nofiles.run(JobKind::Extract, t, id).await, Outcome::Ack);
    let r = row(id).await;
    assert_eq!(r.extraction_status, "failed");
    assert_eq!(
        r.extraction_error.as_deref(),
        Some("notify_storage not configured")
    );
    assert!(nofiles
        .logs()
        .contains(&"notify: extract failed: storage".to_owned()));

    // 失敗を書けなくても ack (ログだけ)
    let (id, key) = document(
        t,
        Doc {
            subject: "__fail:failed",
            redaction_status: "completed",
            ..Doc::default()
        },
    )
    .await;
    b.put_pdf(&key);
    b.redact.then_code(502, "gemini");
    assert_eq!(b.run(JobKind::Extract, t, id).await, Outcome::Ack);
    assert_eq!(row(id).await.extraction_status, "pending");
    assert!(b
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: extract fail update failed: db (")));
}

/// 一時的な失敗は attempt を進めて送り直し、最後の回は失敗を書いて ack
#[tokio::test]
async fn extract_transient_failures_requeue_then_fail() {
    let t = tenant().await;
    let b = bg().await;

    // R2 の読みの失敗
    let (id, _) = document(t, Doc::default()).await;
    b.store.fail_get.store(true, Ordering::SeqCst);
    let o = b.run(JobKind::Extract, t, id).await;
    assert!(requeued(&o, 1, 30), "{o:?}");
    assert_eq!(row(id).await.extraction_status, "pending");
    assert!(b
        .logs()
        .contains(&"notify: extract retry: download".to_owned()));
    let o =
        b.bg.run(Job {
            attempt: 1,
            ..Job::new(JobKind::Extract, t, id)
        })
        .await;
    assert!(requeued(&o, 2, 60), "{o:?}");
    assert_eq!(b.run_last(JobKind::Extract, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.extraction_error.as_deref(),
        Some("r2 download: get")
    );
    b.store.fail_get.store(false, Ordering::SeqCst);

    // RPC の失敗・auth-worker の口のコード・alc-redact 自体の 5xx
    let (id, key) = document(t, Doc::default()).await;
    b.put_pdf(&key);
    b.redact.unreachable();
    assert!(requeued(&b.run(JobKind::Extract, t, id).await, 1, 30));
    for code in [
        "redact_unconfigured",
        "upstream_auth",
        "upstream_unreachable",
    ] {
        b.redact.then_code(503, code);
        assert!(
            requeued(&b.run(JobKind::Extract, t, id).await, 1, 30),
            "{code}"
        );
    }
    b.redact.then(503, "Service Unavailable", &[]);
    assert!(requeued(&b.run(JobKind::Extract, t, id).await, 1, 30));
    assert_eq!(row(id).await.extraction_status, "pending");

    b.redact.unreachable();
    assert_eq!(b.run_last(JobKind::Extract, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.extraction_error.as_deref(),
        Some("gemini: redact service unreachable")
    );
    let (id, key) = document(t, Doc::default()).await;
    b.put_pdf(&key);
    b.redact.then_code(502, "upstream_auth");
    assert_eq!(b.run_last(JobKind::Extract, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.extraction_error.as_deref(),
        Some("gemini: redact service unavailable (upstream_auth)")
    );

    // completed を書けない (DB) は送り直す。最後の回はログだけ
    let (id, key) = document(
        t,
        Doc {
            subject: "__fail:completed",
            redaction_status: "failed",
            ..Doc::default()
        },
    )
    .await;
    b.put_pdf(&key);
    assert!(requeued(&b.run(JobKind::Extract, t, id).await, 1, 30));
    assert!(b
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: extract update failed: db (")));
    assert_eq!(b.run_last(JobKind::Extract, t, id).await, Outcome::Ack);
    assert!(b
        .logs()
        .contains(&"notify: extract gave up: update".to_owned()));
}

/// 文書が無い・DB に繋がらない・読みの途中で DB が切れる
#[tokio::test]
async fn extract_missing_document_and_db_failures() {
    let t = tenant().await;
    let b = bg().await;
    assert_eq!(
        b.run(JobKind::Extract, t, Uuid::new_v4()).await,
        Outcome::Ack
    );
    assert!(b
        .logs()
        .contains(&"notify: extract: document not found".to_owned()));

    let broken = bg_with(broken_client().await, true, true).await;
    let o = broken.run(JobKind::Extract, t, Uuid::new_v4()).await;
    assert!(requeued(&o, 1, 30));
    assert!(broken
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: extract get failed: db (")));
    assert_eq!(
        broken.run_last(JobKind::Redact, t, Uuid::new_v4()).await,
        Outcome::Ack
    );
    assert!(broken
        .logs()
        .contains(&"notify: redact gave up: get".to_owned()));

    // 原本を読む間に DB が切れる: 自社名は取れずに空で呼び、completed を書けずに送り直す
    let (id, key) = document(t, Doc::default()).await;
    let mut pg = rt_client().await;
    let pid = backend_pid(&mut pg).await;
    let killed = bg_with(pg, true, true).await;
    killed.put_pdf(&key);
    *killed.store.kill_on_get.lock().unwrap() = Some(pid);
    assert!(requeued(&killed.run(JobKind::Extract, t, id).await, 1, 30));
    assert_eq!(killed.redact.calls()[0].2, "");
    assert!(killed
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: extract tenant name failed: db (")));
}

// ---- 黒塗り ----

/// PDF を alc-redact の /redact に送り、JPEG を `{tenant}/redacted/{doc}.jpg` に置いて completed。所要時間を残し、push する
#[tokio::test]
async fn redact_completes_and_broadcasts() {
    let t = tenant().await;
    let (id, key) = document(t, Doc::default()).await;
    let b = bg().await;
    b.put_pdf(&key);
    b.redact.then(
        200,
        "JPEGDATA",
        &[
            ("x-redactions-applied", "2"),
            ("x-redact-llm-ms", "1500"),
            ("x-redact-render-ms", "300"),
            ("x-redact-total-ms", "1900"),
        ],
    );

    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    let r = row(id).await;
    let want_key = format!("{t}/redacted/{id}.jpg");
    assert_eq!(r.redaction_status, "completed");
    assert_eq!(r.redaction_error, None);
    assert_eq!(r.redacted_r2_key.as_deref(), Some(want_key.as_str()));
    assert_eq!(r.redactions_applied, Some(2));
    // dl・llm・render・upload・total (時計は呼ぶたびに 10 ms 進む)
    assert_eq!(
        r.timing,
        [Some(10), Some(1500), Some(300), Some(10), Some(40)]
    );
    assert_eq!(
        *b.store.puts.lock().unwrap(),
        [(want_key, "image/jpeg".to_owned(), b"JPEGDATA".to_vec())]
    );
    assert_eq!(b.redact.calls(), [("redact", PDF.to_vec(), String::new())]);
    assert_eq!(
        b.sent(),
        [
            json!({"tenant_id": t, "document_id": id, "status": "completed", "redactions_applied": 2})
        ]
    );

    // 終わった後の再配送は何もしない
    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    assert_eq!(b.redact.calls().len(), 1);

    // processing (前の配送の途中) からは進める。ヘッダーが無ければ 0
    let (id, key) = document(
        t,
        Doc {
            redaction_status: "processing",
            ..Doc::default()
        },
    )
    .await;
    b.put_pdf(&key);
    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    let r = row(id).await;
    assert_eq!(
        (r.redaction_status.as_str(), r.redactions_applied),
        ("completed", Some(0))
    );
    assert_eq!(r.timing[1], Some(0));
}

/// skipped (PDF 以外・not_pdf・gemini_key_missing) と failed (接頭辞は rust と同じ) を書いて push し、ack
#[tokio::test]
async fn redact_skips_and_failures_are_written_and_broadcast() {
    let t = tenant().await;
    let b = bg().await;

    let (id, _) = document(
        t,
        Doc {
            file_name: "x.xlsx",
            ..Doc::default()
        },
    )
    .await;
    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    let r = row(id).await;
    assert_eq!(
        (r.redaction_status.as_str(), r.redaction_error),
        ("skipped", None)
    );
    assert_eq!(
        b.sent().last().unwrap(),
        &json!({"tenant_id": t, "document_id": id, "status": "skipped"})
    );

    for code in ["not_pdf", "gemini_key_missing"] {
        let (id, key) = document(t, Doc::default()).await;
        b.put_pdf(&key);
        b.redact.then_code(503, code);
        assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack, "{code}");
        assert_eq!(row(id).await.redaction_status, "skipped", "{code}");
    }

    let cases: Vec<(u16, &str, &str)> = vec![
        (502, "gemini", "gemini: gemini detail"),
        (500, "apply", "apply: apply detail"),
        (400, "bad_request", "apply: bad_request detail"),
    ];
    for (status, code, want) in cases {
        let (id, key) = document(t, Doc::default()).await;
        b.put_pdf(&key);
        b.redact.then_code(status, code);
        assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack, "{code}");
        let r = row(id).await;
        assert_eq!(r.redaction_status, "failed", "{code}");
        assert_eq!(r.redaction_error.as_deref(), Some(want));
        assert_eq!(
            b.sent().last().unwrap(),
            &json!({"tenant_id": t, "document_id": id, "status": "failed", "redaction_error": want})
        );
    }
    let (id, key) = document(t, Doc::default()).await;
    b.put_pdf(&key);
    b.redact.then(401, "unauthorized", &[]);
    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.redaction_error.as_deref(),
        Some("gemini: upstream status 401")
    );

    // R2 に無い
    let (id, _) = document(t, Doc::default()).await;
    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.redaction_error.as_deref(),
        Some("r2 download: missing")
    );
    assert!(b
        .logs()
        .contains(&"notify: redact failed: r2 download".to_owned()));

    // R2 の binding が無い。realtime-bus が無ければ push しない
    let nofiles = bg_with(rt_client().await, false, false).await;
    let (id, _) = document(t, Doc::default()).await;
    assert_eq!(nofiles.run(JobKind::Redact, t, id).await, Outcome::Ack);
    let r = row(id).await;
    assert_eq!(r.redaction_status, "failed");
    assert_eq!(
        r.redaction_error.as_deref(),
        Some("notify_storage not configured")
    );
    assert!(nofiles.sent().is_empty());
    assert!(nofiles
        .logs()
        .contains(&"notify: redact failed: storage".to_owned()));

    // 終わりの状態を書けなくても push して ack (ログだけ)
    let (id, _) = document(
        t,
        Doc {
            file_name: "x.png",
            subject: "__fail:skipped",
            extraction_status: "completed",
            ..Doc::default()
        },
    )
    .await;
    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    assert_eq!(row(id).await.redaction_status, "pending");
    assert!(b
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: redact status update failed: db (")));
}

/// 一時的な失敗は送り直す (状態は processing のまま)。最後の回は failed を書いて push
#[tokio::test]
async fn redact_transient_failures_requeue_then_fail() {
    let t = tenant().await;
    let b = bg().await;

    // R2 の読み
    let (id, _) = document(t, Doc::default()).await;
    b.store.fail_get.store(true, Ordering::SeqCst);
    assert!(requeued(&b.run(JobKind::Redact, t, id).await, 1, 30));
    assert_eq!(row(id).await.redaction_status, "processing");
    assert!(b.sent().is_empty());
    assert_eq!(b.run_last(JobKind::Redact, t, id).await, Outcome::Ack);
    let r = row(id).await;
    assert_eq!(
        (r.redaction_status.as_str(), r.redaction_error.as_deref()),
        ("failed", Some("r2 download: get"))
    );
    assert_eq!(b.sent().len(), 1);
    b.store.fail_get.store(false, Ordering::SeqCst);

    // RPC・auth-worker の口のコード・alc-redact 自体の 5xx
    let (id, key) = document(t, Doc::default()).await;
    b.put_pdf(&key);
    b.redact.unreachable();
    assert!(requeued(&b.run(JobKind::Redact, t, id).await, 1, 30));
    b.redact.then_code(503, "redact_unconfigured");
    assert!(requeued(&b.run(JobKind::Redact, t, id).await, 1, 30));
    b.redact.then(500, r#"{"error":"internal"}"#, &[]);
    assert!(requeued(&b.run(JobKind::Redact, t, id).await, 1, 30));
    assert_eq!(row(id).await.redaction_status, "processing");
    b.redact.then(503, "", &[]);
    assert_eq!(b.run_last(JobKind::Redact, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.redaction_error.as_deref(),
        Some("gemini: redact service unavailable (status 503)")
    );
    b.redact.unreachable();
    let (id, key) = document(t, Doc::default()).await;
    b.put_pdf(&key);
    assert_eq!(b.run_last(JobKind::Redact, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.redaction_error.as_deref(),
        Some("gemini: redact service unreachable")
    );

    // JPEG を置けない
    let (id, key) = document(t, Doc::default()).await;
    b.put_pdf(&key);
    b.store.fail_put.store(true, Ordering::SeqCst);
    assert!(requeued(&b.run(JobKind::Redact, t, id).await, 1, 30));
    assert!(b
        .logs()
        .contains(&"notify: redact retry: upload".to_owned()));
    assert_eq!(b.run_last(JobKind::Redact, t, id).await, Outcome::Ack);
    assert_eq!(
        row(id).await.redaction_error.as_deref(),
        Some("r2 upload: put")
    );
    b.store.fail_put.store(false, Ordering::SeqCst);

    // processing を書けない・completed を書けない (DB)
    let (id, key) = document(
        t,
        Doc {
            subject: "__fail:processing",
            extraction_status: "completed",
            ..Doc::default()
        },
    )
    .await;
    b.put_pdf(&key);
    let calls = b.redact.calls().len();
    assert!(requeued(&b.run(JobKind::Redact, t, id).await, 1, 30));
    assert_eq!(b.redact.calls().len(), calls);
    assert!(b
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: redact processing failed: db (")));

    let (id, key) = document(
        t,
        Doc {
            subject: "__fail:completed",
            extraction_status: "failed",
            ..Doc::default()
        },
    )
    .await;
    b.put_pdf(&key);
    assert!(requeued(&b.run(JobKind::Redact, t, id).await, 1, 30));
    assert_eq!(row(id).await.redaction_status, "processing");
    assert!(b
        .logs()
        .iter()
        .any(|l| l.starts_with("notify: redact complete failed: db (")));
}

/// push の失敗はログだけ。文書が無ければ ack
#[tokio::test]
async fn redact_broadcast_failures_are_logged() {
    let t = tenant().await;
    let b = bg().await;
    assert_eq!(
        b.run(JobKind::Redact, t, Uuid::new_v4()).await,
        Outcome::Ack
    );
    assert!(b
        .logs()
        .contains(&"notify: redact: document not found".to_owned()));

    let (id, _) = document(
        t,
        Doc {
            file_name: "a.txt",
            ..Doc::default()
        },
    )
    .await;
    *b.bus.reply.lock().unwrap() = Ok(401);
    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    assert!(b
        .logs()
        .contains(&"notify: redact broadcast http 401".to_owned()));

    let (id, _) = document(
        t,
        Doc {
            file_name: "a.txt",
            ..Doc::default()
        },
    )
    .await;
    *b.bus.reply.lock().unwrap() = Err(());
    assert_eq!(b.run(JobKind::Redact, t, id).await, Outcome::Ack);
    assert!(b
        .logs()
        .contains(&"notify: redact broadcast failed".to_owned()));
    assert_eq!(row(id).await.redaction_status, "skipped");
}
