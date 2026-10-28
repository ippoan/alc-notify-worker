//! 抽出と黒塗りの背景処理 (Queues の consumer が 1 ジョブずつ呼ぶ。W8c-2、Refs ippoan/rust-alc-api#680)。
//! rust-alc-api の `crates/alc-notify/src/background_extract.rs`・`background_redaction.rs` の写しで、Gemini と pdfium は
//! alc-redact (GCP Cloud Run) の `POST /extract`・`POST /redact` を auth-worker の RPC `AlcRedactEntrypoint` 越しに呼ぶ ([`RedactApi`])。
//!
//! 状態の列と `*_error` の接頭辞は rust と同じ (`r2 download: ` / `gemini: ` / `parse: ` / `apply: ` / `r2 upload: `)。
//! 黒塗りが終わったら (completed・skipped・failed) nuxt-notify の `notify-realtime-bus` に WS の push を送る ([`Broadcaster`]。
//! 失敗はログだけ)。抽出は rust と同じく push しない。
//!
//! **ack と送り直しの決め方:**
//! - 失敗を DB に書いたら ack する (Gemini を呼び直さない。9/14 以降の 402 をやり直さない決定とも合う)
//! - 送り直す ([`Outcome::Requeue`]) のは一時的な失敗だけ: R2 の読み書き (無いキーは除く)・RPC の失敗・auth-worker の
//!   口の失敗 (`redact_unconfigured`・`upstream_auth`・`upstream_unreachable`)・alc-redact 自体の 5xx (固定のコードを持たない
//!   応答。Cloud Run の 503 など)・DB の失敗。[`MAX_ATTEMPTS`] 回目でも一時的なら、失敗を DB に書いて ack する
//!   (状態が `processing`・`pending` のまま残らないように)
//! - alc-redact の固定のコード (`gemini` は 502 でも) は一時的として扱わない
//!
//! **二重に配られても壊れない:** 黒塗りは状態が `pending`・`processing` のときだけ、抽出は `pending` のときだけ進める
//! (recompute が `pending` に戻す)。JPEG の PUT は同じキーへの上書き。
//!
//! rust との違い: 抽出の `not_pdf` (拡張子は .pdf で中身が PDF でない) は「logistics 無しで完了」にする (rust は Gemini の
//! 失敗になっていた。抽出するものが無いのは .pdf でない文書と同じ)。応答が固定のコードを持たない 4xx は
//! `gemini: upstream status <n>` の失敗 (rust には無い経路)。
//! ログは固定の語・段・kind だけ (識別子・Gemini の応答・detail を出さない)。detail は rust と同じく `*_error` の列にだけ入る。

use std::sync::Arc;

use alc_worker_db::{kind, PgClient};
use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use serde::Serialize;
use uuid::Uuid;

use crate::jobs::{Job, JobKind};
use crate::pg::{self, NotifyDocument, RedactTiming};
use crate::store::{SharedStore, StoreError};
use crate::LogSink;

/// 一時的な失敗を何回まで試すか (初回を含む)。これを超えたら失敗を DB に書いて ack
pub const MAX_ATTEMPTS: u32 = 3;
/// 送り直しの遅れ (秒)。2 回目は 30 秒後、3 回目は 60 秒後
pub const RETRY_DELAY_SECS: u32 = 30;

/// alc-redact (auth-worker の RPC 越し) の応答。`headers` の名前は小文字
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedactReply {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

/// RPC が throw した・binding が無い (一時的として送り直す)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedactUnreachable;

/// alc-redact の口 (Worker では auth-worker の RPC `AlcRedactEntrypoint`、テストではモック)
pub trait RedactApi: Send + Sync {
    /// `POST /redact` (200 は JPEG)
    fn redact(&self, pdf: Vec<u8>) -> BoxFuture<'_, Result<RedactReply, RedactUnreachable>>;
    /// `POST /extract` (200 は `{"logistics": {...}}`)。`self_company` が空なら自社名を渡さない
    fn extract(
        &self,
        pdf: Vec<u8>,
        self_company: String,
    ) -> BoxFuture<'_, Result<RedactReply, RedactUnreachable>>;
}

/// notify-realtime-bus の `POST /broadcast` (Worker では Service Binding、テストではモック)。返すのは status
pub trait Broadcaster: Send + Sync {
    fn broadcast(&self, body: Vec<u8>) -> BoxFuture<'_, Result<u16, ()>>;
}

/// WS で送る黒塗りの終わり (rust の `RedactEvent` と同じ JSON)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedactEvent {
    pub tenant_id: Uuid,
    pub document_id: Uuid,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redactions_applied: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redaction_error: Option<String>,
}

/// 背景処理に要るもの (consumer が 1 バッチごとに組み立てる)
#[derive(Clone)]
pub struct Background {
    pub pg: Arc<Mutex<PgClient>>,
    /// 文書のファイル (R2 の binding `NOTIFY_R2`)。無ければ rust の `notify_storage not configured` の失敗
    pub files: Option<SharedStore>,
    pub redact: Arc<dyn RedactApi>,
    /// 無ければ push しない (rust の broadcaster 無しと同じ)
    pub bus: Option<Arc<dyn Broadcaster>>,
    pub log: LogSink,
    /// 今の時刻 (ミリ秒)。段の所要時間を測る
    pub now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

/// consumer への答え
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ack,
    /// `job` (attempt を 1 つ進めたもの) を `delay_secs` 秒後に送り直して ack する
    Requeue {
        job: Job,
        delay_secs: u32,
    },
}

/// 一時的な失敗 (段の名前は ログ用の固定の語。`error` は最後の回に DB に書く文)
struct Transient {
    stage: &'static str,
    error: String,
}

/// alc-redact の応答の読み方
enum Upstream {
    Ok(RedactReply),
    /// alc-redact の固定のコードと detail
    Code(String, String),
    Transient(String),
    /// 固定のコードを持たない 4xx
    Status(u16),
}

/// 一時的として送り直す auth-worker の口のコード
const TRANSIENT_CODES: [&str; 3] = [
    "redact_unconfigured",
    "upstream_auth",
    "upstream_unreachable",
];

fn classify(reply: RedactReply) -> Upstream {
    if reply.status == 200 {
        return Upstream::Ok(reply);
    }
    let parsed: Option<serde_json::Value> = serde_json::from_slice(&reply.body).ok();
    let code = parsed
        .as_ref()
        .and_then(|v| v["error"].as_str())
        .map(str::to_owned);
    match code {
        Some(c) if TRANSIENT_CODES.contains(&c.as_str()) => Upstream::Transient(c),
        Some(c) if reply.status < 500 || is_redact_code(&c) => {
            let detail = parsed
                .as_ref()
                .and_then(|v| v["detail"].as_str())
                .unwrap_or_default()
                .to_owned();
            Upstream::Code(c, detail)
        }
        _ if reply.status >= 500 => Upstream::Transient(format!("status {}", reply.status)),
        _ => Upstream::Status(reply.status),
    }
}

/// alc-redact が返す固定のコード (5xx でもこれなら一時的としない)
fn is_redact_code(code: &str) -> bool {
    matches!(
        code,
        "bad_request" | "not_pdf" | "gemini_key_missing" | "gemini" | "apply" | "parse"
    )
}

fn header_i32(reply: &RedactReply, name: &str) -> i32 {
    reply
        .headers
        .iter()
        .find(|(k, _)| k == name)
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0)
}

fn is_pdf(doc: &NotifyDocument) -> bool {
    doc.file_name
        .as_deref()
        .unwrap_or_default()
        .to_lowercase()
        .ends_with(".pdf")
}

/// `extracted_data` の `logistics` を差し替える (rust の `merge_logistics_into_data`)。他のキーは残す。
/// `logistics` が空の object なら (rust の `has_any() == false`) キーを消す
pub fn merge_logistics(
    existing: Option<serde_json::Value>,
    logistics: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut data = match existing {
        Some(v) if v.is_object() => v,
        _ => serde_json::json!({}),
    };
    let obj = data.as_object_mut().expect("object");
    match logistics.filter(has_any) {
        Some(l) => {
            obj.insert("logistics".into(), l);
        }
        None => {
            obj.remove("logistics");
        }
    }
    data
}

/// 空でない文字列の値が 1 つでもある (rust の `LogisticsFields::has_any`)
fn has_any(v: &serde_json::Value) -> bool {
    v.as_object().is_some_and(|o| {
        o.values()
            .any(|x| x.as_str().is_some_and(|s| !s.trim().is_empty()))
    })
}

impl Background {
    fn log(&self, message: &str) {
        (self.log)(message);
    }

    fn elapsed(&self, since: u64) -> i32 {
        ((self.now_ms)().saturating_sub(since)).min(i32::MAX as u64) as i32
    }

    /// 1 ジョブを処理する
    pub async fn run(&self, job: Job) -> Outcome {
        let last = job.attempt + 1 >= MAX_ATTEMPTS;
        let result = match job.kind {
            JobKind::Extract => self.extract(&job, last).await,
            JobKind::Redact => self.redact(&job, last).await,
        };
        match result {
            Ok(()) => Outcome::Ack,
            // 最後の回に残るのは DB の失敗だけ (失敗も書けない)。ログだけ残して止める
            Err(t) if last => {
                self.log(&format!(
                    "notify: {} gave up: {}",
                    kind_name(job.kind),
                    t.stage
                ));
                Outcome::Ack
            }
            Err(t) => {
                self.log(&format!(
                    "notify: {} retry: {}",
                    kind_name(job.kind),
                    t.stage
                ));
                Outcome::Requeue {
                    delay_secs: RETRY_DELAY_SECS * (job.attempt + 1),
                    job: Job {
                        attempt: job.attempt + 1,
                        ..job
                    },
                }
            }
        }
    }

    /// 文書を引く。無ければ `None` (ack)、DB の失敗は一時的
    async fn document(
        &self,
        job: &Job,
        what: &'static str,
    ) -> Result<Option<NotifyDocument>, Transient> {
        let got = {
            let mut pg = self.pg.lock().await;
            pg::get_document(&mut pg, job.tenant_id, job.doc_id).await
        };
        match got {
            Ok(Some(d)) => Ok(Some(d)),
            Ok(None) => {
                self.log(&format!("notify: {what}: document not found"));
                Ok(None)
            }
            Err(e) => Err(self.db_transient(what, "get", &e)),
        }
    }

    fn db_transient(
        &self,
        what: &str,
        stage: &'static str,
        e: &tokio_postgres::Error,
    ) -> Transient {
        self.log(&format!("notify: {what} {stage} failed: db ({})", kind(e)));
        Transient {
            stage,
            error: String::new(),
        }
    }

    /// R2 から原本を読む。無いキーは失敗 (`Err(Ok(error))`)、ほかは一時的 (`Err(Err(_))`)
    async fn download(
        &self,
        files: &SharedStore,
        key: &str,
    ) -> Result<Vec<u8>, Result<String, Transient>> {
        match files.get(key).await {
            Ok(b) => Ok(b),
            Err(StoreError(label)) if label == "missing" => {
                Err(Ok(format!("r2 download: {label}")))
            }
            Err(StoreError(label)) => Err(Err(Transient {
                stage: "download",
                error: format!("r2 download: {label}"),
            })),
        }
    }

    // ---- 抽出 ----

    async fn extract(&self, job: &Job, last: bool) -> Result<(), Transient> {
        let Some(doc) = self.document(job, "extract").await? else {
            return Ok(());
        };
        if doc.extraction_status != "pending" {
            return Ok(());
        }
        // PDF 以外は抽出するものが無い (logistics 無しで完了)
        if !is_pdf(&doc) {
            return self.extract_done(job, &doc, None).await;
        }
        let Some(files) = self.files.clone() else {
            self.log("notify: extract failed: storage");
            return self
                .extract_failed(job, "notify_storage not configured".into())
                .await;
        };
        let step = self.extract_steps(job, &doc, &files).await;
        match step {
            Ok(Some(logistics)) => self.extract_done(job, &doc, Some(logistics)).await,
            Ok(None) => self.extract_done(job, &doc, None).await,
            Err(Ok(error)) => self.extract_failed(job, error).await,
            Err(Err(t)) if last => self.extract_failed(job, t.error).await,
            Err(Err(t)) => Err(t),
        }
    }

    /// 読む → 自社名 → alc-redact。`Ok(None)` = 抽出するものが無い (`not_pdf`・`gemini_key_missing`)
    async fn extract_steps(
        &self,
        job: &Job,
        doc: &NotifyDocument,
        files: &SharedStore,
    ) -> Result<Option<serde_json::Value>, Result<String, Transient>> {
        let pdf = self.download(files, &doc.r2_key).await?;
        // 自社名が取れなくてもラベル推論で続ける (rust と同じ)
        let name = {
            let mut pg = self.pg.lock().await;
            pg::get_tenant_name(&mut pg, job.tenant_id).await
        };
        let name = match name {
            Ok(n) => n.unwrap_or_default(),
            Err(e) => {
                self.log(&format!(
                    "notify: extract tenant name failed: db ({})",
                    kind(&e)
                ));
                String::new()
            }
        };
        let reply = self.redact.extract(pdf, name).await.map_err(|_| {
            Err(Transient {
                stage: "rpc",
                error: "gemini: redact service unreachable".into(),
            })
        })?;
        match classify(reply) {
            Upstream::Ok(r) => {
                let body: Option<serde_json::Value> = serde_json::from_slice(&r.body).ok();
                match body.as_ref().map(|b| &b["logistics"]) {
                    Some(l) if l.is_object() => Ok(Some(l.clone())),
                    _ => Err(Ok("parse: invalid redact service response".into())),
                }
            }
            Upstream::Code(c, _) if c == "not_pdf" || c == "gemini_key_missing" => Ok(None),
            Upstream::Code(c, d) if c == "parse" => Err(Ok(format!("parse: {d}"))),
            Upstream::Code(_, d) => Err(Ok(format!("gemini: {d}"))),
            Upstream::Transient(what) => Err(Err(Transient {
                stage: "upstream",
                error: format!("gemini: redact service unavailable ({what})"),
            })),
            Upstream::Status(s) => Err(Ok(format!("gemini: upstream status {s}"))),
        }
    }

    /// logistics を差し替えて completed (無ければ logistics を消す)
    async fn extract_done(
        &self,
        job: &Job,
        doc: &NotifyDocument,
        logistics: Option<serde_json::Value>,
    ) -> Result<(), Transient> {
        let data = merge_logistics(doc.extracted_data.clone(), logistics);
        let r = {
            let mut pg = self.pg.lock().await;
            pg::complete_extraction(&mut pg, job.tenant_id, job.doc_id, data).await
        };
        r.map_err(|e| self.db_transient("extract", "update", &e))
    }

    /// 失敗を書いて ack (書けなくてもログだけ。Gemini を呼び直さない)
    async fn extract_failed(&self, job: &Job, error: String) -> Result<(), Transient> {
        self.log(&format!("notify: extract failed: {}", stage_of(&error)));
        let r = {
            let mut pg = self.pg.lock().await;
            pg::fail_extraction(&mut pg, job.tenant_id, job.doc_id, error).await
        };
        if let Err(e) = r {
            self.log(&format!(
                "notify: extract fail update failed: db ({})",
                kind(&e)
            ));
        }
        Ok(())
    }

    // ---- 黒塗り ----

    async fn redact(&self, job: &Job, last: bool) -> Result<(), Transient> {
        let Some(doc) = self.document(job, "redact").await? else {
            return Ok(());
        };
        if doc.redaction_status != "pending" && doc.redaction_status != "processing" {
            return Ok(());
        }
        if !is_pdf(&doc) {
            return self.redact_end(job, "skipped", None).await;
        }
        let Some(files) = self.files.clone() else {
            self.log("notify: redact failed: storage");
            return self
                .redact_end(job, "failed", Some("notify_storage not configured".into()))
                .await;
        };
        let r = {
            let mut pg = self.pg.lock().await;
            pg::update_redaction_status(&mut pg, job.tenant_id, job.doc_id, "processing", None)
                .await
        };
        r.map_err(|e| self.db_transient("redact", "processing", &e))?;

        match self.redact_steps(job, &doc, &files).await {
            Ok(None) => Ok(()),
            Ok(Some((status, error))) => self.redact_end(job, status, error).await,
            Err(t) if last => self.redact_end(job, "failed", Some(t.error)).await,
            Err(t) => Err(t),
        }
    }

    /// 読む → alc-redact → JPEG を置く → completed。`Ok(Some)` は終わりの状態 (skipped・failed) を書く
    #[allow(clippy::type_complexity)]
    async fn redact_steps(
        &self,
        job: &Job,
        doc: &NotifyDocument,
        files: &SharedStore,
    ) -> Result<Option<(&'static str, Option<String>)>, Transient> {
        let t_total = (self.now_ms)();
        let pdf = match self.download(files, &doc.r2_key).await {
            Ok(b) => b,
            Err(Ok(error)) => return Ok(Some(("failed", Some(error)))),
            Err(Err(t)) => return Err(t),
        };
        let dl_ms = self.elapsed(t_total);

        let reply = self.redact.redact(pdf).await.map_err(|_| Transient {
            stage: "rpc",
            error: "gemini: redact service unreachable".into(),
        })?;
        let reply = match classify(reply) {
            Upstream::Ok(r) => r,
            Upstream::Code(c, _) if c == "not_pdf" || c == "gemini_key_missing" => {
                return Ok(Some(("skipped", None)));
            }
            Upstream::Code(c, d) if c == "gemini" => {
                return Ok(Some(("failed", Some(format!("gemini: {d}")))))
            }
            Upstream::Code(_, d) => return Ok(Some(("failed", Some(format!("apply: {d}"))))),
            Upstream::Transient(what) => {
                return Err(Transient {
                    stage: "upstream",
                    error: format!("gemini: redact service unavailable ({what})"),
                })
            }
            Upstream::Status(s) => {
                return Ok(Some((
                    "failed",
                    Some(format!("gemini: upstream status {s}")),
                )))
            }
        };
        let applied = header_i32(&reply, "x-redactions-applied");
        let llm_ms = header_i32(&reply, "x-redact-llm-ms");
        let render_ms = header_i32(&reply, "x-redact-render-ms");

        let t_up = (self.now_ms)();
        let key = format!("{}/redacted/{}.jpg", job.tenant_id, job.doc_id);
        files
            .put(&key, reply.body, "image/jpeg")
            .await
            .map_err(|StoreError(label)| Transient {
                stage: "upload",
                error: format!("r2 upload: {label}"),
            })?;
        let timing = RedactTiming {
            dl_ms,
            llm_ms,
            render_ms,
            upload_ms: self.elapsed(t_up),
            total_ms: self.elapsed(t_total),
        };
        let r = {
            let mut pg = self.pg.lock().await;
            pg::complete_redaction(&mut pg, job.tenant_id, job.doc_id, key, applied, timing).await
        };
        r.map_err(|e| self.db_transient("redact", "complete", &e))?;
        self.broadcast(job, "completed", Some(applied), None).await;
        Ok(None)
    }

    /// 終わりの状態 (skipped・failed) を書いて push する。書けなくてもログだけで ack (Gemini を呼び直さない)
    async fn redact_end(
        &self,
        job: &Job,
        status: &'static str,
        error: Option<String>,
    ) -> Result<(), Transient> {
        if let Some(e) = &error {
            self.log(&format!("notify: redact failed: {}", stage_of(e)));
        }
        let r = {
            let mut pg = self.pg.lock().await;
            pg::update_redaction_status(&mut pg, job.tenant_id, job.doc_id, status, error.clone())
                .await
        };
        if let Err(e) = r {
            self.log(&format!(
                "notify: redact status update failed: db ({})",
                kind(&e)
            ));
        }
        self.broadcast(job, status, None, error).await;
        Ok(())
    }

    async fn broadcast(
        &self,
        job: &Job,
        status: &'static str,
        redactions_applied: Option<i32>,
        redaction_error: Option<String>,
    ) {
        let Some(bus) = &self.bus else {
            return;
        };
        let ev = RedactEvent {
            tenant_id: job.tenant_id,
            document_id: job.doc_id,
            status,
            redactions_applied,
            redaction_error,
        };
        let body = serde_json::to_vec(&ev).expect("serialize");
        match bus.broadcast(body).await {
            Ok(s) if (200..300).contains(&s) => {}
            Ok(s) => self.log(&format!("notify: redact broadcast http {s}")),
            Err(()) => self.log("notify: redact broadcast failed"),
        }
    }
}

fn kind_name(k: JobKind) -> &'static str {
    match k {
        JobKind::Extract => "extract",
        JobKind::Redact => "redact",
    }
}

/// `*_error` の接頭辞 (ログには detail を出さず、段だけ出す)
fn stage_of(error: &str) -> &str {
    error.split(':').next().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reply(status: u16, body: &str) -> RedactReply {
        RedactReply {
            status,
            body: body.as_bytes().to_vec(),
            headers: Vec::new(),
        }
    }

    #[test]
    fn classify_maps_codes_and_statuses() {
        assert!(matches!(classify(reply(200, "x")), Upstream::Ok(_)));
        for c in TRANSIENT_CODES {
            let body = format!(r#"{{"error":"{c}"}}"#);
            assert!(matches!(classify(reply(503, &body)), Upstream::Transient(w) if w == c));
        }
        // alc-redact の固定のコードは 5xx でも一時的としない
        assert!(matches!(
            classify(reply(502, r#"{"error":"gemini","detail":"gemini status 402: pay"}"#)),
            Upstream::Code(c, d) if c == "gemini" && d == "gemini status 402: pay"
        ));
        assert!(
            matches!(classify(reply(500, r#"{"error":"apply"}"#)), Upstream::Code(c, d) if c == "apply" && d.is_empty())
        );
        // 知らないコードでも 4xx はコードとして
        assert!(
            matches!(classify(reply(409, r#"{"error":"other"}"#)), Upstream::Code(c, _) if c == "other")
        );
        // 知らないコードの 5xx・JSON でない 5xx は一時的
        assert!(
            matches!(classify(reply(500, r#"{"error":"other"}"#)), Upstream::Transient(w) if w == "status 500")
        );
        assert!(
            matches!(classify(reply(503, "Service Unavailable")), Upstream::Transient(w) if w == "status 503")
        );
        // コードを持たない 4xx
        assert!(matches!(
            classify(reply(403, "forbidden")),
            Upstream::Status(403)
        ));
        assert!(matches!(
            classify(reply(404, r#"{"x":1}"#)),
            Upstream::Status(404)
        ));
    }

    #[test]
    fn merge_logistics_keeps_other_keys() {
        let existing = json!({"logistics": {"notes": "old"}, "other": 1});
        assert_eq!(
            merge_logistics(Some(existing.clone()), Some(json!({"loading_place": "A"}))),
            json!({"logistics": {"loading_place": "A"}, "other": 1})
        );
        // 空 (空白だけ・null だけを含む) なら消す
        assert_eq!(
            merge_logistics(
                Some(existing.clone()),
                Some(json!({"notes": "  ", "x": null}))
            ),
            json!({"other": 1})
        );
        assert_eq!(merge_logistics(Some(existing), None), json!({"other": 1}));
        // object でなければ作り直す
        assert_eq!(
            merge_logistics(Some(json!([1])), Some(json!({"notes": "n"}))),
            json!({"logistics": {"notes": "n"}})
        );
        assert_eq!(merge_logistics(None, None), json!({}));
        assert!(!has_any(&json!("str")));
    }

    #[test]
    fn header_and_stage_helpers() {
        let r = RedactReply {
            status: 200,
            body: Vec::new(),
            headers: vec![
                ("x-redactions-applied".into(), "3".into()),
                ("x-redact-llm-ms".into(), "bad".into()),
            ],
        };
        assert_eq!(header_i32(&r, "x-redactions-applied"), 3);
        assert_eq!(header_i32(&r, "x-redact-llm-ms"), 0);
        assert_eq!(header_i32(&r, "x-redact-render-ms"), 0);
        assert_eq!(stage_of("r2 download: missing"), "r2 download");
        assert_eq!(kind_name(JobKind::Extract), "extract");
    }

    #[test]
    fn event_json_matches_rust() {
        let t = Uuid::nil();
        let ev = RedactEvent {
            tenant_id: t,
            document_id: t,
            status: "completed",
            redactions_applied: Some(2),
            redaction_error: None,
        };
        assert_eq!(
            serde_json::to_value(&ev).unwrap(),
            json!({"tenant_id": t, "document_id": t, "status": "completed", "redactions_applied": 2})
        );
    }
}
