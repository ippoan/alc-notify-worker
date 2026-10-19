//! 実 DB のテストの準備・接続 (ippoan/alc-lineworks-worker の tests/support の型)。
//!
//! - **`NOTIFY_TEST_ADMIN_DATABASE_URL` が未設定なら失敗する** (skip して緑にしない)。値は準備用の superuser の
//!   接続文字列で、**使い捨ての DB (database 名 `postgres`) に向けること** (ロール・schema・全 migration を作る)。表示はしない
//! - 準備の SQL は ippoan/alc-migrations から版を固定して取る (`scripts/ALC_MIGRATIONS_REV`、
//!   `bash scripts/fetch-migrations.sh` が repo 直下の `.alc-migrations/` に取り出す)。流す順は init_local_db.sql →
//!   空の `_sqlx_migrations` → migrations (1 ファイル 1 transaction) → local_app_grants.sql (alc-vein-worker の container/start.sh と同じ)
//! - 本体のテストは、本番の worker と同じ実行用ロール `alc_api_rt` (migration 158。表の所有者ではない・`NOSUPERUSER`・
//!   `NOBYPASSRLS`) で繋ぐ ([`rt_client`] が毎回確かめる)。パスワードは使い捨ての DB 専用の固定の文字列で、秘密ではない

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};

use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::Request;
use axum::Router;
use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use notify::distribute::Outbound;
use notify::email_routing::{EmailRoutingApi, ErMethod, ErRequest, ErResponse, ErUnreachable};
use notify::http::{Http, HttpError, HttpRequest, HttpResponse};
use notify::line::{LineEndpoints, LineTokenCache};
use notify::lineworks::{LineworksApi, LwRequest, LwResponse, LwUnreachable};
use notify::{router, NotifyState};
use tokio::sync::OnceCell;
use tokio_postgres::{Client, Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

pub const ADMIN_URL_ENV: &str = "NOTIFY_TEST_ADMIN_DATABASE_URL";
pub const RT_ROLE: &str = "alc_api_rt";
/// 使い捨ての DB 専用 (秘密ではない)
const PASSWORD: &str = "notify-test-throwaway";
const SEARCH_PATH: &str = "-c search_path=alc_api,public";
/// 準備を直列にする advisory lock の鍵 (並列に走るテストの binary どうし)
const PREPARE_LOCK: i64 = 1_012_007;

fn migrations_dir() -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../.alc-migrations"
    ));
    assert!(
        dir.join("migrations").is_dir(),
        ".alc-migrations が無い。先に `bash scripts/fetch-migrations.sh` を流す"
    );
    dir
}

pub fn admin_config() -> Config {
    let url = std::env::var(ADMIN_URL_ENV).unwrap_or_else(|_| {
        panic!("{ADMIN_URL_ENV} が未設定。実 DB のテストは接続先が無ければ失敗させる (skip しない)")
    });
    url.parse()
        .unwrap_or_else(|_| panic!("{ADMIN_URL_ENV} が接続文字列として読めない"))
}

async fn raw_connect(config: &Config) -> Client {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .unwrap_or_else(|_| panic!("{ADMIN_URL_ENV} の DB に繋げない"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// superuser の接続 (準備と、テストの行を作る・確かめるのだけに使う)
pub async fn admin() -> Client {
    let mut config = admin_config();
    config.options(SEARCH_PATH);
    raw_connect(&config).await
}

/// 準備をプロセスの中で 1 回だけ流す。DB で既に `alc_api_rt` がログインできれば (別の binary が流した) 何もしない
pub async fn prepare() {
    static PREPARED: OnceCell<()> = OnceCell::const_new();
    PREPARED
        .get_or_init(|| async {
            let su = admin().await;
            su.batch_execute(&format!("SELECT pg_advisory_lock({PREPARE_LOCK})"))
                .await
                .unwrap();
            let done = su
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1 AND rolcanlogin)",
                    &[&RT_ROLE],
                )
                .await
                .unwrap()
                .get::<_, bool>(0);
            if !done {
                migrate(&su).await;
            }
            su.batch_execute(&format!("SELECT pg_advisory_unlock({PREPARE_LOCK})"))
                .await
                .unwrap();
            eprintln!("[notify-db] 実 DB で準備済み (migrations は {})", rev());
        })
        .await;
}

fn rev() -> String {
    std::fs::read_to_string(migrations_dir().join(".rev"))
        .unwrap()
        .trim()
        .to_owned()
}

async fn migrate(su: &Client) {
    let dir = migrations_dir();
    let read = |p: std::path::PathBuf| std::fs::read_to_string(p).unwrap();
    su.batch_execute(&read(dir.join("scripts/init_local_db.sql")))
        .await
        .unwrap();
    su.batch_execute("CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
        .await
        .unwrap();
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("migrations"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    assert!(
        files.iter().any(|f| f
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with("_runtime_role.sql")),
        "取り出した migrations に 158 (alc_api_rt) が無い (scripts/ALC_MIGRATIONS_REV が古い)"
    );
    for file in files {
        let name = file.file_name().unwrap().to_owned();
        if let Err(e) = su
            .batch_execute(&format!("BEGIN;\n{}\n;COMMIT;", read(file)))
            .await
        {
            panic!("migration {name:?}: {:?}", e.code());
        }
    }
    su.batch_execute(&read(dir.join("scripts/local_app_grants.sql")))
        .await
        .unwrap();
    // 実行用ロール (158 が NOLOGIN で作る)
    su.batch_execute(&format!(
        "ALTER ROLE {RT_ROLE} LOGIN PASSWORD '{PASSWORD}';"
    ))
    .await
    .unwrap();
}

/// `alc_api_rt` の接続 (worker と同じく `PgClient` に包む)。**superuser / BYPASSRLS / 触る表の所有者なら panic**
pub async fn rt_client() -> PgClient {
    prepare().await;
    let mut config = admin_config();
    config.user(RT_ROLE).password(PASSWORD);
    let client = raw_connect(&config).await;
    let row = client
        .query_one(
            "SELECT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
             (SELECT bool_or(tableowner = current_user) FROM pg_tables WHERE schemaname = 'alc_api' \
              AND tablename IN ('notify_recipients', 'notify_groups', 'notify_recipient_groups', \
              'notify_line_configs', 'lineworks_channels', 'notify_documents', 'notify_deliveries'))",
            &[],
        )
        .await
        .unwrap();
    assert!(!row.get::<_, bool>(0), "RLS を素通りするロールで繋いでいる");
    assert!(!row.get::<_, bool>(1), "表の所有者で繋いでいる");
    PgClient::new(client)
}

/// 接続の backend の pid
pub async fn backend_pid(pg: &mut PgClient) -> i32 {
    pg.tenant_tx(Uuid::nil(), |tx| {
        Box::pin(async move {
            let row = tx.query_typed_one("SELECT pg_backend_pid()", &[]).await?;
            Ok(row.get::<_, i32>(0))
        })
    })
    .await
    .unwrap()
}

/// backend を落とす (以後その接続の DB の操作は失敗する)
pub async fn terminate(pid: i32) {
    let terminated: bool = admin()
        .await
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap()
        .get(0);
    assert!(terminated);
}

/// 接続が切れた `alc_api_rt` の接続 (backend を落とした接続)
pub async fn broken_client() -> PgClient {
    let mut pg = rt_client().await;
    let pid = backend_pid(&mut pg).await;
    terminate(pid).await;
    pg
}

/// tenant を 1 つ作る (superuser で。名前は乱数)
pub async fn tenant() -> Uuid {
    prepare().await;
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenants (id, name) VALUES ($1, $2)",
            &[&id, &format!("notify-test-{id}")],
        )
        .await
        .unwrap();
    id
}

/// lineworks-worker (Service Binding) のモック。受けたリクエストを残し、決めた応答を返す (既定は 204 本文なし)。
/// [`MockLineworks::then_reply`] で積んだ応答があれば、それを先に 1 つずつ返す
pub struct MockLineworks {
    pub calls: StdMutex<Vec<LwRequest>>,
    pub reply: StdMutex<Result<LwResponse, LwUnreachable>>,
    pub queued: StdMutex<VecDeque<Result<LwResponse, LwUnreachable>>>,
}

impl MockLineworks {
    /// 次の 1 回だけの応答を積む (積んだ順に返す)
    pub fn then_reply(&self, status: u16) {
        self.queued.lock().unwrap().push_back(Ok(LwResponse {
            status,
            body: Vec::new(),
        }));
    }

    /// 次からの応答を `status` と本文にする
    pub fn reply(&self, status: u16, body: &[u8]) {
        *self.reply.lock().unwrap() = Ok(LwResponse {
            status,
            body: body.to_vec(),
        });
    }

    pub fn unreachable(&self, label: &'static str) {
        *self.reply.lock().unwrap() = Err(LwUnreachable(label));
    }

    pub fn calls(&self) -> Vec<LwRequest> {
        self.calls.lock().unwrap().clone()
    }
}

impl LineworksApi for MockLineworks {
    fn call(&self, req: LwRequest) -> BoxFuture<'_, Result<LwResponse, LwUnreachable>> {
        self.calls.lock().unwrap().push(req);
        let queued = self.queued.lock().unwrap().pop_front();
        let reply = queued.unwrap_or_else(|| self.reply.lock().unwrap().clone());
        Box::pin(async move { reply })
    }
}

/// Cloudflare Email Routing のモック。受けたリクエストを残し、POST には決めた応答、DELETE には決めた応答、GET には積んだ応答を順に返す
/// (積んだ分が尽きたら 200 で空の一覧)。POST は [`MockEmailRouting::then_post`] で積んだ応答を先に 1 つずつ返す。`configured` が `false` なら未設定として振る舞う (呼ばれない)
pub struct MockEmailRouting {
    pub configured: bool,
    pub calls: StdMutex<Vec<ErRequest>>,
    pub post_reply: StdMutex<Result<ErResponse, ErUnreachable>>,
    pub post_queue: StdMutex<VecDeque<Result<ErResponse, ErUnreachable>>>,
    pub delete_reply: StdMutex<Result<ErResponse, ErUnreachable>>,
    pub get_queue: StdMutex<VecDeque<Result<ErResponse, ErUnreachable>>>,
}

impl MockEmailRouting {
    fn new(configured: bool) -> Self {
        Self {
            configured,
            calls: StdMutex::new(Vec::new()),
            post_reply: StdMutex::new(Ok(ErResponse {
                status: 200,
                body: b"{}".to_vec(),
            })),
            post_queue: StdMutex::new(VecDeque::new()),
            delete_reply: StdMutex::new(Ok(ErResponse {
                status: 200,
                body: b"{}".to_vec(),
            })),
            get_queue: StdMutex::new(VecDeque::new()),
        }
    }

    /// 次の POST 1 回だけの応答を積む (積んだ順に返す)
    pub fn then_post(&self, status: u16, body: &str) {
        self.post_queue.lock().unwrap().push_back(Ok(ErResponse {
            status,
            body: body.as_bytes().to_vec(),
        }));
    }

    /// 以降の DELETE (送り先を消す) の応答
    pub fn delete_reply(&self, status: u16, body: &str) {
        *self.delete_reply.lock().unwrap() = Ok(ErResponse {
            status,
            body: body.as_bytes().to_vec(),
        });
    }

    pub fn delete_unreachable(&self, label: &'static str) {
        *self.delete_reply.lock().unwrap() = Err(ErUnreachable(label));
    }

    /// 呼ばれた method の列 (呼ばれた順)
    pub fn methods(&self) -> Vec<ErMethod> {
        self.calls().into_iter().map(|c| c.method).collect()
    }

    /// 以降の POST (送り先を作る) の応答
    pub fn post_reply(&self, status: u16, body: &str) {
        *self.post_reply.lock().unwrap() = Ok(ErResponse {
            status,
            body: body.as_bytes().to_vec(),
        });
    }

    pub fn post_unreachable(&self, label: &'static str) {
        *self.post_reply.lock().unwrap() = Err(ErUnreachable(label));
    }

    /// 次の GET (一覧) の応答を 1 つ積む
    pub fn then_list(&self, status: u16, body: &str) {
        self.get_queue.lock().unwrap().push_back(Ok(ErResponse {
            status,
            body: body.as_bytes().to_vec(),
        }));
    }

    pub fn then_list_unreachable(&self, label: &'static str) {
        self.get_queue
            .lock()
            .unwrap()
            .push_back(Err(ErUnreachable(label)));
    }

    pub fn calls(&self) -> Vec<ErRequest> {
        self.calls.lock().unwrap().clone()
    }

    /// 送り先を作った POST の本文の `email`(呼ばれた順)
    pub fn created_emails(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter(|c| c.method == ErMethod::Post)
            .map(|c| {
                let v: serde_json::Value =
                    serde_json::from_str(c.body.as_deref().unwrap()).unwrap();
                v["email"].as_str().unwrap().to_owned()
            })
            .collect()
    }
}

impl EmailRoutingApi for MockEmailRouting {
    fn configured(&self) -> bool {
        self.configured
    }

    fn call(&self, req: ErRequest) -> BoxFuture<'_, Result<ErResponse, ErUnreachable>> {
        let reply = match req.method {
            ErMethod::Post => {
                let queued = self.post_queue.lock().unwrap().pop_front();
                queued.unwrap_or_else(|| self.post_reply.lock().unwrap().clone())
            }
            ErMethod::Delete => self.delete_reply.lock().unwrap().clone(),
            ErMethod::Get => self
                .get_queue
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(ErResponse {
                    status: 200,
                    body: br#"{"result":[]}"#.to_vec(),
                })),
        };
        self.calls.lock().unwrap().push(req);
        Box::pin(async move { reply })
    }
}

/// [`MockHttp`] の規則 (URL の頭, 本文に含む文字列, 応答)
type HttpRule = (String, Option<String>, Result<HttpResponse, HttpError>);

/// 外への POST (LINE・viewer の register-view) のモック。受けたリクエストを残し、URL に合う規則の応答を返す
/// (後から足した規則を先に見る。合わなければ 200 `{}`)
#[derive(Default)]
pub struct MockHttp {
    pub calls: StdMutex<Vec<HttpRequest>>,
    rules: StdMutex<Vec<HttpRule>>,
    /// 次の POST の間に落とす DB の backend (配信の途中で DB が切れたとき)
    kill: StdMutex<Option<i32>>,
}

impl MockHttp {
    /// URL が `url` で始まる POST に `status` と本文を返す
    pub fn on(&self, url: &str, status: u16, body: &str) {
        self.rules.lock().unwrap().push((
            url.to_owned(),
            None,
            Ok(HttpResponse {
                status,
                body: body.as_bytes().to_vec(),
            }),
        ));
    }

    /// URL が `url` で始まり、本文が `needle` を含む POST に `status` を返す
    pub fn on_body(&self, url: &str, needle: &str, status: u16) {
        self.rules.lock().unwrap().push((
            url.to_owned(),
            Some(needle.to_owned()),
            Ok(HttpResponse {
                status,
                body: Vec::new(),
            }),
        ));
    }

    /// URL が `url` で始まる POST を届かなかったことにする
    pub fn fail(&self, url: &str) {
        self.rules
            .lock()
            .unwrap()
            .push((url.to_owned(), None, Err(HttpError)));
    }

    /// 次の POST の間に DB の backend `pid` を落とす
    pub fn kill_db_on_next_post(&self, pid: i32) {
        *self.kill.lock().unwrap() = Some(pid);
    }

    pub fn calls(&self) -> Vec<HttpRequest> {
        self.calls.lock().unwrap().clone()
    }

    /// URL が `url` で始まる呼び出し
    pub fn calls_to(&self, url: &str) -> Vec<HttpRequest> {
        self.calls()
            .into_iter()
            .filter(|c| c.url.starts_with(url))
            .collect()
    }
}

impl Http for MockHttp {
    fn post(&self, req: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>> {
        let reply = self
            .rules
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(url, needle, _)| {
                req.url.starts_with(url.as_str())
                    && needle
                        .as_ref()
                        .is_none_or(|n| String::from_utf8_lossy(&req.body).contains(n.as_str()))
            })
            .map(|(_, _, r)| r.clone())
            .unwrap_or(Ok(HttpResponse {
                status: 200,
                body: b"{}".to_vec(),
            }));
        self.calls.lock().unwrap().push(req);
        let kill = self.kill.lock().unwrap().take();
        Box::pin(async move {
            if let Some(pid) = kill {
                terminate(pid).await;
            }
            reply
        })
    }
}

/// テストの `SSO_ENCRYPTION_KEY` (秘密ではない)
pub const TEST_SSO_KEY: &str = "notify-test-sso-key";
/// テストの LINE の口・viewer・`INTERNAL_SHARED_SECRET`・今の時刻 (どれも外へ出ない名目の値)
pub const LINE_TOKEN_URL: &str = "https://line.test/oauth2/v2.1/token";
pub const LINE_PUSH_URL: &str = "https://line.test/v2/bot/message/push";
pub const FRONTEND_URL: &str = "https://viewer.test";
pub const REGISTER_URL: &str = "https://viewer.test/api/notify/register-view";
pub const VIEWER_SECRET: &str = "viewer-test-secret";
pub const NOW: u64 = 1_700_000_000;

/// [`build_with`] の差し替え
pub struct Options {
    pub sso_key: Option<&'static str>,
    pub frontend_url: Option<&'static str>,
    pub viewer_secret: Option<&'static str>,
    /// Cloudflare Email Routing の token・account ID が在るか
    pub email_routing_configured: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sso_key: Some(TEST_SSO_KEY),
            frontend_url: Some(FRONTEND_URL),
            viewer_secret: Some(VIEWER_SECRET),
            email_routing_configured: true,
        }
    }
}

pub struct App {
    pub router: Router,
    pub logs: Arc<StdMutex<Vec<String>>>,
    pub lineworks: Arc<MockLineworks>,
    pub http: Arc<MockHttp>,
    pub email: Arc<MockEmailRouting>,
    pub line_tokens: Arc<LineTokenCache>,
}

impl App {
    pub fn logs(&self) -> Vec<String> {
        self.logs.lock().unwrap().clone()
    }
}

/// worker の src/lib.rs と同じ組み立て (鍵は [`TEST_SSO_KEY`])
pub fn build(pg: PgClient) -> App {
    build_with_key(pg, Some(TEST_SSO_KEY))
}

pub fn build_with_key(pg: PgClient, sso_key: Option<&'static str>) -> App {
    build_with(
        pg,
        Options {
            sso_key,
            ..Options::default()
        },
    )
}

pub fn build_with(pg: PgClient, opts: Options) -> App {
    let logs = Arc::new(StdMutex::new(Vec::new()));
    let sink = logs.clone();
    let lineworks = Arc::new(MockLineworks {
        calls: StdMutex::new(Vec::new()),
        reply: StdMutex::new(Ok(LwResponse {
            status: 204,
            body: Vec::new(),
        })),
        queued: StdMutex::new(VecDeque::new()),
    });
    let http = Arc::new(MockHttp::default());
    let email = Arc::new(MockEmailRouting::new(opts.email_routing_configured));
    let line_tokens = Arc::new(LineTokenCache::default());
    let state = NotifyState {
        pg: Arc::new(Mutex::new(pg)),
        lineworks: lineworks.clone(),
        email_routing: email.clone(),
        sso_key: opts.sso_key.map(Arc::from),
        outbound: Outbound {
            http: http.clone(),
            line_endpoints: LineEndpoints {
                token: LINE_TOKEN_URL.to_owned(),
                push: LINE_PUSH_URL.to_owned(),
            },
            line_tokens: line_tokens.clone(),
            frontend_url: opts.frontend_url.map(Arc::from),
            viewer_secret: opts.viewer_secret.map(Arc::from),
            now: Arc::new(|| NOW),
        },
        log: Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
    };
    App {
        router: router().with_state(state),
        logs,
        lineworks,
        http,
        email,
        line_tokens,
    }
}

pub async fn app() -> App {
    build(rt_client().await)
}

/// 接続が切れた DB の app (どの口も DB の失敗の経路を通る)
pub async fn broken_app() -> App {
    build(broken_client().await)
}

pub struct Got {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Got {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

/// 1 リクエストを打つ。`tenant` が在れば `X-Tenant-ID` を付ける。`body` が在れば JSON で送る
pub async fn call(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    body: Option<serde_json::Value>,
) -> Got {
    let headers: Vec<(&str, String)> = tenant
        .map(|t| ("X-Tenant-ID", t.to_string()))
        .into_iter()
        .collect();
    call_with(app, method, uri, &headers, body).await
}

/// auth-worker が付ける identity のヘッダー (`X-Tenant-ID`・`X-User-ID`・`X-User-Email`・`X-User-Role`)
pub fn identity(tenant: Uuid, user: Uuid) -> Vec<(&'static str, String)> {
    vec![
        ("X-Tenant-ID", tenant.to_string()),
        ("X-User-ID", user.to_string()),
        ("X-User-Email", "user@example.com".to_owned()),
        ("X-User-Role", "admin".to_owned()),
    ]
}

/// ヘッダーを渡して 1 リクエストを打つ
pub async fn call_with(
    app: &App,
    method: &str,
    uri: &str,
    headers: &[(&str, String)],
    body: Option<serde_json::Value>,
) -> Got {
    let mut b = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        b = b.header(*k, v);
    }
    let body = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&v).unwrap())
        }
        None => Body::empty(),
    };
    let resp = app
        .router
        .clone()
        .oneshot(b.body(body).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    Got { status, body }
}
