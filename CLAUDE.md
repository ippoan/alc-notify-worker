# alc-notify-worker

notify の配信・宛先の Worker `alc-notify` (Refs ippoan/rust-alc-api#747)。rust-alc-api の `crates/alc-notify` のうち、
配信・宛先・LINE WORKS の口だけを移す。**文書の取り込み・PDF 加工 (pdfium)・viewer・ingest・LINE webhook・read_tracker は
Cloud Run (rust-alc-api) に残っている。** 型は ippoan/alc-lineworks-worker と同じ。
**呼び手は auth-worker の Service Binding だけ** (呼び手を認証しない。auth-worker が付けた `X-Tenant-ID` を信頼する)。

いまの口 (path・method・JSON・status は rust と同じ。nuxt-notify と auth-worker がそのまま使う):
- `/api/notify/recipients` (GET / POST)・`/bulk` (POST)・`/{id}` (GET / PUT / DELETE)・`/email-status` (GET。rust に無い口。
  宛先のメールアドレスの Cloudflare Email Routing の送り先としての状態 `verified` / `pending` / `not_registered`。未設定なら `{configured:false}`)
- `/api/notify/groups` (GET / POST)・`/{id}` (GET / PUT / DELETE)・`/{id}/members` (GET / POST)・`/{id}/members/{recipient_id}` (DELETE)
- `/api/notify/line-config` (GET / POST / DELETE。POST は秘密値を `SSO_ENCRYPTION_KEY` で暗号化。GET は暗号文を返さない)
- `/api/notify/lineworks/channels` (GET)・`/{id}` (DELETE)・`/{id}/test-send` (POST)
- `/api/notify/lineworks/users` (GET)・`/api/notify/lineworks/login-activity?days=` (GET)
- `/api/notify/documents/{id}/distribute` (POST)・`/api/notify/test-distribute` (POST) (配信。viewer の Worker
  (ippoan/nuxt-notify) の `/api/notify/register-view` へ view を登録し、`NOTIFY_FRONTEND_URL` の URL を載せて送る)

宛先の保存 (POST / PUT / bulk) は、メールアドレスが新しく入った・変わったとき、Cloudflare Email Routing の送り先 (destination address) を
作る (`crates/notify/src/email_routing.rs` の `EmailRoutingApi`、worker の実装は `src/email_routing.rs`)。送れるのは確認済みの送り先だけ
(後続の alc-measurements の `send_email`)。DB の lock の外で呼ぶ best-effort。**宛先を消しても・変えても送り先は消さない**
(アカウント全体で共有で、他の tenant も使うため)。token は Secrets Store の `CF_EMAIL_ROUTING_TOKEN`、account ID は `[vars]` の `CF_ACCOUNT_ID` (他の Worker と同じく平文)。

LINE WORKS の API はこの worker では叩かず、ippoan/alc-lineworks-worker を Service Binding `ALC_LINEWORKS` で呼ぶ
(`X-Tenant-ID` 付き。`crates/notify/src/lineworks.rs` の `LineworksApi`、worker の実装は `src/lineworks.rs`)。
**bot の選び方の契約:** notify の個人宛は tenant の既定 bot (lineworks-worker の `/messages` に `bot_config_id` を渡さない)、
channel 宛は行の `bot_config_id`。宛先を Bot ごとに分けたくなったら notify_recipients に bot_config_id を足す (ここで別ルールを
足さない。rust の `lineworks_channels.rs` の `pick_lineworks_bot_config_id` の doc と同じ)。lineworks-worker の `/send` の
呼び出し側指定 `bot_config_id` は勤怠 (rust-leave-worker) 用の別経路。

直下 = Worker (wasm 専用。DB への経路 `src/db.rs`・lineworks-worker の binding `src/lineworks.rs`・workers-rs への載せ方 `src/lib.rs`)、`crates/notify/` = 口・SQL。

## コマンド
```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
bash scripts/fetch-migrations.sh   # 版は scripts/ALC_MIGRATIONS_REV
NOTIFY_TEST_ADMIN_DATABASE_URL=postgresql://postgres:<pw>@127.0.0.1:<port>/postgres cargo test --workspace
worker-build --release && npx wrangler@4.144.0 deploy --dry-run
```
DB テストは使い捨ての postgres:17 へ。URL が無ければ失敗 (skip しない)。接続ロールは `alc_api_rt`。

## 規範
- 公開範囲を弱めない: 本番・staging とも Service Binding だけ (どの env も `workers_dev`/`preview_urls` = false、route 無し)。
  Hyperdrive (`NOTIFY_HYPERDRIVE`) は本番だけ、staging は VPC (`NOTIFY_DB_VPC`)。`SSO_ENCRYPTION_KEY` (Secrets Store) と
  `ALC_LINEWORKS` (Service Binding)・`INTERNAL_SHARED_SECRET` (Secrets Store。viewer 登録のヘッダ)・`CF_EMAIL_ROUTING_TOKEN` (Secrets Store。Email Routing の送り先)・`[vars]`
  `NOTIFY_FRONTEND_URL`・`CF_ACCOUNT_ID` は両方の env に書く (binding も vars も env に継承されない)。`wrangler.toml` の表の順を変えない
  (新しい表は placement より後。`[env.staging]` と `[env.staging.observability]` の間に挟まない)。
- **ログ・エラー応答は固定の語だけ。** `{e}`・DB の message・識別子 (tenant_id・行 id)・入力の値を出さない。
  DB の失敗は `notify: <段> failed: db (<kind>)` (kind は SQLSTATE か tokio-postgres の種類の label)。
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。SQL は `crates/notify/src/pg.rs` の 1 か所。
  名前付き prepared statement 禁止。Row を transaction の外へ持ち出さない (owned な型に `TxOutput`)。
  tenant_id の列が在る表は WHERE にも tenant_id を書く。
- rust に無い検査 (role など) を足さない。rust と違う振る舞いにするときは理由を書く (いまは 3 つ: (1) members の POST と bulk のグループ追加は、宛先が同じ tenant に無ければ何も足さない。POST は 404。RLS の WITH CHECK が group の tenant しか見ないため。(2) LINE WORKS の口の失敗の本文は lineworks-worker の status と語 (`error`・`missing_scope` の `scope`・語の形の `message`) だけを写し、自由文 (rust の 502 の `message`) は載せない。tenant に bot が無いときの語は lineworks-worker の `bot_config_not_found` (rust は `no_lineworks_config`。画面は status しか見ない)。ログに上流の文を出さないため。(3) 宛先の保存で Cloudflare の送り先を作るのは `X-User-Role` が `admin` のときだけ (それ以外は宛先は保存するが送り先は作らない)。任意のアドレスに Cloudflare の確認メールが飛ぶため。`email-status` は読むだけなので role を見ない)。
- `coverage_100.toml` の登録を外さない。CI の本数 (recipients_db 16・lineworks_db 7・distribute_db 9。
  `.github/workflows/ci.yml` の gate) はテストを足したら上げる。
- public repo: ホスト名・account ID・tenant UUID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
  例外: 利用者に配る公開の viewer URL `NOTIFY_FRONTEND_URL` は wrangler.toml の `[vars]` に平文で置く
  (rust-alc-api の Cloud Run・ippoan/nuxt-notify と同じ値)。Cloudflare の account ID `CF_ACCOUNT_ID` も `[vars]` に平文で置く
  (auth-worker・nuxt-notify などの Worker と同じ扱い。ユーザー決定 2026-10-19、Refs ippoan/alc-app#436)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-core-wasm・alc-worker-db・alc-worker-crypto を同じ rev で)。
