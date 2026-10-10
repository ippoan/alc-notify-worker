# alc-notify-worker

notify の配信・宛先の Worker `alc-notify` (Refs ippoan/rust-alc-api#747)。rust-alc-api の `crates/alc-notify` のうち、
配信・宛先・LINE WORKS の口だけを移す。**文書の取り込み・PDF 加工 (pdfium)・viewer・ingest・LINE webhook・read_tracker は
Cloud Run (rust-alc-api) に残っている。** 型は ippoan/alc-lineworks-worker と同じ。
**呼び手は auth-worker の Service Binding だけ** (呼び手を認証しない。auth-worker が付けた `X-Tenant-ID` を信頼する)。

いまの口 (path・method・JSON・status は rust と同じ。nuxt-notify と auth-worker がそのまま使う):
- `/api/notify/recipients` (GET / POST)・`/bulk` (POST)・`/{id}` (GET / PUT / DELETE)
- `/api/notify/groups` (GET / POST)・`/{id}` (GET / PUT / DELETE)・`/{id}/members` (GET / POST)・`/{id}/members/{recipient_id}` (DELETE)

直下 = Worker (wasm 専用。DB への経路 `src/db.rs` と workers-rs への載せ方 `src/lib.rs`)、`crates/notify/` = 口・SQL。

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
  Hyperdrive (`NOTIFY_HYPERDRIVE`) は本番だけ、staging は VPC (`NOTIFY_DB_VPC`)。`wrangler.toml` の表の順を変えない
  (新しい表は placement より後)。
- **ログ・エラー応答は固定の語だけ。** `{e}`・DB の message・識別子 (tenant_id・行 id)・入力の値を出さない。
  DB の失敗は `notify: <段> failed: db (<kind>)` (kind は SQLSTATE か tokio-postgres の種類の label)。
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。SQL は `crates/notify/src/pg.rs` の 1 か所。
  名前付き prepared statement 禁止。Row を transaction の外へ持ち出さない (owned な型に `TxOutput`)。
  tenant_id の列が在る表は WHERE にも tenant_id を書く。
- rust に無い検査 (role など) を足さない。rust と違う振る舞いにするときは理由を書く (いまは 1 つ: members の POST と bulk のグループ追加は、宛先が同じ tenant に無ければ何も足さない。POST は 404。RLS の WITH CHECK が group の tenant しか見ないため)。
- `coverage_100.toml` の登録を外さない。CI の本数 (recipients_db 8) はテストを足したら上げる。
- public repo: ホスト名・account ID・tenant UUID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-lineworks-worker と同じ rev)。
