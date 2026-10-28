# alc-notify-worker

notify の配信・宛先の Worker `alc-notify` (Refs ippoan/rust-alc-api#747)。rust-alc-api の `crates/alc-notify` のうち、
配信・宛先・LINE WORKS の口と、LINE・LINE WORKS の webhook の受け口 (W8b)、文書とメールの閲覧の口 (W8c-1) を移す。
文書の書きの口 (upload・DELETE)・メールの取り込み (ingest)・抽出と黒塗りの背景処理とやり直しの口 (W8c-2) も持つ (rust の notify の口は残らない)。 型は ippoan/alc-lineworks-worker と同じ。
**呼び手は auth-worker の Service Binding だけ** (呼び手を認証しない。auth-worker が付けた `X-Tenant-ID` を信頼する)。例外は ingest で、
呼び手は nuxt-notify の email-receiver の Service Binding (`NOTIFY`) だけ。**ingest を auth-worker の振り分け表 (`ALC_BINDING_ROUTES`) に載せない** (載せると外からどの tenant にも書ける)。

いまの口 (path・method・JSON・status は rust と同じ。nuxt-notify と auth-worker がそのまま使う):
- `/api/notify/recipients/{id}/resend-verification` (POST。本文なし。確認メールの再送。rust に無い口。admin だけ。無ければ作って `{status:"created"}`、
  未確認なら消して作り直して `{status:"resent"}`、確認済みは消さず 409 `already_verified`。宛先なし 404・メールなし 400 `no_email`・Cloudflare 未設定 503 `not_configured`・
  Cloudflare の失敗 502 `upstream_error`。消した後の作成の失敗はログの語が `recreate`。Cloudflare に再送の API が無いための代わり)
- `/api/notify/recipients` (GET / POST。POST と PUT は provider が `line` / `lineworks` / `email` 以外なら 400 `invalid_provider`、
  provider ごとの必須が空なら 400 `line_user_id_required` / `lineworks_user_id_required` / `email_required`。rust に無い検査)・`/bulk` (POST)・`/{id}` (GET / PUT / DELETE)・`/email-status` (GET。rust に無い口。
  宛先のメールアドレスの Cloudflare Email Routing の送り先としての状態 `verified` / `pending` / `not_registered`。未設定なら `{configured:false}`)
  **Cloudflare の送り先一覧 (`GET .../email/routing/addresses`) の応答の `result_info` は `total_pages` を持たない (資料と違う)。ページ数は `total_count` を 50 で割って数える。**
- `/api/notify/employees` (GET。rust に無い口。社員マスタの `[{id, name, code, role}]` (role = 役割の配列 `driver`/`manager`/`admin`。血圧の日報を受け取れるのは manager・admin)。削除済みを除く・社員コードの順。admin だけ。宛先の画面の選択肢)
- 宛先の応答は `employee_id` (社員マスタの社員。null か UUID。migration 184) を持つ。PUT で `employee_id` を UUID にすれば結び付け・`null` で解除・送らなければ変えない
  (admin だけ。他の宛先に結び付いた社員は 409 `employee_already_linked`、この tenant に無い・削除済みは 400 `employee_not_found`)。
  `/api/notify/recipients/link-employees-by-name` (POST。body `{"dry_run": bool}`、既定 true。admin だけ。応答 `{dry_run, candidates, skipped, linked}`。
  宛先と社員の名前を空白 (半角・全角) を全部除いて比べ、一致する社員がちょうど 1 人・宛先も社員も未結合のものだけを結ぶ。
  結ばない理由 `skipped[].reason` は `already_linked`・`no_match`・`multiple_employees`・`employee_already_linked`・`multiple_recipients`)
- `/api/notify/groups` (GET / POST)・`/{id}` (GET / PUT / DELETE)・`/{id}/members` (GET / POST)・`/{id}/members/{recipient_id}` (DELETE)
- `/api/notify/line-config` (GET / POST / DELETE。POST は秘密値を `SSO_ENCRYPTION_KEY` で暗号化。GET は暗号文を返さない)
- `/api/notify/lineworks/channels` (GET)・`/{id}` (DELETE)・`/{id}/test-send` (POST)
- `/api/notify/lineworks/users` (GET)・`/api/notify/lineworks/login-activity?days=` (GET)
- webhook の受け口 (W8b。**tenant のヘッダーを持たない** = tenant の layer の外。署名・bot_id から SECURITY DEFINER の関数で tenant を引く。
  テナントが決まる前の読みは `tenant_tx(Uuid::nil())` の中でその関数だけを呼ぶ (ippoan/alc-lineworks-worker の `pg.rs` と同じ型)):
  - `/api/internal/notify/line/webhook` (POST。auth-worker の `POST /line/webhook` が raw body と `x-line-signature` を渡す。全テナントの
    有効な LINE の設定 (`list_enabled_line_configs()`) の channel secret を `SSO_ENCRYPTION_KEY` で復号して HMAC-SHA256 を照合。
    署名なし・JSON でない 400、合わない 401、合った設定に key_id / private_key が無い・復号できない 500。`follow` なら profile API の表示名
    (取れなければ `LINE User <先頭 8 文字>`) で宛先を作る・名前を更新する (書けなくても 200)。応答は本文なし)
  - `/api/internal/lineworks/bot-secret/{bot_id}` (GET。`{bot_secret_encrypted}`。404 `bot_not_found`・`bot_secret_not_configured`、500 `lookup_failed`)・
    `/api/internal/lineworks/event` (POST。auth-worker の Durable Object が検証済みのイベントを送る。join/joined でトークルームを足す・
    leave/left で外す。`{ok:true}`。404 `bot_not_found`、500 `lookup_failed`・`upsert_failed`・`mark_left_failed`)
- 文書とメールの閲覧 (W8c-1。`crates/notify/src/documents.rs`。ファイルは R2 の binding `NOTIFY_R2` (本番 `notify-files`・staging `notify-files-staging`。
  rust の `NOTIFY_R2_BUCKET` と同じ bucket):
  `/api/notify/documents?limit=&offset=` (既定 50・0)・`/{id}` (`{document, deliveries}`。document は rust の `NotifyDocument` の全列)・
  `/{id}/preview?original=` (黒塗り済み `redacted_r2_key`、無ければ・`original=true` なら原本。Content-Type は key の拡張子)・
  `/{id}/download` (原本を `application/octet-stream` の添付)・`/api/notify/emails` (email_message_id ごと 200 件)・`/api/notify/emails/{message_id}`。
  文書・メールが無ければ 404、R2 の binding が無ければ preview 503・download 500、R2 に無い・読めないなら 500 (rust と同じ)
- 文書の削除・upload・取り込み (W8c-2 PR-3b): `DELETE /api/notify/documents/{id}` (`documents.rs`。無ければ 404、行を消し (配信の記録は CASCADE)、
  R2 の原本も消す (失敗はログだけ)。204)・`POST /api/notify/documents/upload` (`intake.rs`。multipart。pdf・docx・xlsx・png・jpg・jpeg 以外 400、
  0 件 400、20 件超・25 MiB 超 413。R2 `{tenant}/manual/{batch}/{名前}`。201 `{document_ids, count}`)・`POST /api/notify/ingest` (`intake.rs`。
  **tenant の layer の外**、Service Binding の信頼だけ (`X-Worker-Secret` は見ない)。`tenant_short_id` → `tenants.short_id` (空 400・無い 404)、
  添付 0 件 400・21 件以上 413・base64 が読めない 400・25 MiB 超 413。R2 `{tenant}/email/{email_message_id}/{名前}`。201 `{email_message_id, document_ids, count}`)。
  本文の上限は rust と同じ 20 MiB。PDF は黒塗り・全部で抽出を queue に積む (積めなくても 201)。行は 1 つの transaction で作る (rust は 1 件ずつ)。
  batch・email_message_id は DB の `gen_random_uuid()`
- 抽出・黒塗りのやり直し (W8c-2。`crates/notify/src/recompute.rs`): `/api/notify/documents/{id}/redact-recompute` (POST。黒塗りの結果を空にして
  `pending` へ戻し queue に積む。戻せなければ 500)・`/{id}/extract-recompute` (POST。`extraction_status` を `pending` へ戻し (失敗はログだけ) queue に積む)。
  どちらも 202 本文なし・文書が無ければ 404・queue に積めなければ 500 (rust の `tokio::spawn` は失敗しない)
- 抽出・黒塗りの背景処理 (W8c-2。`crates/notify/src/background.rs`、consumer は `src/lib.rs` の `#[event(queue)]`): Cloudflare Queues の `notify-docs`
  (staging `notify-docs-staging`、binding `NOTIFY_DOCS_QUEUE`、max_batch_size 1) のメッセージ `{kind: "extract"|"redact", tenant_id, doc_id, attempt}`。
  Gemini と pdfium は alc-redact (GCP Cloud Run) の `POST /extract`・`POST /redact` を auth-worker の RPC `AlcRedactEntrypoint` 越しに呼ぶ
  (binding `ALC_REDACT`。`src/redact.rs`。SA の鍵はこの worker に無い)。状態の列・`*_error` の接頭辞は rust の `background_extract.rs`・
  `background_redaction.rs` と同じ。黒塗りの JPEG は R2 `{tenant}/redacted/{doc}.jpg` (`image/jpeg`)。黒塗りの終わり (completed・skipped・failed) は
  nuxt-notify の notify-realtime-bus の `POST /broadcast` (Service Binding `NOTIFY_REALTIME_BUS`、ヘッダー `X-Broadcast-Secret` = Secrets Store の
  `NOTIFY_REDACT_BROADCAST_SECRET`。失敗はログだけ) に rust の `RedactEvent` と同じ JSON を送る (抽出は rust と同じく送らない)。
  **失敗を DB に書いたら ack** (Gemini を呼び直さない)。送り直すのは一時的な失敗 (R2 の読み書き (無いキーは除く)・RPC・auth-worker の口のコード
  `redact_unconfigured`・`upstream_auth`・`upstream_unreachable`・固定のコードを持たない 5xx・DB) だけで、本文の `attempt` を進めて 30 秒・60 秒後に
  送り直し、3 回目でも一時的なら失敗を書いて ack (workers-rs 0.8 は Queues の試行回数を出さないので本文で数える。Queues の retry は requeue に失敗したときだけ)。
  二重に配られても壊れないよう、黒塗りは `pending`・`processing`、抽出は `pending` のときだけ進める
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

直下 = Worker (wasm 専用。DB への経路 `src/db.rs`・lineworks-worker の binding `src/lineworks.rs`・workers-rs への載せ方 `src/lib.rs`)、`crates/notify/` = 口・SQL。R2 の読み書きは `src/r2.rs` (trait は `crates/notify/src/store.rs`)、
queue は `src/queue.rs` (trait は `crates/notify/src/jobs.rs`)、auth-worker の RPC は `src/redact.rs`、realtime-bus は `src/bus.rs` (trait は `crates/notify/src/background.rs`)。

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
  `NOTIFY_FRONTEND_URL`・`CF_ACCOUNT_ID` は両方の env に書く。queue (`NOTIFY_DOCS_QUEUE`)・`ALC_REDACT` (auth-worker の `AlcRedactEntrypoint`)・
  `NOTIFY_REALTIME_BUS`・`NOTIFY_REDACT_BROADCAST_SECRET` も両方の env に書き、env (staging) は本番の queue・service・secret を指さない (scripts/check-exposure.sh が検査)。R2 (`NOTIFY_R2`) も両方に書き、env (staging) は本番の bucket を指さない (scripts/check-exposure.sh が検査) (binding も vars も env に継承されない)。`wrangler.toml` の表の順を変えない
  (新しい表は placement より後。`[env.staging]` と `[env.staging.observability]` の間に挟まない)。
- **ログ・エラー応答は固定の語だけ。** `{e}`・DB の message・識別子 (tenant_id・行 id)・入力の値を出さない。
  DB の失敗は `notify: <段> failed: db (<kind>)` (kind は SQLSTATE か tokio-postgres の種類の label)。
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。SQL は `crates/notify/src/pg.rs` の 1 か所。
  名前付き prepared statement 禁止。Row を transaction の外へ持ち出さない (owned な型に `TxOutput`)。
  tenant_id の列が在る表は WHERE にも tenant_id を書く。
- rust に無い検査 (role など) を足さない。rust と違う振る舞いにするときは理由を書く (いまは 7 つ: (1) members の POST と bulk のグループ追加は、宛先が同じ tenant に無ければ何も足さない。POST は 404。RLS の WITH CHECK が group の tenant しか見ないため。(2) LINE WORKS の口の失敗の本文は lineworks-worker の status と語 (`error`・`missing_scope` の `scope`・語の形の `message`) だけを写し、自由文 (rust の 502 の `message`) は載せない。tenant に bot が無いときの語は lineworks-worker の `bot_config_not_found` (rust は `no_lineworks_config`。画面は status しか見ない)。ログに上流の文を出さないため。(3) 宛先の保存で Cloudflare の送り先を作るのは `X-User-Role` が `admin` のときだけ (それ以外は宛先は保存するが送り先は作らない)。任意のアドレスに Cloudflare の確認メールが飛ぶため。`email-status` は読むだけなので role を見ない。(4) 確認メールの再送は、未確認の送り先に限り消して作り直す (Cloudflare に再送の API が無いため。確認済みは消さない。「宛先を消しても・変えても送り先は消さない」の、未確認に限った例外。ユーザー決定 2026-10-19)；(5) 宛先の POST / PUT は provider ごとの必須を検査して 400 (`line` は line_user_id・`lineworks` は lineworks_user_id・`email` は email。空白だけも空。PUT は既存の行と入力を合成して検査する。理由: 画面を経由しない呼び出しでも DB の CHECK `at_least_one_messaging_id` 違反の 500 にしないため。bulk は LINE WORKS の一括登録なので変えない)。加えて provider `email` の宛先は文書配信の対象外 (全員・グループ・指定のどれでも delivery 行も送信も作らない。LINE / LINE WORKS の ID を持たないため。`send_to_recipient` の `UnknownProvider` はそのまま)；(6) 宛先と社員の結び付け (PUT の `employee_id`・社員の一覧・氏名での自動結合) は admin だけ (403 `forbidden`)。誰の測定結果が誰に届くかを決めるため。社員の存在は tenant で確かめる (FK は RLS を通らず別 tenant の id も通るため)；(7) 文書の preview・download の `Content-Disposition` は ASCII だけで組み (`filename*` に UTF-8 の percent-encoding。rust は生の UTF-8。Workers がヘッダの非 ASCII をファイル名入りの警告でログに出すため。ippoan/alc-carins-worker と同じ)、`X-Content-Type-Options: nosniff` を付け、preview を inline で返すのは pdf と画像 (png・jpeg・gif・webp) だけ (svg・txt は `application/octet-stream` の添付。rust は inline。同じ origin で script が動かないよう nuxt-notify の `isSafeInline` と同じ範囲に絞る))。
- `coverage_100.toml` の登録を外さない。CI の本数 (recipients_db 29・lineworks_db 7・distribute_db 10・inbound_db 9・documents_db 9・background_db 12・intake_db 7。
  `.github/workflows/ci.yml` の gate) はテストを足したら上げる。
- public repo: ホスト名・account ID・tenant UUID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
  例外: 利用者に配る公開の viewer URL `NOTIFY_FRONTEND_URL` は wrangler.toml の `[vars]` に平文で置く
  (rust-alc-api の Cloud Run・ippoan/nuxt-notify と同じ値)。Cloudflare の account ID `CF_ACCOUNT_ID` も `[vars]` に平文で置く
  (auth-worker・nuxt-notify などの Worker と同じ扱い。ユーザー決定 2026-10-19、Refs ippoan/alc-app#436)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-core-wasm・alc-worker-db・alc-worker-crypto を同じ rev で)。
