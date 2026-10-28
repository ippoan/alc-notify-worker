#!/usr/bin/env bash
# check-exposure.sh の陰性対照 (ippoan/alc-lineworks-worker の同名の script の写し、元は ippoan/alc-vein-worker。Refs ippoan/rust-alc-api#747)。wrangler.toml を 1 か所ずつ崩したコピーで
# exit 1 になること、元のままなら exit 0 になることを確かめる。CI で check-exposure.sh の直後に走る。
set -euo pipefail
cd "$(dirname "$0")/.."

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail=0

expect() {
  local want="$1" label="$2" toml="$3"
  if bash scripts/check-exposure.sh "$toml" >"$tmp/out" 2>&1; then got=0; else got=1; fi
  if [ "$got" = "$want" ]; then
    echo "ok   ${label} (exit ${got})"
  else
    echo "FAIL ${label}: exit ${got}, want ${want}"; sed 's/^/     /' "$tmp/out"; fail=1
  fi
}

mutate() { # $1 = label, $2 = python の置換式 (s を書き換える)
  python3 - wrangler.toml "$tmp/w.toml" "$2" <<'PY'
import re
import sys
s = open(sys.argv[1]).read()
before = s
exec(sys.argv[3])
assert s != before, "mutation did not change wrangler.toml"
open(sys.argv[2], "w").write(s)
PY
  expect 1 "$1" "$tmp/w.toml"
}

expect 0 "wrangler.toml そのまま" wrangler.toml
mutate "staging で workers_dev = true" \
  's = s.replace("name = \"alc-notify-staging\"\nworkers_dev = false", "name = \"alc-notify-staging\"\nworkers_dev = true", 1)'
mutate "staging で preview_urls = true" \
  's = s.replace("preview_urls = false\n\n[env.staging.observability]", "preview_urls = true\n\n[env.staging.observability]", 1)'
mutate "staging 以外の env で workers_dev = true" \
  's += "\n[env.dev]\nname = \"notify-dev\"\nworkers_dev = true\n"'
mutate "staging 以外の env で preview_urls = true" \
  's += "\n[env.preview]\nname = \"notify-preview\"\npreview_urls = true\n"'
mutate "staging 以外の env がトップレベルの true を継承" \
  's = re.sub(r"^workers_dev = false$", "workers_dev = true", s, count=1, flags=re.M) + "\n[env.other]\nname = \"x\"\n"'
mutate "トップレベルの vars に ALLOW_INSECURE_DB" \
  's = s.replace("\n[vars]\n", "\n[vars]\nALLOW_INSECURE_DB = \"1\"\n", 1)'
mutate "staging の vars に ALLOW_INSECURE_DB" \
  's = s.replace("[env.staging.vars]\n", "[env.staging.vars]\nALLOW_INSECURE_DB = \"1\"\n", 1)'
mutate "トップレベルの workers_dev を true に" \
  's = re.sub(r"^workers_dev = false$", "workers_dev = true", s, count=1, flags=re.M)'
mutate "トップレベルの preview_urls を消す" \
  's = re.sub(r"^preview_urls = false\n", "", s, count=1, flags=re.M)'
mutate "本番に routes を足す" \
  's = s.replace("[build]", "routes = [{ pattern = \"notify.example.com\", custom_domain = true }]\n\n[build]", 1)'
mutate "staging に route を足す" \
  's = s.replace("[env.staging.observability]", "route = \"notify.example.com/*\"\n\n[env.staging.observability]", 1)'
mutate "本番に vpc_services (平文の DB 経路) を足す" \
  's = s.replace("[build]", "[[vpc_services]]\nbinding = \"NOTIFY_DB_VPC\"\nservice_id = \"x\"\nremote = true\n\n[build]", 1)'
mutate "staging に durable_objects を足す (この worker は DO も Container も持たない)" \
  's = s.replace("[env.staging.observability]", "[[env.staging.durable_objects.bindings]]\nname = \"NOTIFY_DB\"\nclass_name = \"NotifyDb\"\n\n[env.staging.observability]", 1)'
mutate "staging に hyperdrive (本番の DB へ届く binding) を足す" \
  's = s.replace("[env.staging.observability]", "[[env.staging.hyperdrive]]\nbinding = \"NOTIFY_HYPERDRIVE\"\nid = \"x\"\n\n[env.staging.observability]", 1)'

mutate "staging の NOTIFY_R2 が本番の bucket notify-files を指す" \
  's = s.replace("bucket_name = \"notify-files-staging\"", "bucket_name = \"notify-files\"", 1)'
mutate "staging 以外の env の r2_buckets が本番の bucket を指す" \
  's += "\n[env.other]\nname = \"x\"\nworkers_dev = false\npreview_urls = false\n\n[[env.other.r2_buckets]]\nbinding = \"NOTIFY_R2\"\nbucket_name = \"notify-files\"\n"'


exit "$fail"
