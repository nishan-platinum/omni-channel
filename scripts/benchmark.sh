#!/usr/bin/env bash
# Repeatable M01 benchmark (see docs/architecture/performance-testing.md).
#
#   ./scripts/benchmark.sh [base_url]          default http://localhost:3000
#   DURATION=30s CONCURRENCY=64 ./scripts/benchmark.sh
#
# Requirements (benchmark only — never needed to run the application):
#   oha (HTTP load generator: `cargo install oha`), curl, python3.
# Run the app in RELEASE mode with access logging off for comparable numbers:
#   ACCESS_LOG=false docker compose up --build      (the Docker image is a release build)
#   or: ACCESS_LOG=false cargo run --release
# Authorization, RLS and tenant isolation stay fully enabled; only the benchmark tenant's API
# rate limit is raised (through the normal host quota UI) so the guardrail does not cap the run.
set -euo pipefail

BASE="${1:-http://localhost:3000}"
DURATION="${DURATION:-20s}"
CONCURRENCY="${CONCURRENCY:-32}"
LOOP_N="${LOOP_N:-200}"
LOOP_P="${LOOP_P:-8}"
cd "$(dirname "$0")/.."
set -a; [ -f .env ] && . ./.env; set +a
command -v oha >/dev/null || { echo "oha not found — install with: cargo install oha" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required for result parsing" >&2; exit 2; }

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="bench-results/$STAMP"; mkdir -p "$OUT"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
SA_EMAIL="$BOOTSTRAP_SUPERADMIN_EMAIL"; SA_PASS="$BOOTSTRAP_SUPERADMIN_PASSWORD"
TA_PASS="Bench-Tenant-Admin-Pw1!"
SUFFIX="$(date +%s)$RANDOM"
json() { python3 -c "import sys,json; d=json.load(sys.stdin); print(eval('d'+sys.argv[1]))" "$1"; }
csrf_of() { grep -o 'name="_csrf" value="[^"]*"' "$1" | head -1 | sed 's/.*value="//;s/"$//'; }

echo "== Setup (benchmark tenants)"
SA_TOKEN="$(curl -s -X POST "$BASE/v1/bootstrap/token" -H 'content-type: application/json' -d "{\"email\":\"$SA_EMAIL\",\"password\":\"$SA_PASS\"}" | json '["data"]["access_token"]')"
api() { curl -s -H "authorization: Bearer $SA_TOKEN" -H 'content-type: application/json' "$@"; }
mk() { api -X POST "$BASE/v1/tenants" -d "{\"name\":\"Bench $1 $SUFFIX\",\"region\":\"my-central\",\"plan_id\":\"01920000-0000-7000-8000-000000000001\",\"primary_admin_email\":\"bench@$1-$SUFFIX.example\",\"tenant_code\":\"bench-$1-$SUFFIX\"}" | json '["data"]["tenant_id"]'; }
TID="$(mk a)"; OTHER="$(mk b)"
api -X PATCH "$BASE/v1/tenants/$TID/status" -d '{"status":"active"}' >/dev/null
api -X PATCH "$BASE/v1/tenants/$OTHER/status" -d '{"status":"active"}' >/dev/null

# Host browser session: raise the bench tenant's guardrails and read the invitation (dev outbox).
JAR="$WORK/sa.jar"
curl -s -c "$JAR" -b "$JAR" "$BASE/login" -o "$WORK/l.html"
curl -s -o /dev/null -c "$JAR" -b "$JAR" -X POST "$BASE/login" --data-urlencode "email=$SA_EMAIL" --data-urlencode "password=$SA_PASS" --data-urlencode "_csrf=$(csrf_of "$WORK/l.html")"
curl -s -b "$JAR" "$BASE/admin/tenants/$TID/quotas" -o "$WORK/q.html"
CSRF="$(csrf_of "$WORK/q.html")"
LIMITS=(--data-urlencode "_csrf=$CSRF")
for m in api_requests_per_minute campaign_sends_per_hour; do
  LIMITS+=(--data-urlencode "limit:$m=1000000000000" --data-urlencode "threshold:$m=0.8")
done
curl -s -o /dev/null -b "$JAR" -X POST "$BASE/admin/tenants/$TID/quotas/limits" "${LIMITS[@]}"
curl -s -b "$JAR" "$BASE/dev/outbox" -o "$WORK/outbox.html"
TOKEN="$(python3 - "$WORK/outbox.html" "bench-a-$SUFFIX" <<'PY'
import html, re, sys
s = open(sys.argv[1]).read()
for m in re.finditer(r'<pre class="wrap">(.*?)</pre>', s, re.S):
    b = html.unescape(m.group(1))
    if sys.argv[2] in b:
        print(re.search(r'token=([A-Za-z0-9_-]+)', b).group(1)); break
PY
)"
[ -n "$TOKEN" ] || { echo "invitation not found (APP_ENV must be development for the dev outbox)" >&2; exit 1; }
IJAR="$WORK/i.jar"
curl -s -c "$IJAR" -b "$IJAR" "$BASE/invitations/accept?token=$TOKEN" -o "$WORK/a.html"
curl -s -o /dev/null -b "$IJAR" -X POST "$BASE/invitations/accept" --data-urlencode "_csrf=$(csrf_of "$WORK/a.html")" --data-urlencode "token=$TOKEN" --data-urlencode "password=$TA_PASS" --data-urlencode "confirm=$TA_PASS"
TA_TOKEN="$(curl -s -X POST "$BASE/v1/bootstrap/token" -H 'content-type: application/json' -d "{\"email\":\"bench@a-$SUFFIX.example\",\"password\":\"$TA_PASS\",\"tenant_code\":\"bench-a-$SUFFIX\"}" | json '["data"]["access_token"]')"

run_oha() { # name, expected status, oha args...
  local name="$1" expect="$2"; shift 2
  echo "-- $name"
  oha -z "$DURATION" -c "$CONCURRENCY" --no-tui --output-format json "$@" > "$OUT/$name.json" 2>/dev/null || true
  python3 - "$OUT/$name.json" "$name" "$expect" >> "$OUT/summary.tsv" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
s = d.get("summary", {}); p = d.get("latencyPercentiles", {})
codes = d.get("statusCodeDistribution", {})
total = sum(codes.values()) or 1
ok = codes.get(sys.argv[3], 0)
ms = lambda v: f"{(v or 0)*1000:.2f}"
print("\t".join([sys.argv[2], f"{s.get('requestsPerSec',0):.0f}", ms(p.get("p50")), ms(p.get("p95")), ms(p.get("p99")),
                 f"{100*(total-ok)/total:.2f}", str(total)]))
PY
}

run_loop() { # name, count, parallelism, command template (uses $i)
  local name="$1" n="$2" par="$3" cmd="$4"
  echo "-- $name ($n requests, $par parallel)"
  seq 1 "$n" | xargs -P "$par" -I{} bash -c "i={}; $cmd" > "$OUT/$name.raw" 2>/dev/null || true
  python3 - "$OUT/$name.raw" "$name" >> "$OUT/summary.tsv" <<'PY'
import sys
rows = [l.split() for l in open(sys.argv[1]) if l.strip()]
lat = sorted(float(r[1]) * 1000 for r in rows)
err = sum(1 for r in rows if not r[0].startswith("2"))
q = lambda f: lat[min(len(lat) - 1, int(f * len(lat)))] if lat else 0
print("\t".join([sys.argv[2], "n/a", f"{q(.5):.2f}", f"{q(.95):.2f}", f"{q(.99):.2f}", f"{100*err/max(1,len(rows)):.2f}", str(len(rows))]))
PY
}

printf 'scenario\trps\tp50_ms\tp95_ms\tp99_ms\terror_pct\trequests\n' > "$OUT/summary.tsv"
H_TA=(-H "authorization: Bearer $TA_TOKEN")
H_SA=(-H "authorization: Bearer $SA_TOKEN")
echo "== Load scenarios (duration $DURATION, concurrency $CONCURRENCY)"
run_oha session_validation 200 "${H_TA[@]}" "$BASE/v1/tenants/$TID"
run_oha tenant_list 200 "${H_SA[@]}" "$BASE/v1/tenants?limit=25"
run_oha tenant_fetch_super_admin 200 "${H_SA[@]}" "$BASE/v1/tenants/$TID"
run_oha config_read 200 "${H_TA[@]}" "$BASE/v1/tenants/$TID/config"
run_oha quota_check 200 "${H_TA[@]}" -m POST -H 'content-type: application/json' -d '{"metric":"campaign_sends_per_hour","amount":1}' "$BASE/v1/tenants/$TID/quota/consume"
run_oha isolation_rejection 403 "${H_TA[@]}" "$BASE/v1/tenants/$OTHER"
echo "-- login (Argon2id, concurrency 8)"
CONCURRENCY_SAVE="$CONCURRENCY"; CONCURRENCY=8
run_oha login 200 -m POST -H 'content-type: application/json' -d "{\"email\":\"bench@a-$SUFFIX.example\",\"password\":\"$TA_PASS\",\"tenant_code\":\"bench-a-$SUFFIX\"}" "$BASE/v1/bootstrap/token"
CONCURRENCY="$CONCURRENCY_SAVE"

export BASE SA_TOKEN TA_TOKEN TID SUFFIX
run_loop tenant_creation "$LOOP_N" "$LOOP_P" 'curl -s -o /dev/null -w "%{http_code} %{time_total}\n" -X POST "$BASE/v1/tenants" -H "authorization: Bearer $SA_TOKEN" -H "content-type: application/json" -d "{\"name\":\"Bench c$i\",\"plan_id\":\"01920000-0000-7000-8000-000000000001\",\"primary_admin_email\":\"c$i@c$SUFFIX.example\",\"tenant_code\":\"bc-$SUFFIX-$i\"}"'
run_loop config_update "$LOOP_N" "$LOOP_P" 'cur=$([ $((i % 2)) -eq 0 ] && echo MYR || echo SGD); curl -s -o /dev/null -w "%{http_code} %{time_total}\n" -X PATCH "$BASE/v1/tenants/$TID/config" -H "authorization: Bearer $TA_TOKEN" -H "content-type: application/json" -d "{\"config\":{\"locale.currency\":\"$cur\"}}"'
run_loop lifecycle_update "$LOOP_N" 1 'st=$([ $((i % 2)) -eq 1 ] && echo suspended || echo active); curl -s -o /dev/null -w "%{http_code} %{time_total}\n" -X PATCH "$BASE/v1/tenants/$TID/status" -H "authorization: Bearer $SA_TOKEN" -H "content-type: application/json" -d "{\"status\":\"$st\",\"reason\":\"benchmark\"}"'

echo "== Resources"
{
  echo "## Resources ($STAMP)"
  if command -v docker >/dev/null && docker compose ps app --status running -q 2>/dev/null | grep -q .; then
    echo '```'; docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' 2>/dev/null; echo '```'
    echo "- startup: $(docker compose logs app 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' | grep -o 'startup_ms[^0-9]*[0-9]*' | tail -1 | grep -o '[0-9]*$') ms"
    echo "- central DB connections (app pool): $(docker compose exec -T central-db psql -U crm_owner -d omni_control -tAc "select count(*) from pg_stat_activity where application_name='omni-m01'" 2>/dev/null)"
  else
    PID="$(pgrep -x omni-m01 | head -1 || true)"
    [ -n "$PID" ] && echo "- process RSS/CPU: $(ps -o rss=,pcpu= -p "$PID" | awk '{printf "%.1f MiB, %s%% CPU", $1/1024, $2}')"
  fi
} > "$OUT/resources.md"

python3 - "$OUT/summary.tsv" > "$OUT/summary.md" <<'PY'
import sys
rows = [l.rstrip("\n").split("\t") for l in open(sys.argv[1])]
print("| " + " | ".join(rows[0]) + " |"); print("|" + "---|" * len(rows[0]))
for r in rows[1:]: print("| " + " | ".join(r) + " |")
PY
cat "$OUT/summary.md"; echo; cat "$OUT/resources.md"
echo; echo "Results written to $OUT/"
