#!/usr/bin/env bash
# End-to-end smoke test against a RUNNING stack (docker compose up, or cargo run).
# Exercises the browser forms (CSRF, sessions), the invitation flow via the development outbox,
# Tenant Admin isolation and the /v1 API including PostgreSQL and MySQL dedicated tenants, then the
# M10 hub gateway end to end (needs HUB_DEMO_SEED=true; uses target/release/hub_load).
#
#   ./scripts/smoke_test.sh [base_url]        (default http://localhost:3000)
#
# Requires: bash, curl, python3 (JSON/HTML parsing only). Reads credentials from .env.
set -euo pipefail

BASE="${1:-http://localhost:3000}"
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
set -a; [ -f .env ] && . ./.env; set +a
SA_EMAIL="${BOOTSTRAP_SUPERADMIN_EMAIL:?BOOTSTRAP_SUPERADMIN_EMAIL not set}"
SA_PASS="${BOOTSTRAP_SUPERADMIN_PASSWORD:?BOOTSTRAP_SUPERADMIN_PASSWORD not set}"
TA_PASS="Smoke-Tenant-Admin-Pw1!"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
PASS=0; FAIL=0

ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (expected $3, got $2)"; fi; }
json() { python3 -c "import sys,json; d=json.load(sys.stdin); print(eval('d'+sys.argv[1]))" "$1"; }
csrf_of() { grep -o 'name="_csrf" value="[^"]*"' "$1" | head -1 | sed 's/.*value="//;s/"$//'; }
status() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
SUFFIX="$(date +%s)$RANDOM"

echo "== Platform"
check "GET /health" "$(status "$BASE/health")" 200
check "GET /ready" "$(status "$BASE/ready")" 200

echo "== Super Admin browser login"
JAR="$WORK/sa.jar"
curl -s -c "$JAR" -b "$JAR" "$BASE/login" -o "$WORK/login.html"
check "login page renders" "$(grep -c 'Sign in' "$WORK/login.html" | tr -d ' ' | sed 's/^[1-9][0-9]*$/yes/')" yes
LC="$(csrf_of "$WORK/login.html")"
check "login without CSRF is refused" "$(status -b "$JAR" -X POST "$BASE/login" --data-urlencode "email=$SA_EMAIL" --data-urlencode "password=$SA_PASS")" 403
check "Super Admin login" "$(status -c "$JAR" -b "$JAR" -X POST "$BASE/login" --data-urlencode "email=$SA_EMAIL" --data-urlencode "password=$SA_PASS" --data-urlencode "_csrf=$LC")" 303
check "dashboard" "$(status -b "$JAR" "$BASE/admin")" 200
check "tenant directory" "$(status -b "$JAR" "$BASE/admin/tenants")" 200
curl -s -b "$JAR" "$BASE/admin/tenants/new" -o "$WORK/new.html"
CSRF="$(csrf_of "$WORK/new.html")"

echo "== Create + activate a tenant through the UI"
CODE="smoke-$SUFFIX"; ADMIN="admin@$CODE.example"
LOC="$(curl -s -o /dev/null -w '%{redirect_url}' -b "$JAR" -X POST "$BASE/admin/tenants" \
  --data-urlencode "_csrf=$CSRF" --data-urlencode "name=Smoke Org $SUFFIX" --data-urlencode "region=my-central" \
  --data-urlencode "plan_id=01920000-0000-7000-8000-000000000001" --data-urlencode "primary_admin_email=$ADMIN" \
  --data-urlencode "tenant_code=$CODE" --data-urlencode "template_code=tpl-contact-centre")"
TID="${LOC##*/}"
[ -n "$TID" ] && ok "tenant created ($CODE → $TID)" || bad "tenant creation redirect"
check "activation" "$(status -b "$JAR" -X POST "$BASE/admin/tenants/$TID/status" --data-urlencode "_csrf=$CSRF" --data-urlencode "status=active" --data-urlencode "reason=")" 303
curl -s -b "$JAR" "$BASE/admin/tenants/$TID" -o "$WORK/detail.html"
check "status badge shows active" "$(grep -c 'Lifecycle status">active' "$WORK/detail.html" | sed 's/^[1-9][0-9]*$/yes/')" yes
for tab in config quotas branding storage support sandboxes baselines release keys offboarding audit backups; do
  check "tab $tab" "$(status -b "$JAR" "$BASE/admin/tenants/$TID/$tab")" 200
done
check "feature beyond plan via UI is refused (redirect with error)" "$(status -b "$JAR" -X POST "$BASE/admin/tenants/$TID/features/module.automated_marketing" --data-urlencode "_csrf=$CSRF" --data-urlencode "enabled=true")" 303

echo "== Invitation via the development outbox"
curl -s -b "$JAR" "$BASE/dev/outbox" -o "$WORK/outbox.html"
LINK="$(python3 - "$WORK/outbox.html" "$CODE" <<'PY'
import html, re, sys
s = open(sys.argv[1]).read()
for m in re.finditer(r'<pre class="wrap">(.*?)</pre>', s, re.S):
    body = html.unescape(m.group(1))
    if sys.argv[2] in body:
        print(re.search(r'(https?://\S+/invitations/accept\?token=[A-Za-z0-9_-]+)', body).group(1)); break
PY
)"
[ -n "$LINK" ] && ok "invitation link found in outbox" || bad "invitation link missing"
TOKEN="${LINK##*token=}"
IJAR="$WORK/inv.jar"
curl -s -c "$IJAR" -b "$IJAR" "$BASE/invitations/accept?token=$TOKEN" -o "$WORK/accept.html"
IC="$(csrf_of "$WORK/accept.html")"
check "set password" "$(status -b "$IJAR" -X POST "$BASE/invitations/accept" --data-urlencode "_csrf=$IC" --data-urlencode "token=$TOKEN" --data-urlencode "password=$TA_PASS" --data-urlencode "confirm=$TA_PASS")" 303
check "Super Admin logout" "$(status -b "$JAR" -X POST "$BASE/logout" --data-urlencode "_csrf=$CSRF")" 303

echo "== Tenant Admin login + isolation"
TJAR="$WORK/ta.jar"
curl -s -c "$TJAR" -b "$TJAR" "$BASE/login" -o "$WORK/login2.html"
check "Tenant Admin login" "$(status -c "$TJAR" -b "$TJAR" -X POST "$BASE/login" --data-urlencode "email=$ADMIN" --data-urlencode "password=$TA_PASS" --data-urlencode "_csrf=$(csrf_of "$WORK/login2.html")")" 303
check "own tenant overview" "$(status -b "$TJAR" "$BASE/tenant")" 200
check "own configuration" "$(status -b "$TJAR" "$BASE/tenant/config")" 200
check "host console forbidden" "$(status -b "$TJAR" "$BASE/admin")" 403

echo "== /v1 API"
SA_TOKEN="$(curl -s -X POST "$BASE/v1/bootstrap/token" -H 'content-type: application/json' -d "{\"email\":\"$SA_EMAIL\",\"password\":\"$SA_PASS\"}" | json '["data"]["access_token"]')"
TA_TOKEN="$(curl -s -X POST "$BASE/v1/bootstrap/token" -H 'content-type: application/json' -d "{\"email\":\"$ADMIN\",\"password\":\"$TA_PASS\",\"tenant_code\":\"$CODE\"}" | json '["data"]["access_token"]')"
api() { curl -s -H "authorization: Bearer $1" -H 'content-type: application/json' "${@:2}"; }
OTHER="$(api "$SA_TOKEN" -X POST "$BASE/v1/tenants" -d "{\"name\":\"Other $SUFFIX\",\"region\":\"my-central\",\"plan_id\":\"01920000-0000-7000-8000-000000000001\",\"primary_admin_email\":\"o@o$SUFFIX.example\"}" | json '["data"]["tenant_id"]')"
[ -n "$OTHER" ] && ok "POST /v1/tenants (201)" || bad "POST /v1/tenants"
check "GET own tenant" "$(status -H "authorization: Bearer $TA_TOKEN" "$BASE/v1/tenants/$TID")" 200
check "GET other tenant → 403" "$(status -H "authorization: Bearer $TA_TOKEN" "$BASE/v1/tenants/$OTHER")" 403
check "PATCH other tenant config → 403" "$(status -X PATCH -H "authorization: Bearer $TA_TOKEN" -H 'content-type: application/json' -d '{"config":{"locale.currency":"USD"}}' "$BASE/v1/tenants/$OTHER/config")" 403
check "GET config" "$(status -H "authorization: Bearer $TA_TOKEN" "$BASE/v1/tenants/$TID/config")" 200
check "GET quota" "$(status -H "authorization: Bearer $TA_TOKEN" "$BASE/v1/tenants/$TID/quota")" 200
check "PATCH branding invalid colour → 400" "$(status -X PATCH -H "authorization: Bearer $TA_TOKEN" -H 'content-type: application/json' -d '{"primary_color":"red"}' "$BASE/v1/tenants/$TID/branding")" 400
check "PATCH branding" "$(status -X PATCH -H "authorization: Bearer $TA_TOKEN" -H 'content-type: application/json' -d '{"primary_color":"#123456"}' "$BASE/v1/tenants/$TID/branding")" 200
check "non-entitled feature → 403" "$(status -X PATCH -H "authorization: Bearer $TA_TOKEN" -H 'content-type: application/json' -d '{"feature_flags":{"module.automated_marketing":true}}' "$BASE/v1/tenants/$TID/config")" 403
check "illegal transition → 409" "$(status -X PATCH -H "authorization: Bearer $SA_TOKEN" -H 'content-type: application/json' -d '{"status":"terminated"}' "$BASE/v1/tenants/$TID/status")" 409
check "suspend" "$(status -X PATCH -H "authorization: Bearer $SA_TOKEN" -H 'content-type: application/json' -d '{"status":"suspended","reason":"smoke test"}' "$BASE/v1/tenants/$TID/status")" 200
check "suspended tenant login → 403" "$(status -X POST "$BASE/v1/bootstrap/token" -H 'content-type: application/json' -d "{\"email\":\"$ADMIN\",\"password\":\"$TA_PASS\",\"tenant_code\":\"$CODE\"}")" 403
check "reinstate" "$(status -X PATCH -H "authorization: Bearer $SA_TOKEN" -H 'content-type: application/json' -d '{"status":"active"}' "$BASE/v1/tenants/$TID/status")" 200

echo "== Dedicated tenant databases"
for target in dedicated-pg-my-central dedicated-mysql-my-central; do
  R="$(api "$SA_TOKEN" -X POST "$BASE/v1/tenants" -d "{\"name\":\"Regulated $target $SUFFIX\",\"region\":\"my-central\",\"plan_id\":\"01920000-0000-7000-8000-000000000003\",\"primary_admin_email\":\"r@r$SUFFIX.example\",\"db_target\":\"$target\"}")"
  check "$target provisioning" "$(echo "$R" | json '["data"]["provisioning_status"]')" completed
  check "$target isolation smoke test" "$(echo "$R" | json '["data"]["isolation_check_status"]')" passed
done

echo "== M10 hub gateway (WhatsApp via fake-meta + web chat over WebSockets; demo tenant)"
check "GET /ready reports the hub bus" "$(curl -s "$BASE/ready" | json '["hub"]["bus_ok"]')" True
check "WhatsApp verify handshake" "$(curl -s "$BASE/v1/hub/channels/whatsapp/webhook?hub.mode=subscribe&hub.verify_token=${WHATSAPP_VERIFY_TOKEN:-dev-verify-token}&hub.challenge=4242")" 4242
check "unsigned WhatsApp webhook → 401" "$(status -X POST -H 'content-type: application/json' -d '{"object":"whatsapp_business_account","entry":[]}' "$BASE/v1/hub/channels/whatsapp/webhook")" 401
# The WebSocket flow needs a real client: the hub_load tool (built on demand).
HUB_LOAD="target/release/hub_load"
[ -x "$HUB_LOAD" ] || cargo build --release --quiet --bin hub_load
if "$HUB_LOAD" smoke --base "$BASE"; then ok "hub end-to-end flow (hub_load smoke)"; else bad "hub end-to-end flow (hub_load smoke)"; fi

echo
echo "Smoke test: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
