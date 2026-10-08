#!/usr/bin/env bash
# Bake-off eval harness for the gateway (ADR-0014): scenarios E1–E9 against the two-node
# deployment (`docker compose --profile gateway up -d`). The load generator (`gw_load`) runs in
# containers on the same Docker network; latency uses the generator's clock only.
#
# Scaled to the machine it runs on — the bake-off's fixed environment (2 × 4 vCPU / 8 GB nodes,
# separate DB and load boxes) is NOT reproduced here, so results are indicative, not official.
#
#   ./scripts/gateway_eval.sh e1      [SESSIONS=100000 RATE=1000 HOLD=60]   idle sessions on gw1, RSS/session
#   ./scripts/gateway_eval.sh e2      [RATE=500 DURATION=120]               steady 1× load
#   ./scripts/gateway_eval.sh e3      [DURATION=60]                         step load 0.5× 1× 2× 3×
#   ./scripts/gateway_eval.sh e5      [DURATION=180]                        SIGKILL gw2's process every 60 s under 1×
#   ./scripts/gateway_eval.sh e6      [CYCLES=2]                            SIGKILL node gw2, 60 s down, restart; under 1×
#   ./scripts/gateway_eval.sh e7      [SESSIONS=20000]                      rolling restart gw1 then gw2 under idle sessions
#   ./scripts/gateway_eval.sh e9                                            ingest ceiling (webhooks only)
#   ./scripts/gateway_eval.sh t10     [DURATION=150 RUNS=3]                 C30–C33 three times while 1× load runs
#   ./scripts/gateway_eval.sh reset                                         empty conversations/messages (test DB only)
#
# Results: one JSON file per run in ${OUT:-eval-results}/ (raw generator output + docker inspect).
set -euo pipefail
cd "$(dirname "$0")/.."
SCENARIO="${1:?scenario e1|e2|e3|e5|e6|e7|e9}"
NETWORK="${NETWORK:-omni-m01_default}"
IMAGE="${IMAGE:-omni-m01-app:local}"
OUT="${OUT:-eval-results}"
TOKEN="${GATEWAY_TOKEN:-dev-gateway-token-change-me}"
P="docker compose --profile gateway"
mkdir -p "$OUT"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

gen() { # gen NAME ARGS… — run gw_load in a container on the gateway network
  local name="$1"; shift
  docker run --rm --name "gwload-$name-$$" --network "$NETWORK" -e GW_TOKEN="$TOKEN" \
    --ulimit nofile=1048576:1048576 --sysctl net.ipv4.ip_local_port_range="1024 65000" \
    "$IMAGE" gw_load "$@"
}

rss_mb() { # resident memory of a compose service's container (cgroup), MiB
  local id; id="$($P ps -q "$1")"
  docker exec "$id" cat /sys/fs/cgroup/memory.current 2>/dev/null | awk '{printf "%.1f", $1/1048576}'
}

record() { # record SCENARIO FILE_WITH_RESULT_LINES
  local file="$OUT/${STAMP}-$1.json"
  {
    echo "{\"scenario\": \"$1\", \"stamp\": \"$STAMP\", \"results\": ["
    grep '^RESULT ' "$2" | sed 's/^RESULT //' | paste -sd, -
    echo "], \"extra\": $(cat "$2.extra" 2>/dev/null || echo '{}'),"
    echo "\"images\": $(docker inspect --format '{"service":"{{index .Config.Labels "com.docker.compose.service"}}","image":"{{.Image}}"}' $($P ps -q gw1 gw2) | paste -sd, - | sed 's/^/[/;s/$/]/')}"
  } > "$file"
  echo "wrote $file"
}

wait_healthy() {
  for _ in $(seq 1 120); do
    if curl -fs -H "Authorization: Bearer $TOKEN" "http://localhost:$1/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "node on :$1 did not become healthy" >&2; return 1
}

LOG="$(mktemp)"
trap 'rm -f "$LOG" "$LOG".*' EXIT

# Routing rules from the fixture file (200 agents, whatsapp → chat); the conformance suite replaces
# the fixture, so re-read the file first.
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8088/config/reload >/dev/null
depth="$(curl -fsS -H "Authorization: Bearer $TOKEN" http://localhost:8088/presence | python3 -c 'import sys,json; print(sum(json.load(sys.stdin)["queues"].values()))')"
if [ "$depth" != 0 ] && [ "$SCENARIO" != reset ]; then
  echo "queues are not empty ($depth conversations waiting from earlier runs); run: $0 reset" >&2; exit 1
fi

case "$SCENARIO" in
  reset)
    # Every run must start with empty queues: earlier runs' conversations would be served first.
    $P exec -T gateway-db psql -U gateway -d gateway -q -c "TRUNCATE gw.messages, gw.conversations"
    echo "gateway test database: conversations and messages removed"
    exit 0
    ;;
  e1)
    SESSIONS="${SESSIONS:-100000}"; RATE="${RATE:-1000}"; HOLD="${HOLD:-60}"; SHARDS="${SHARDS:-4}"
    base="$(rss_mb gw1)"
    echo "== E1: $SESSIONS idle sessions on gw1 at $RATE/s (${SHARDS} generator containers), hold ${HOLD}s; gw1 RSS before: ${base} MiB"
    for s in $(seq 1 "$SHARDS"); do
      gen "e1-$s" ramp --ws ws://gw1:4000 --sessions $((SESSIONS / SHARDS)) --rate $((RATE / SHARDS)) \
        --hold "$HOLD" --prefix "e1-$STAMP-$s" > "$LOG.$s" 2>&1 &
    done
    peak=0
    while [ "$(jobs -r | wc -l)" -gt 0 ]; do
      sleep 5
      now="$(rss_mb gw1)"; open="$(curl -s -H "Authorization: Bearer $TOKEN" localhost:4001/metrics | awk -F' ' '/gateway_sessions_open\{.*customer/ {print $2}')"
      echo "  gw1 rss ${now} MiB, customer sessions ${open}"
      if [ "${open:-0}" -gt "$peak" ]; then peak="$open"; peak_rss="$now"; fi
    done
    cat "$LOG".[0-9]* > "$LOG"
    kb="$(awk -v r="${peak_rss:-0}" -v b="$base" -v n="$peak" 'BEGIN { if (n > 0) printf "%.1f", (r - b) * 1024 / n; else print "null" }')"
    echo "{\"peak_sessions\": $peak, \"rss_before_mib\": $base, \"rss_at_peak_mib\": ${peak_rss:-0}, \"kb_per_session\": $kb}" > "$LOG.extra"
    grep -h '^RESULT' "$LOG"
    echo "peak sessions on gw1: $peak; RSS ${base} → ${peak_rss:-?} MiB; ≈ ${kb} KB per idle session"
    record e1 "$LOG"
    ;;
  e2)
    IDLE="${IDLE:-0}"   # idle sessions per node held during the run (spec 1×: 50000)
    if [ "$IDLE" -gt 0 ]; then
      echo "== E2 background: $IDLE idle sessions per node"
      for n in gw1 gw2; do for s in 1 2; do
        gen "e2-idle-$n-$s" ramp --ws "ws://$n:4000" --sessions $((IDLE / 2)) --rate 500 --hold $(( ${DURATION:-120} + 120 )) --prefix "e2-$STAMP-$n-$s" > "$LOG.idle-$n-$s" 2>&1 &
      done; done
      until [ "$(curl -s -H "Authorization: Bearer $TOKEN" localhost:4001/metrics | awk '/gateway_sessions_open\{.*customer/ {print $2}')" -ge "$IDLE" ] 2>/dev/null; do sleep 5; done
      until [ "$(curl -s -H "Authorization: Bearer $TOKEN" localhost:4002/metrics | awk '/gateway_sessions_open\{.*customer/ {print $2}')" -ge "$IDLE" ] 2>/dev/null; do sleep 5; done
      echo "   idle sessions open: gw1 $(rss_mb gw1) MiB, gw2 $(rss_mb gw2) MiB"
    fi
    echo "== E2: steady load ${RATE:-500} msg/s for ${DURATION:-120}s through the load balancer"
    gen e2 load --base http://gw-lb:8088 --rate "${RATE:-500}" --duration "${DURATION:-120}" --agents 200 --customers "${CUSTOMERS:-2000}" | tee "$LOG.main"
    echo "{\"idle_per_node\": $IDLE, \"rss_gw1_mib\": $(rss_mb gw1), \"rss_gw2_mib\": $(rss_mb gw2)}" > "$LOG.extra"
    if [ "$IDLE" -gt 0 ]; then docker ps -q --filter "name=gwload-e2-idle" | xargs -r docker stop -t 2 >/dev/null; wait || true; fi
    cat "$LOG".main "$LOG".idle-* 2>/dev/null > "$LOG"
    record e2 "$LOG"
    ;;
  e3)
    for mult in 0.5 1 2 3; do
      rate="$(awk -v m="$mult" 'BEGIN { printf "%d", 500 * m }')"
      echo "== E3 step ${mult}× = ${rate} msg/s for ${DURATION:-60}s"
      gen "e3-$mult" load --base http://gw-lb:8088 --rate "$rate" --duration "${DURATION:-60}" --agents 200 --customers $((2000 * ${mult%.*} > 0 ? 2000 * ${mult%.*} : 1000)) | tee -a "$LOG"
    done
    record e3 "$LOG"
    ;;
  e5)
    DURATION="${DURATION:-180}"
    echo "== E5: 1× load for ${DURATION}s; SIGKILL the gateway process on gw2 every 60 s (Docker restarts it)"
    gen e5 load --base http://gw-lb:8088 --rate 500 --duration "$DURATION" --agents 200 --customers 2000 > "$LOG" 2>&1 &
    gen_pid=$!
    for t in $(seq 60 60 $((DURATION - 30))); do
      sleep 60
      echo "  $(date -u +%T) SIGKILL gw2 process"
      docker kill --signal KILL "$($P ps -q gw2)" >/dev/null
      sleep 1; $P start gw2 >/dev/null 2>&1 || true
    done
    wait "$gen_pid" || true
    cat "$LOG"; record e5 "$LOG"
    ;;
  e6)
    CYCLES="${CYCLES:-2}"; DURATION=$((CYCLES * 90 + 60))
    echo "== E6: 1× load for ${DURATION}s; ${CYCLES}× SIGKILL node gw2, 60 s down, restart"
    gen e6 load --base http://gw-lb:8088 --rate 500 --duration "$DURATION" --agents 200 --customers 2000 > "$LOG" 2>&1 &
    gen_pid=$!
    sleep 20
    for c in $(seq 1 "$CYCLES"); do
      echo "  $(date -u +%T) SIGKILL node gw2 (cycle $c)"
      $P kill -s SIGKILL gw2 >/dev/null 2>&1
      sleep 60
      t0=$(date +%s.%N); $P start gw2 >/dev/null 2>&1; wait_healthy 4002
      echo "  gw2 serving again after $(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN{printf "%.1f", b-a}')s"
      sleep 10
    done
    wait "$gen_pid" || true
    cat "$LOG"; record e6 "$LOG"
    ;;
  e7)
    SESSIONS="${SESSIONS:-20000}"
    echo "== E7: $SESSIONS idle sessions through the LB; rolling restart gw1 then gw2"
    for s in 1 2; do
      gen "e7-$s" ramp --ws ws://gw-lb:8088 --sessions $((SESSIONS / 2)) --rate 1000 --hold 120 --prefix "e7-$STAMP-$s" --reconnect > "$LOG.$s" 2>&1 &
    done
    sleep $((SESSIONS / 1000 + 15))
    for n in gw1 gw2; do
      echo "  $(date -u +%T) restart $n (SIGTERM → drain)"
      $P restart "$n" >/dev/null 2>&1
      [ "$n" = gw1 ] && wait_healthy 4001 || wait_healthy 4002
      sleep 15
    done
    wait || true
    cat "$LOG".[0-9]* > "$LOG"; grep -h '^RESULT' "$LOG"
    record e7 "$LOG"
    ;;
  t10)
    # T10 / E8: the cross-node conformance tests must pass while the cluster carries 1× load. The load
    # goes in as SIP events (skill voice, agents agent-001…200) so the tests own WhatsApp routing.
    curl -fsS -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
      --data @conformance/fixtures/t10-load-agents.json http://localhost:8088/config/reload
    echo "== T10: 1× SIP load for ${DURATION:-150}s; C30–C33 ${RUNS:-3}× during it"
    gen t10 load --base http://gw-lb:8088 --channel sip --rate 500 --duration "${DURATION:-150}" --agents 200 --customers 2000 > "$LOG.main" 2>&1 &
    load_pid=$!
    sleep 20
    if GW_BASE_FIXTURE=/suite/fixtures/t10-load-agents.json FILTER=test_c3 RUNS="${RUNS:-3}" ./conformance/run.sh > "$LOG.suite" 2>&1; then suite=pass; else suite=fail; fi
    grep -E "^(PASSED|FAILED)|passed|failed|Conformance" "$LOG.suite"
    wait "$load_pid" || true
    echo "{\"suite_under_load\": \"$suite\", \"suite_runs\": ${RUNS:-3}}" > "$LOG.extra"
    cat "$LOG.main" > "$LOG"; grep '^RESULT' "$LOG"
    record t10 "$LOG"
    # Back to the fixture file for the next scenarios.
    curl -fsS -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8088/config/reload >/dev/null
    ;;
  e9)
    for rate in 500 1000 2000 4000 8000; do
      echo "== E9 ingest ${rate}/s for 30s (no sessions)"
      gen "e9-$rate" load --base http://gw-lb:8088 --rate "$rate" --duration 30 --agents 0 --customers 5000 | grep -E '^RESULT' | tee -a "$LOG" || true
    done
    record e9 "$LOG"
    ;;
  *) echo "unknown scenario $SCENARIO" >&2; exit 2 ;;
esac
