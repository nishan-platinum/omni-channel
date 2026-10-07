#!/usr/bin/env bash
# Two hub nodes behind one load balancer (docker compose profile "cluster", nginx on :8080).
# Customers and agents chat continuously through the LB while we:
#   1. SIGKILL node app1 (crash)          → clients reconnect to app2 and resume by sequence
#   2. start app1 again
#   3. restart app2 with SIGTERM (rolling) → graceful drain: `reconnect` + close 1012
# Then every message the server acknowledged must be stored (gap-free per conversation) and must
# have reached an agent: "zero acknowledged messages lost". Cross-node delivery is exercised
# because the LB spreads agents and customers over both nodes (not sticky).
#
#   ./scripts/hub_cluster_test.sh            (needs Docker; builds target/release/hub_load)
set -euo pipefail
cd "$(dirname "$0")/.."
LB="${LB:-http://localhost:8080}"
DURATION="${DURATION:-60}"
CUSTOMERS="${CUSTOMERS:-50}"
P="docker compose --profile cluster"

echo "== Starting the cluster (app1, app2, nginx)"
$P up -d app1 app2 lb >/dev/null
wait_ready() {
  for _ in $(seq 1 60); do
    if $P exec -T "$1" bash -c ': </dev/tcp/127.0.0.1/3000' 2>/dev/null; then
      if curl -fs "$LB/ready" >/dev/null 2>&1; then return 0; fi
    fi
    sleep 1
  done
  echo "node $1 did not become ready" >&2; return 1
}
wait_ready app1; wait_ready app2
NODES="$(for _ in 1 2 3 4 5 6; do curl -s "$LB/ready" | python3 -c 'import sys,json; print(json.load(sys.stdin)["hub"]["node"])'; done | sort -u | tr '\n' ' ')"
echo "   LB serves nodes: $NODES"

HUB_LOAD="target/release/hub_load"
[ -x "$HUB_LOAD" ] || cargo build --release --quiet --bin hub_load

echo "== Chaos run: $CUSTOMERS customers for ${DURATION}s through $LB"
"$HUB_LOAD" chaos --base "$LB" --customers "$CUSTOMERS" --duration "$DURATION" --interval-ms 600 &
CHAOS=$!
sleep $((DURATION / 4))
echo "== $(date +%T) SIGKILL app1 (node crash)"
docker kill --signal KILL "$($P ps -q app1)" >/dev/null
sleep $((DURATION / 6))
echo "== $(date +%T) starting app1 again"
$P start app1 >/dev/null
wait_ready app1
sleep $((DURATION / 6))
echo "== $(date +%T) rolling restart of app2 (SIGTERM → graceful drain)"
$P restart app2 >/dev/null
wait_ready app2
set +e
wait "$CHAOS"; RC=$?
set -e
echo
if [ "$RC" -eq 0 ]; then echo "Cluster test: PASSED"; else echo "Cluster test: FAILED (exit $RC)"; fi
exit "$RC"
