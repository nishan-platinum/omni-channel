#!/usr/bin/env bash
# Runs the gateway conformance suite (C01–C51) against the two-node deployment
# (`docker compose --profile gateway up -d`). One line per test ID in the report.
#
#   ./conformance/run.sh                 # whole suite
#   FILTER=test_c2 ./conformance/run.sh  # a subset (pytest -k expression)
#   RUNS=3 ./conformance/run.sh          # must pass 3 times in a row (bake-off rule)
#   NO_FAULTS=1 ./conformance/run.sh     # skip the node-kill tests (C40–C43, C50)
set -euo pipefail
cd "$(dirname "$0")/.."
NETWORK="${NETWORK:-omni-m01_default}"
RUNS="${RUNS:-1}"
docker build -q -t gateway-conformance:local conformance >/dev/null
args=("$@")
filter="${FILTER:-}"
if [ "${NO_FAULTS:-0}" = 1 ]; then filter="${filter:+($filter) and }not test_c4 and not test_c50"; fi
if [ -n "$filter" ]; then args+=(-k "$filter"); fi
for run in $(seq 1 "$RUNS"); do
  echo "== conformance run $run/$RUNS"
  docker run --rm --network "$NETWORK" \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e GW_URL="${GW_URL:-http://gw-lb:8088}" -e GW_NODE_A="${GW_NODE_A:-http://gw1:4000}" \
    -e GW_NODE_B="${GW_NODE_B:-http://gw2:4000}" -e GW_TOKEN="${GATEWAY_TOKEN:-dev-gateway-token-change-me}" \
    gateway-conformance:local "${args[@]}"
done
echo "Conformance: $RUNS/$RUNS runs passed"
