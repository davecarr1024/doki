#!/usr/bin/env bash
# smoke_test.sh — Quick sanity check against a running Doki cluster.
# Assumes: coordinator at localhost:7000, nodes at 8001, 8002, 8003.

set -euo pipefail

COORD="http://localhost:7000"
NODES=("http://localhost:8001" "http://localhost:8002" "http://localhost:8003")

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC} $1"; }
fail() { echo -e "${RED}FAIL${NC} $1"; exit 1; }

check() {
  local desc="$1"
  local url="$2"
  local expected_status="${3:-200}"
  status=$(curl -s -o /dev/null -w "%{http_code}" "$url")
  if [ "$status" -eq "$expected_status" ]; then
    pass "$desc (HTTP $status)"
  else
    fail "$desc (expected HTTP $expected_status, got $status)"
  fi
}

echo "=== Doki Smoke Test ==="
echo ""

echo "--- Coordinator ---"
check "coordinator /ready" "$COORD/ready"
check "coordinator /status" "$COORD/status"
check "coordinator /shardmap" "$COORD/shardmap"

echo ""
echo "--- Nodes ---"
for i in "${!NODES[@]}"; do
  node="${NODES[$i]}"
  check "node-$((i+1)) /ready" "$node/ready"
  check "node-$((i+1)) /status" "$node/status"
done

echo ""
echo "--- Cluster State ---"
alive_count=$(curl -s "$COORD/status" | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(1 for n in d['nodes'] if n['is_alive']))")
echo "Alive nodes: $alive_count"
if [ "$alive_count" -ge 2 ]; then
  pass "quorum available ($alive_count/3 nodes alive)"
else
  fail "quorum unavailable ($alive_count/3 nodes alive)"
fi

echo ""
echo "=== Smoke test passed ==="
