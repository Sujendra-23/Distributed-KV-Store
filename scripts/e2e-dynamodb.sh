#!/usr/bin/env bash
# End-to-end check of the DynamoDB backend: 3 nodes + coordinator on the host,
# storage in DynamoDB Local. Verifies put/get/delete, that data SURVIVES a node
# restart (impossible with the in-memory backend), and tombstone persistence.
# Requires: DynamoDB Local on $KV_DYNAMODB_ENDPOINT (default http://localhost:8000),
#   e.g.  docker run -d --rm --name ddb-local -p 8000:8000 amazon/dynamodb-local
set -euo pipefail
cd "$(dirname "$0")/.."
export KV_BACKEND=dynamodb KV_DYNAMODB_ENDPOINT="${KV_DYNAMODB_ENDPOINT:-http://localhost:8000}" AWS_REGION=us-east-1
RUN=$(date +%s)
make build >/dev/null
pids=()
cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
start_node() { KV_DYNAMODB_TABLE="kv-e2e-$RUN-$1" ./bin/node -addr=":$2" >"/tmp/kv-e2e-node$1.log" 2>&1 & pids+=($!); eval "NODE$1_PID=$!"; }
start_node 1 7101; start_node 2 7102; start_node 3 7103
./bin/coordinator -http=:8180 -nodes=localhost:7101,localhost:7102,localhost:7103 >/tmp/kv-e2e-coord.log 2>&1 & pids+=($!)
fail() { echo "FAIL: $*"; exit 1; }
# Wait until the coordinator's health loop has all 3 nodes in the ring.
wait_ring() { for _ in $(seq 1 30); do [ "$(curl -s localhost:8180/status | grep -o 'localhost:71' | wc -l)" -ge 3 ] && return 0; sleep 1; done; fail "ring never reached 3 members"; }
wait_ring
curl -sf -X PUT --data 'hello' localhost:8180/kv/foo || fail "put"
[ "$(curl -sf localhost:8180/kv/foo)" = hello ] || fail "get after put"
echo "ok: put/get through coordinator -> DynamoDB-backed nodes"
for i in $(seq 1 20); do curl -sf -X PUT --data "v$i" localhost:8180/kv/key$i; done
sleep 1   # let async replication land
# Restart every node process: data must still be there because it lives in DynamoDB.
kill "$NODE1_PID" "$NODE2_PID" "$NODE3_PID"; sleep 1
start_node 1 7101; start_node 2 7102; start_node 3 7103
wait_ring
[ "$(curl -sf localhost:8180/kv/foo)" = hello ] || fail "foo lost after node restart"
[ "$(curl -sf localhost:8180/kv/key17)" = v17 ] || fail "key17 lost after node restart"
echo "ok: data survived restart of all 3 node processes"
curl -sf -X DELETE localhost:8180/kv/foo
[ "$(curl -s -o /dev/null -w '%{http_code}' localhost:8180/kv/foo)" = 404 ] || fail "delete should 404"
echo "ok: delete -> 404"
kill "$NODE1_PID" "$NODE2_PID" "$NODE3_PID"; sleep 1
start_node 1 7101; start_node 2 7102; start_node 3 7103; wait_ring
[ "$(curl -s -o /dev/null -w '%{http_code}' localhost:8180/kv/foo)" = 404 ] || fail "tombstone lost after restart"
echo "ok: tombstone persisted across restart (key stays deleted)"
echo "E2E PASS"
