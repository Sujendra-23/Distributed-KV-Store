# Distributed KV Store

A sharded, replicated key-value store written in Go. A coordinator maps keys
onto storage nodes via consistent hashing, replicates every write to a second
node for durability, resolves conflicting writes with last-writer-wins
versioning, and health-checks the cluster so it automatically routes around a
dead node. Deployable locally, via Docker Compose, on Kubernetes, or
provisioned onto bare hosts with Ansible.

## Why this exists

Storing key-value pairs on one machine is trivial. It stops being trivial the
moment you need to: split data across machines because one can't hold or
serve it all (**sharding**), survive a machine dying without losing data
(**replication**), decide what happens when two writes to the same key race
each other (**conflict resolution**), and keep routing traffic correctly when
a node goes down without a human intervening (**failure detection**). This
project implements all four with the same building blocks production systems
like DynamoDB and Cassandra are built on, deliberately kept small enough to
read end-to-end in one sitting.

## Architecture

```
                    ┌─────────────┐
   client  ───────▶ │ coordinator │  (HTTP API :8080)
                    └──────┬──────┘
                           │ gRPC (hash ring picks primary + replica)
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌────────┐   ┌────────┐   ┌────────┐
         │ node-1 │   │ node-2 │   │ node-3 │   (gRPC :7001-7003)
         └────────┘   └────────┘   └────────┘
```

- **Coordinator** (`cmd/coordinator`) owns a consistent-hash ring (100
  virtual nodes per physical node, `internal/hashring`), polls every storage
  node's `Health` RPC every 3s, and removes dead nodes from the ring so new
  writes route around them. It's the only thing clients talk to, over plain
  HTTP.
- **Storage nodes** (`cmd/node`) hold an in-memory shard (`internal/store`).
  Each is a dumb key-value map that only knows how to serve `Put`/`Get`/
  `Delete`/`Replicate`/`Health` over gRPC — it has no knowledge of the ring
  or the other nodes.
- **Replication factor 2** by default (`replicationFactor` in
  `cmd/coordinator/main.go`): a write goes to the primary node
  synchronously (the client waits for it), then to the replica
  asynchronously (fire-and-forget, so durability doesn't cost write
  latency).
- **Last-writer-wins conflict resolution**: every write carries a per-key
  logical version (`internal/store`). A node rejects any incoming replicated
  write whose version isn't newer than what it already has, so an
  out-of-order replicated write can never clobber a newer one. Deletes are
  tombstones, not hard deletes, so a replica can't resurrect a deleted key.
- **Client** (`cmd/client`) is a thin CLI wrapper around the coordinator's
  HTTP API.

## Running it locally

Requires Go 1.22+.

```bash
make build
make run-local      # 3 nodes + coordinator on :7001-7003 and :8080, foreground
```

In another terminal:

```bash
./bin/client -coordinator localhost:8080 put foo bar
./bin/client -coordinator localhost:8080 get foo       # -> bar
./bin/client -coordinator localhost:8080 delete foo
./bin/client -coordinator localhost:8080 status         # -> ring membership
```

Or talk to it directly over HTTP:

```bash
curl -X PUT --data 'bar' localhost:8080/kv/foo
curl localhost:8080/kv/foo          # -> bar
curl -X DELETE localhost:8080/kv/foo
curl -i localhost:8080/status
```

`make run-local` runs in the foreground; `Ctrl+C` stops the coordinator, but
background node processes started with `&` in the same shell can be left
orphaned holding their ports. If a re-run fails with `address already in
use`, clear everything first:

```bash
pkill -f "bin/node"; pkill -f "bin/coordinator"
```

## DynamoDB storage backend (optional)

Storage nodes default to the in-memory store. Passing `-backend=dynamodb`
(or `KV_BACKEND=dynamodb`) makes a node persist to a DynamoDB table instead,
using the AWS SDK for Go v2 (`internal/dynamostore`).

- Item shape: `{key (S, partition key), value (B), version (N), tombstone (BOOL)}`.
- **Last-writer-wins is enforced by DynamoDB conditional writes.** Every write
  is a `PutItem` with `ConditionExpression: attribute_not_exists(#k) OR #v < :ver`
  on the `version` attribute. A write whose version is not strictly newer gets
  `ConditionalCheckFailedException`, which the store treats as "stale, ignored"
  (the same contract as the in-memory `ApplyReplicated`). Locally originated
  `Put`/`Delete` read the current version (strongly consistent read), attempt a
  conditional write at `version+1`, and retry (max 10) if another writer won.
- **Deletes stay tombstones** (version bumped, value dropped, row kept), so a
  late stale replicated write cannot resurrect a deleted key.
- Each node needs its **own table** (it owns one shard); default name is
  `kvstore-<addr>`, override with `-ddb-table` / `KV_DYNAMODB_TABLE`. The table
  is created on startup (on-demand billing) unless `-ddb-create-table=false`.

| Flag | Env var | Default |
| ---- | ------- | ------- |
| `-backend` | `KV_BACKEND` | `memory` |
| `-ddb-endpoint` | `KV_DYNAMODB_ENDPOINT` | empty = real AWS; set `http://localhost:8000` for DynamoDB Local |
| `-ddb-region` | `AWS_REGION` | `us-east-1` when an endpoint override is set |
| `-ddb-table` | `KV_DYNAMODB_TABLE` | `kvstore-<addr>` |

Run it against DynamoDB Local:

```bash
docker run -d --rm --name ddb-local -p 8000:8000 amazon/dynamodb-local
export KV_DYNAMODB_ENDPOINT=http://localhost:8000
go test ./... -v                 # dynamostore integration tests run (and skip if the var is unset)
./scripts/e2e-dynamodb.sh        # 3 nodes + coordinator on DynamoDB, restarts nodes, checks data survives
```

Or with Compose: `docker compose -f docker-compose.yml -f docker-compose.dynamodb.yml up --build`.

## HTTP API

| Method | Path        | Body       | Response                                  |
| ------ | ----------- | ---------- | ------------------------------------------ |
| PUT    | `/kv/{key}` | raw value  | `204` on success                          |
| GET    | `/kv/{key}` | —          | `200` + value, or `404` if missing        |
| DELETE | `/kv/{key}` | —          | `204` on success                          |
| GET    | `/status`   | —          | `200` + `{"members": [...]}` — live nodes |

## Running it with Docker Compose

```bash
docker compose up --build
curl -X PUT --data 'bar' localhost:8080/kv/foo
curl localhost:8080/kv/foo
```

## Running it on Kubernetes

```bash
kubectl apply -f k8s/storage-statefulset.yaml
kubectl apply -f k8s/coordinator-deployment.yaml
kubectl -n kvstore port-forward svc/kv-coordinator 8080:8080
```

## Provisioning bare hosts with Ansible

```bash
ansible-playbook -i ansible/inventory.ini ansible/deploy.yml
```

## CI/CD

`Jenkinsfile` runs `go build`, `go test -cover`, builds both Docker images,
pushes them on `main`, then triggers the Ansible deploy playbook.

## Testing

```bash
make test    # go test ./... -v
```

9 unit tests cover the hash ring (`internal/hashring`) — replica count,
determinism, fallthrough after a node is removed, distribution balance — and
the store (`internal/store`) — put/get, tombstone deletes, and that a stale
replicated write is correctly rejected in favor of a newer one.

## What's verified vs. simplified

**DynamoDB backend, verified against DynamoDB Local (Docker, `amazon/dynamodb-local`) only - never against real AWS:**
5 integration tests pass (`internal/dynamostore`): version increments, a
**stale replicated write is rejected** (lower and equal versions) and the newer
value survives, delete leaves a tombstone row that blocks resurrection by an
older write, `Count` excludes tombstones, and 8 concurrent `Put`s to one key get
8 distinct versions (1..8, no lost update). `scripts/e2e-dynamodb.sh` passes: a
3-node cluster + coordinator on DynamoDB Local round-trips put/get/delete and
data and tombstones survive killing and restarting all node processes. The node
image still builds (`docker build -f Dockerfile.node`). **Not run:**
`docker-compose.dynamodb.yml`, real AWS (IAM, throttling, latency), LocalStack
for this feature. `Count` (used by the health RPC) is a table `Scan`, so it is
O(table size).

**Verified by actually running it:** `go build ./...` succeeds, all 9 unit
tests pass, and a live 3-node cluster round-trips `put`/`get`/`delete`
(including tombstone deletes correctly 404ing) through both the CLI client
and raw HTTP. Writing 20 keys distributed them across all 3 nodes rather than
hot-spotting one, confirming the hash ring actually shards. Killing a node
mid-run drops it from the coordinator's `/status` within one health-check
interval (3s), and reads/writes keep succeeding on the survivors — the
failover path works, not just the code path.

**Not exercised end-to-end in this environment** (no Docker daemon /
Kubernetes cluster / real hosts available at the time): `docker-compose.yml`
and the two `Dockerfile`s are standard multi-stage Go builds, not run;
`k8s/*.yaml` is validated by hand against the Kubernetes API shapes
(StatefulSet, headless Service, Deployment), not applied to a real cluster;
`ansible/deploy.yml` uses standard `apt`/`docker_container` modules, not run
against real hosts; the `Jenkinsfile` is standard declarative pipeline
syntax, not run against a real Jenkins instance.

**Known limitations** (by design, to keep this a readable reference rather
than a production system):

- **Persistence is opt-in** — the default storage is in-memory only (a node
  restart loses its shard, no WAL/snapshots); use the DynamoDB backend above
  for durable storage.
- **Single coordinator, no HA** — if the coordinator process dies, the
  cluster is unreachable until it's restarted. Every node could in principle
  be coordinator-capable, but isn't here.
- **Clock-based conflict resolution** — versions are wall-clock timestamps,
  which is vulnerable to clock skew across nodes. Production systems
  typically use vector clocks or hybrid logical clocks instead.
- **No quorum reads/writes** — a read can return a stale replica if the
  primary happens to be down when read fails through.
- **Ring only shrinks automatically** — a node coming back healthy is
  re-added, but there's no rebalancing/anti-entropy step to backfill data it
  missed while it was down.
