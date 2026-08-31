# Mini Raft Store

A dependency-free, durable distributed key-value store that implements the essential Raft protocol. Three nodes elect a leader, replicate writes to a majority, survive process crashes, and repair lagging or conflicting logs after restart.

> This is an educational Raft subset built to make the protocol inspectable. It is not a replacement for production systems such as etcd.

## What it demonstrates

- Randomized leader election with term-scoped voting
- Leader heartbeats and automatic failover
- Raft log matching and conflict repair via `nextIndex` backtracking
- Majority-acknowledged commits; clients never receive success before quorum
- Durable term, vote, log, and commit index using `fsync` plus atomic rename
- State-machine replay and follower catch-up after a crash
- HTTP redirects and a leader-discovering CLI client
- End-to-end tests that kill leaders and followers during cluster activity
- Persistent Docker volumes and GitHub Actions CI

## Architecture

```text
                         PUT /kv/order-42
                                  │
                     ┌────────────▼────────────┐
                     │  node1 — LEADER, term 4 │
                     │ append → replicate →    │
                     │ majority → commit       │
                     └───────┬─────────┬────────┘
                  AppendEntries       AppendEntries
                          │                 │
                ┌─────────▼──────┐ ┌────────▼───────┐
                │ node2 FOLLOWER │ │ node3 FOLLOWER │
                │ durable log    │ │ durable log    │
                └────────────────┘ └────────────────┘

             A write succeeds after any 2 of 3 nodes persist it.
```

Every node exposes both client and Raft RPC endpoints. The leader maintains a per-follower replication cursor. A rejected append makes the leader walk that cursor backward until the logs share a prefix, then retransmit the missing suffix. Commit indexes flow in subsequent heartbeats, causing followers to apply the same deterministic state-machine commands.

## Quick start

Requirements: Docker with Compose.

```bash
docker compose up --build -d

# Find the elected leader
curl -s localhost:8001/status
curl -s localhost:8002/status
curl -s localhost:8003/status

# The client discovers/follows the leader automatically
docker compose exec node1 python -m raftstore.client \
  --nodes http://node1:8000,http://node2:8000,http://node3:8000 \
  put order-42 paid

curl -s localhost:8002/kv/order-42
```

You can also send a write to any node. Followers respond with HTTP `307` and the known leader in both the `Location` header and JSON body:

```bash
curl -i -X PUT localhost:8002/kv/color \
  -H 'Content-Type: application/json' -d '{"value":"blue"}'
```

## Failure demo

Kill the current leader (replace `node1` after checking `/status`), wait about a second, then write through the newly elected leader:

```bash
docker compose kill node1
sleep 2

curl -s localhost:8002/kv/order-42    # committed value remains readable
curl -X PUT localhost:8002/kv/order-43 \
  -H 'Content-Type: application/json' -d '{"value":"shipped"}'

docker compose start node1
sleep 2
curl -s localhost:8001/kv/order-43    # restarted node caught up
```

The old leader cannot acknowledge a write without a majority. If it becomes isolated, a majority elects a newer-term leader; when the old node reconnects, it steps down and its uncommitted suffix is repaired.

## API

| Method | Path | Purpose |
|---|---|---|
| `PUT` | `/kv/{key}` | Replicate and commit `{ "value": ... }` |
| `GET` | `/kv/{key}` | Read the locally applied committed value |
| `GET` | `/status` | Role, term, leader, log, and commit metadata |
| `GET` | `/health` | Liveness and node status |
| `POST` | `/raft/vote` | Internal RequestVote RPC |
| `POST` | `/raft/append` | Internal AppendEntries RPC |

Reads are local and may briefly lag on a follower. For linearizable reads, query the current leader. Membership is intentionally static and configured at process start.

## Run without Docker

Python 3.11+ is the only requirement. In three terminals:

```bash
python -m raftstore.server --id n1 --port 8001 --data-dir data/n1 \
  --peers n2=http://localhost:8002,n3=http://localhost:8003
python -m raftstore.server --id n2 --port 8002 --data-dir data/n2 \
  --peers n1=http://localhost:8001,n3=http://localhost:8003
python -m raftstore.server --id n3 --port 8003 --data-dir data/n3 \
  --peers n1=http://localhost:8001,n2=http://localhost:8002
```

## Tests

```bash
python -m unittest discover -v
```

The integration suite launches real OS processes on ephemeral ports and verifies:

1. A committed write survives killing the leader.
2. The remaining majority elects a new leader and continues accepting writes.
3. The restarted node recovers both pre- and post-failure values.
4. A follower killed during several writes catches up completely after restart.

## Protocol scope and trade-offs

This project follows Raft's core safety rules: one vote per term, log freshness checks, log-prefix matching, current-term majority commits, and persistence before successful RPC responses. To remain compact, it deliberately omits snapshots, dynamic membership, pre-vote, authentication, TLS, batching, and linearizable follower reads. Those are natural next steps for production hardening.

## Repository layout

```text
raftstore/node.py       Raft state machine, elections, replication, commit
raftstore/storage.py    Atomic durable state
raftstore/server.py     HTTP client and peer API
raftstore/client.py     Leader-discovering CLI
tests/                  Unit and multi-process failure tests
docker-compose.yml      Three-node persistent cluster
```

Licensed under the MIT License.
