# Mini Raft Store

[![CI](https://github.com/brohum10/mini-raft-store/actions/workflows/ci.yml/badge.svg)](https://github.com/brohum10/mini-raft-store/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A dependency-free, durable distributed key-value store that implements the essential Raft protocol. Three nodes elect a leader, replicate mutations to a majority, serve quorum-verified reads, survive process crashes, and repair lagging or conflicting logs after restart.

> This is an educational Raft subset built to make the protocol inspectable. It is not a replacement for production systems such as etcd.

## What it demonstrates

- Randomized leader election with term-scoped voting
- Leader heartbeats and automatic failover
- Raft log matching and conflict repair via `nextIndex` backtracking
- Majority-acknowledged commits; clients never receive success before quorum
- Linearizable leader reads guarded by a current-term commit barrier and fresh quorum acknowledgement
- Replicated `PUT` and `DELETE` operations with deterministic state-machine replay
- Durable term, vote, log, and commit index using `fsync` plus atomic rename
- State-machine replay and follower catch-up after a crash
- HTTP redirects and a leader-discovering CLI client
- End-to-end tests that kill leaders and followers during cluster activity
- Persistent Docker volumes and GitHub Actions CI
- Bounded JSON requests, explicit corruption failures, readiness checks, and per-follower replication diagnostics

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

docker compose exec node1 python -m raftstore.client \
  --nodes http://node1:8000,http://node2:8000,http://node3:8000 \
  get order-42
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
| `DELETE` | `/kv/{key}` | Replicate and commit a key deletion |
| `GET` | `/kv/{key}` | Read the locally applied committed value (fast, potentially stale) |
| `GET` | `/kv/{key}?consistency=linearizable` | Verify leadership with a quorum, then read |
| `GET` | `/status` | Role, term, leader, log, and commit metadata |
| `GET` | `/health` | Liveness and node status |
| `GET` | `/ready` | Readiness after a leader is known |
| `POST` | `/raft/vote` | Internal RequestVote RPC |
| `POST` | `/raft/append` | Internal AppendEntries RPC |

The CLI uses linearizable reads by default and follows leader redirects. Pass `get KEY --local` only when lower latency matters more than freshness. Raw `GET /kv/{key}` remains a deliberately explicit local-read endpoint for follower catch-up inspection. Values can be any JSON value; request bodies are capped at 1 MiB and decoded keys at 512 bytes.

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

Or install the two command-line entry points locally:

```bash
python -m pip install -e .
raftstore-node --help
raftstore --help
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
5. A linearizable read sent to a follower reaches the leader and crosses a current-term quorum barrier.
6. A replicated deletion remains deleted after leader failure and re-election.

Unit tests also cover conflict repair, vote freshness, state-machine replay, missing-key semantics, atomic persistence, and corrupt-state detection.

## Protocol scope and trade-offs

This project follows Raft's core safety rules: one vote per term, log freshness checks, log-prefix matching, current-term majority commits, persistence before successful RPC responses, and a committed current-term barrier before linearizable reads. To remain compact, it deliberately omits snapshots, dynamic membership, pre-vote, authentication, TLS, batching, and follower leases. Those are natural next steps for production hardening.

See [Architecture](docs/architecture.md) for the write/read paths and concurrency model, and [Safety notes](docs/safety.md) for the invariants, fault model, and explicit non-goals.

## Repository layout

```text
raftstore/node.py       Raft state machine, elections, replication, commit
raftstore/storage.py    Atomic durable state
raftstore/server.py     HTTP client and peer API
raftstore/client.py     Leader-discovering CLI
tests/                  Unit and multi-process failure tests
docs/                   Architecture and safety rationale
docker-compose.yml      Three-node persistent cluster
```

Licensed under the MIT License.
