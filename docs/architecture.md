# Architecture

Mini Raft Store separates the consensus state machine from transport and persistence so the safety-critical code remains inspectable.

```text
client / CLI
    │  PUT, DELETE, or linearizable GET
    ▼
ThreadingHTTPServer ─────── follower: 307 + known leader
    │
    ▼
RaftNode (single RLock guards protocol state)
    ├── election loop        randomized timeout, term-scoped vote
    ├── replication workers  one bounded RPC per peer
    ├── commit advancement   majority match + current-term rule
    ├── state machine        deterministic set/delete replay
    └── Storage              temp file → fsync → rename → directory fsync
```

## Mutation path

1. Only the leader appends a command to its durable log.
2. Per-peer workers send `AppendEntries` from that follower's `next_index`.
3. Rejection backs the cursor toward the last common prefix; success advances `match_index`.
4. The leader commits the highest current-term entry present on a majority.
5. Committed entries apply in order. The HTTP request succeeds only after its index is committed.
6. Followers learn the commit index in subsequent heartbeats and replay the same commands.

`DELETE` is a first-class log command rather than a local storage shortcut, so deletion follows the same durability and failover rules as `PUT`.

## Linearizable read path

Election alone does not prove that a newly elected leader has applied everything it can safely commit. On election, the leader appends a no-op entry and records its index as a leadership barrier. A linearizable read waits until:

- that current-term barrier is committed, and
- the leader receives successful current-term `AppendEntries` acknowledgements from a majority during the read.

The first condition advances the state machine through all preceding committed entries. The second prevents an isolated, superseded leader from serving a stale value. Local follower reads remain available as an explicit lower-consistency option.

## Concurrency model

The HTTP server handles client requests concurrently. Protocol state is guarded by one re-entrant lock; disk persistence occurs while that lock is held so no RPC can acknowledge state that has not been made durable. Network calls happen outside the lock. A replication round uses one short-lived worker per follower, avoiding head-of-line blocking on an unavailable peer.

## Persistence boundary

`raft-state.json` contains the term, vote, log, and commit index. Each update writes a sibling temporary file, flushes it, atomically replaces the state file, then flushes the directory entry. Startup validates the top-level schema and fails loudly on corruption instead of silently creating an empty node.
