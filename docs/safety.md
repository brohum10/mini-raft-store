# Safety and fault model

This repository is an educational implementation of Raft's core replicated-log protocol, not a production database.

## Invariants implemented

- **Election safety:** a node persists at most one vote per term.
- **Leader completeness:** voters reject candidates whose logs are less up to date.
- **Log matching:** followers accept entries only after matching the preceding index and term.
- **Conflict repair:** leaders backtrack `next_index` and replace conflicting suffixes.
- **Commit safety:** leaders advance commit directly only for entries from their current term replicated to a majority.
- **Durable acknowledgement:** term, vote, append, and commit state are atomically persisted before success is returned.
- **Ordered application:** the state machine applies each committed entry exactly once per process lifetime and replays through the persisted commit index after restart.
- **Read safety:** linearizable reads require both a committed current-term barrier and a fresh majority acknowledgement.

## Faults exercised by tests

- leader process termination followed by re-election
- follower downtime during multiple writes
- restart from durable state and full catch-up
- divergent uncommitted suffix repair
- replicated delete across failover
- malformed or corrupt persistence files

The tests use independent OS processes and TCP connections, not mocked consensus calls.

## Explicit non-goals

The implementation assumes a static, trusted cluster and crash-stop/restart failures. It does not defend against Byzantine nodes, compromised peers, disk lies, or arbitrary network attackers. It also omits log snapshots, joint-consensus membership changes, TLS/authentication, pre-vote, request deduplication, multi-key transactions, and performance batching.

Those limits are why the service identifies itself as an inspectable protocol project rather than a production replacement for etcd or Consul.
