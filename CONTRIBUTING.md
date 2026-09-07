# Contributing to Mini Raft Store

Contributions are welcome when they make the consensus protocol clearer without weakening its safety properties.

## Local checks

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install .
python -m compileall -q raftstore tests
python -m unittest discover -v
docker build -t mini-raft-store .
```

## Expectations

- State the Raft safety or liveness property affected by a change.
- Add deterministic unit coverage and a multi-process failure scenario where appropriate.
- Preserve durable term, vote, log, and commit-index ordering.
- Keep network and storage failures explicit rather than silently recovering unsafe state.
- Update `docs/safety.md` for protocol deviations or new limitations.

Pull requests should describe the invariant, failure schedule tested, compatibility impact, and verification performed.
