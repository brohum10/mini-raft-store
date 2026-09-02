import json
import os
import tempfile
import threading
from pathlib import Path


class StorageCorruptionError(RuntimeError):
    """Raised when persisted Raft metadata cannot be decoded safely."""


class Storage:
    """Crash-safe single-file persistence using fsync + atomic rename."""

    def __init__(self, data_dir: str):
        self.path = Path(data_dir) / "raft-state.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()

    def load(self) -> dict:
        with self.lock:
            if not self.path.exists():
                return {"term": 0, "voted_for": None, "log": [], "commit_index": -1}
            try:
                with self.path.open(encoding="utf-8") as f:
                    state = json.load(f)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
                raise StorageCorruptionError(f"cannot read Raft state at {self.path}") from error
            required = {"term": int, "log": list, "commit_index": int}
            if not isinstance(state, dict) or any(not isinstance(state.get(key), kind) for key, kind in required.items()):
                raise StorageCorruptionError(f"invalid Raft state schema at {self.path}")
            state.setdefault("voted_for", None)
            entries_valid = all(
                isinstance(entry, dict) and isinstance(entry.get("term"), int)
                for entry in state["log"]
            )
            metadata_valid = (
                state["term"] >= 0
                and -1 <= state["commit_index"] < len(state["log"])
                and (state["voted_for"] is None or isinstance(state["voted_for"], str))
            )
            if not entries_valid or not metadata_valid:
                raise StorageCorruptionError(f"invalid Raft state values at {self.path}")
            return state

    def save(self, state: dict) -> None:
        with self.lock:
            fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".raft-")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state, f, separators=(",", ":"))
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.path)
                dirfd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(dirfd)
                finally:
                    os.close(dirfd)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
