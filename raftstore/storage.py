import json
import os
import tempfile
import threading
from pathlib import Path


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
            with self.path.open(encoding="utf-8") as f:
                return json.load(f)

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

