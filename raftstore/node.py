import json
import logging
import random
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

from .storage import Storage

LOG = logging.getLogger("raftstore")


@dataclass(frozen=True)
class Peer:
    node_id: str
    url: str


class RaftNode:
    def __init__(self, node_id: str, peers: list[Peer], data_dir: str,
                 election_min: float = .45, election_max: float = .85,
                 heartbeat: float = .12):
        self.node_id, self.peers = node_id, peers
        self.storage = Storage(data_dir)
        saved = self.storage.load()
        self.term = saved["term"]
        self.voted_for = saved["voted_for"]
        self.log = saved["log"]
        self.commit_index = min(saved.get("commit_index", -1), len(self.log) - 1)
        self.last_applied = -1
        self.kv = {}
        self.role, self.leader_id = "follower", None
        self.leader_barrier_index = None
        self.next_index, self.match_index = {}, {}
        self.election_min, self.election_max, self.heartbeat = election_min, election_max, heartbeat
        self.lock = threading.RLock()
        self.replication_lock = threading.Lock()
        self.commit_cond = threading.Condition(self.lock)
        self.stop_event = threading.Event()
        self.last_contact = time.monotonic()
        self.election_deadline = 0.0
        self._reset_deadline()
        self._apply_committed()
        self.thread = None

    @property
    def majority(self):
        return (len(self.peers) + 1) // 2 + 1

    def _persist(self):
        self.storage.save({"term": self.term, "voted_for": self.voted_for,
                           "log": self.log, "commit_index": self.commit_index})

    def _reset_deadline(self):
        self.election_deadline = time.monotonic() + random.uniform(self.election_min, self.election_max)

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            if entry.get("op") == "set":
                self.kv[entry["key"]] = entry["value"]
            elif entry.get("op") == "delete":
                self.kv.pop(entry["key"], None)

    def start(self):
        self.thread = threading.Thread(target=self._run, name=f"raft-{self.node_id}", daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)

    def _run(self):
        while not self.stop_event.wait(.03):
            with self.lock:
                role, due = self.role, time.monotonic() >= self.election_deadline
            if role == "leader":
                self.replicate_all()
                self.stop_event.wait(self.heartbeat)
            elif due:
                self.start_election()

    def _rpc(self, peer: Peer, path: str, payload: dict, timeout=.35):
        req = urllib.request.Request(peer.url + path, json.dumps(payload).encode(),
                                     {"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.load(response)
        except (OSError, urllib.error.URLError, json.JSONDecodeError):
            return None

    def _become_follower(self, term: int, leader_id=None):
        changed = term > self.term
        if changed:
            self.term, self.voted_for = term, None
        self.role, self.leader_id = "follower", leader_id
        self.leader_barrier_index = None
        self.last_contact = time.monotonic()
        self._reset_deadline()
        if changed:
            self._persist()

    def start_election(self):
        with self.lock:
            self.role, self.term = "candidate", self.term + 1
            election_term = self.term
            self.voted_for, self.leader_id = self.node_id, None
            self._reset_deadline()
            self._persist()
            last_index = len(self.log) - 1
            last_term = self.log[-1]["term"] if self.log else 0
        votes = 1
        request = {"term": election_term, "candidate_id": self.node_id,
                   "last_log_index": last_index, "last_log_term": last_term}
        for peer in self.peers:
            response = self._rpc(peer, "/raft/vote", request)
            if not response:
                continue
            with self.lock:
                if response["term"] > self.term:
                    self._become_follower(response["term"])
                    return
                if self.role != "candidate" or self.term != election_term:
                    return
                votes += bool(response.get("vote_granted"))
        with self.lock:
            if self.role == "candidate" and self.term == election_term and votes >= self.majority:
                self.role, self.leader_id = "leader", self.node_id
                # A current-term no-op lets the new leader safely commit entries
                # inherited from an earlier term (Raft paper, section 5.4.2).
                self.log.append({"term": self.term, "op": "noop"})
                self.leader_barrier_index = len(self.log) - 1
                self._persist()
                self.next_index = {p.node_id: len(self.log) for p in self.peers}
                self.match_index = {p.node_id: -1 for p in self.peers}
                self.election_deadline = time.monotonic()
                LOG.info("node %s became leader for term %s", self.node_id, self.term)

    def request_vote(self, req):
        with self.lock:
            term = int(req["term"])
            if term > self.term:
                self._become_follower(term)
            my_last_term = self.log[-1]["term"] if self.log else 0
            my_last_index = len(self.log) - 1
            up_to_date = (req["last_log_term"], req["last_log_index"]) >= (my_last_term, my_last_index)
            grant = term == self.term and up_to_date and self.voted_for in (None, req["candidate_id"])
            if grant:
                self.voted_for = req["candidate_id"]
                self.last_contact = time.monotonic()
                self._reset_deadline()
                self._persist()
            return {"term": self.term, "vote_granted": grant}

    def append_entries(self, req):
        with self.lock:
            term = int(req["term"])
            if term < self.term:
                return {"term": self.term, "success": False, "match_index": len(self.log) - 1}
            self._become_follower(term, req["leader_id"])
            prev = int(req["prev_log_index"])
            if prev >= len(self.log) or (prev >= 0 and self.log[prev]["term"] != req["prev_log_term"]):
                hint = min(prev, len(self.log) - 1)
                return {"term": self.term, "success": False, "match_index": hint}
            incoming = req.get("entries", [])
            pos = prev + 1
            changed = False
            for entry in incoming:
                if pos < len(self.log) and self.log[pos]["term"] != entry["term"]:
                    self.log = self.log[:pos]
                    changed = True
                if pos == len(self.log):
                    self.log.append(entry)
                    changed = True
                pos += 1
            new_commit = min(int(req.get("leader_commit", -1)), len(self.log) - 1)
            if new_commit > self.commit_index:
                self.commit_index = new_commit
                changed = True
                self._apply_committed()
                self.commit_cond.notify_all()
            if changed:
                self._persist()
            return {"term": self.term, "success": True, "match_index": pos - 1}

    def _replicate_peer(self, peer):
        with self.lock:
            if self.role != "leader": return False
            nxt = self.next_index.get(peer.node_id, len(self.log))
            prev = nxt - 1
            req = {"term": self.term, "leader_id": self.node_id, "prev_log_index": prev,
                   "prev_log_term": self.log[prev]["term"] if prev >= 0 else 0,
                   "entries": self.log[nxt:], "leader_commit": self.commit_index}
            sent_term = self.term
        response = self._rpc(peer, "/raft/append", req)
        if not response: return False
        with self.lock:
            if response["term"] > self.term:
                self._become_follower(response["term"]); return False
            if self.role != "leader" or self.term != sent_term: return False
            if response.get("success"):
                matched = int(response["match_index"])
                self.match_index[peer.node_id] = matched
                self.next_index[peer.node_id] = matched + 1
                return True
            else:
                self.next_index[peer.node_id] = max(0, min(nxt - 1, int(response.get("match_index", -1)) + 1))
                return False

    def replicate_all(self):
        # Client writes, reads, and the heartbeat loop may all request a round.
        # Serializing rounds prevents delayed responses from moving a follower's
        # replication cursor backwards after a newer response already advanced it.
        with self.replication_lock:
            return self._replication_round()

    def _replication_round(self):
        acknowledgements = set()
        acknowledgements_lock = threading.Lock()

        def replicate(peer):
            if self._replicate_peer(peer):
                with acknowledgements_lock:
                    acknowledgements.add(peer.node_id)

        threads = [threading.Thread(target=replicate, args=(p,), daemon=True) for p in self.peers]
        for t in threads: t.start()
        for t in threads: t.join(timeout=.5)
        with self.lock:
            if self.role != "leader": return 0
            for idx in range(len(self.log) - 1, self.commit_index, -1):
                replicated = 1 + sum(m >= idx for m in self.match_index.values())
                if replicated >= self.majority and self.log[idx]["term"] == self.term:
                    self.commit_index = idx
                    self._apply_committed()
                    self._persist()
                    self.commit_cond.notify_all()
                    break
            return 1 + len(acknowledgements)

    def _propose(self, command, timeout=2.5):
        with self.lock:
            if self.role != "leader":
                return False, self.leader_id
            index = len(self.log)
            self.log.append({"term": self.term, **command})
            self._persist()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.replicate_all()
            with self.lock:
                if self.role != "leader": return False, self.leader_id
                if self.commit_index >= index: return True, self.node_id
                self.commit_cond.wait(timeout=.05)
        return False, self.node_id

    def put(self, key, value, timeout=2.5):
        return self._propose({"op": "set", "key": key, "value": value}, timeout)

    def delete(self, key, timeout=2.5):
        return self._propose({"op": "delete", "key": key}, timeout)

    def linearizable_get(self, key, timeout=1.5):
        """Read only after this leader proves authority to a current-term quorum.

        The current-term no-op is a leadership barrier: committing it also commits
        every preceding entry known to this leader before the value is returned.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if self.role != "leader":
                    return False, self.leader_id, False, None
                barrier = self.leader_barrier_index
            acknowledgements = self.replicate_all()
            with self.lock:
                if self.role != "leader":
                    return False, self.leader_id, False, None
                if acknowledgements >= self.majority and barrier is not None and self.commit_index >= barrier:
                    return True, self.node_id, key in self.kv, self.kv.get(key)
            self.stop_event.wait(.03)
        return False, self.node_id, False, None

    def status(self):
        with self.lock:
            status = {"node_id": self.node_id, "role": self.role, "term": self.term,
                    "leader_id": self.leader_id, "commit_index": self.commit_index,
                    "last_applied": self.last_applied, "log_length": len(self.log)}
            if self.role == "leader":
                status["replication"] = {
                    peer.node_id: {
                        "next_index": self.next_index.get(peer.node_id, len(self.log)),
                        "match_index": self.match_index.get(peer.node_id, -1),
                    }
                    for peer in self.peers
                }
            return status
