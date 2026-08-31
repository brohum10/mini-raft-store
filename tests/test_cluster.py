import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0)); return sock.getsockname()[1]


class ClusterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(); self.ports = [free_port() for _ in range(3)]
        self.urls = [f"http://127.0.0.1:{p}" for p in self.ports]
        self.procs = [None] * 3
        for i in range(3): self.start(i)

    def tearDown(self):
        for p in self.procs:
            if p and p.poll() is None: p.terminate()
        for p in self.procs:
            if p:
                try: p.wait(timeout=2)
                except subprocess.TimeoutExpired: p.kill()
        shutil.rmtree(self.tmp)

    def start(self, i):
        peers = ",".join(f"n{j+1}={self.urls[j]}" for j in range(3) if j != i)
        self.procs[i] = subprocess.Popen([sys.executable, "-m", "raftstore.server", "--id", f"n{i+1}",
            "--host", "127.0.0.1", "--port", str(self.ports[i]), "--peers", peers,
            "--data-dir", os.path.join(self.tmp, f"n{i+1}")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def call(self, i, method, path, body=None):
        req = urllib.request.Request(self.urls[i] + path,
            json.dumps(body).encode() if body is not None else None,
            {"Content-Type": "application/json"}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=3) as response: return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            try: return exc.code, json.load(exc)
            finally: exc.close()

    def leader(self, timeout=8):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for i, proc in enumerate(self.procs):
                if not proc or proc.poll() is not None: continue
                try:
                    status, body = self.call(i, "GET", "/status")
                    if status == 200 and body["role"] == "leader": return i
                except OSError: pass
            time.sleep(.1)
        self.fail("leader not elected")

    def wait_value(self, indexes, key, value, timeout=6):
        deadline = time.time() + timeout
        while time.time() < deadline:
            good = 0
            for i in indexes:
                try:
                    status, body = self.call(i, "GET", "/kv/" + key)
                    good += status == 200 and body["value"] == value
                except OSError: pass
            if good == len(indexes): return
            time.sleep(.1)
        self.fail(f"value {key}={value} not present on nodes {indexes}")

    def test_leader_crash_preserves_committed_write_and_restart_catches_up(self):
        leader = self.leader()
        status, _ = self.call(leader, "PUT", "/kv/order-42", {"value": "paid"})
        self.assertEqual(200, status)
        self.procs[leader].kill(); self.procs[leader].wait()
        new_leader = self.leader()
        self.wait_value([new_leader], "order-42", "paid")
        status, _ = self.call(new_leader, "PUT", "/kv/order-43", {"value": "shipped"})
        self.assertEqual(200, status)
        self.start(leader)
        self.wait_value([leader], "order-42", "paid")
        self.wait_value([leader], "order-43", "shipped")

    def test_follower_crash_during_writes_recovers(self):
        leader = self.leader(); follower = next(i for i in range(3) if i != leader)
        self.procs[follower].kill(); self.procs[follower].wait()
        for n in range(5):
            status, _ = self.call(leader, "PUT", f"/kv/k{n}", {"value": n})
            self.assertEqual(200, status)
        self.start(follower)
        for n in range(5): self.wait_value([follower], f"k{n}", n)
