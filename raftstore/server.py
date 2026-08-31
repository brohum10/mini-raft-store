import argparse
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

from .node import Peer, RaftNode


class Handler(BaseHTTPRequestHandler):
    server_version = "MiniRaftStore/1.0"

    def _json(self, status, body, headers=None):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for key, value in (headers or {}).items(): self.send_header(key, value)
        self.end_headers(); self.wfile.write(data)

    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    @property
    def node(self): return self.server.node

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/health", "/status"):
            return self._json(200, self.node.status())
        if path.startswith("/kv/"):
            key = unquote(path[4:])
            with self.node.lock:
                if key not in self.node.kv: return self._json(404, {"error": "key not found"})
                return self._json(200, {"key": key, "value": self.node.kv[key]})
        self._json(404, {"error": "not found"})

    def do_PUT(self):
        path = urlparse(self.path).path
        if not path.startswith("/kv/"): return self._json(404, {"error": "not found"})
        key, body = unquote(path[4:]), self._body()
        if "value" not in body: return self._json(400, {"error": "value is required"})
        ok, leader = self.node.put(key, body["value"])
        if ok: return self._json(200, {"ok": True, "key": key, "value": body["value"]})
        leader_url = self.server.peer_urls.get(leader)
        headers = {"Location": leader_url + path} if leader_url else {}
        self._json(307 if leader_url else 503,
                   {"error": "not leader", "leader_id": leader, "leader_url": leader_url}, headers)

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/raft/vote": return self._json(200, self.node.request_vote(self._body()))
        if path == "/raft/append": return self._json(200, self.node.append_entries(self._body()))
        self._json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        logging.getLogger("http").debug(fmt, *args)


def parse_peers(spec):
    return [Peer(*item.split("=", 1)) for item in spec.split(",") if item]


def main():
    p = argparse.ArgumentParser(description="Run a Mini Raft Store node")
    p.add_argument("--id", default=os.getenv("NODE_ID"), required=os.getenv("NODE_ID") is None)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    p.add_argument("--peers", default=os.getenv("PEERS", ""))
    p.add_argument("--data-dir", default=os.getenv("DATA_DIR", "./data"))
    args = p.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    peers = parse_peers(args.peers)
    node = RaftNode(args.id, peers, args.data_dir)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.node = node
    server.peer_urls = {p.node_id: p.url for p in peers} | {args.id: f"http://localhost:{args.port}"}
    node.start()
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: node.stop(); server.server_close()


if __name__ == "__main__": main()

