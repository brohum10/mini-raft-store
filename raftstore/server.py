import argparse
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .node import Peer, RaftNode

MAX_REQUEST_BYTES = 1_048_576
MAX_KEY_BYTES = 512


class RequestError(ValueError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class Handler(BaseHTTPRequestHandler):
    server_version = "MiniRaftStore/1.0"

    def _json(self, status, body, headers=None):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Raft-Node", self.node.node_id)
        for key, value in (headers or {}).items(): self.send_header(key, value)
        self.end_headers(); self.wfile.write(data)

    def _body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as error:
            raise RequestError(400, "invalid Content-Length") from error
        if length > MAX_REQUEST_BYTES:
            raise RequestError(413, "request body exceeds 1 MiB")
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RequestError(400, "body must be valid JSON") from error
        if not isinstance(body, dict):
            raise RequestError(400, "body must be a JSON object")
        return body

    def _key(self, path):
        key = unquote(path[4:])
        if not key:
            raise RequestError(400, "key must not be empty")
        if len(key.encode("utf-8")) > MAX_KEY_BYTES:
            raise RequestError(414, "key exceeds 512 bytes")
        return key

    def _redirect_or_unavailable(self, path, leader):
        leader_url = self.server.peer_urls.get(leader)
        headers = {"Location": leader_url + path} if leader_url else {}
        self._json(
            307 if leader_url else 503,
            {"error": "not leader", "leader_id": leader, "leader_url": leader_url},
            headers,
        )

    @property
    def node(self): return self.server.node

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ("/health", "/status"):
            return self._json(200, self.node.status())
        if path == "/ready":
            status = self.node.status()
            ready = status["role"] == "leader" or status["leader_id"] is not None
            return self._json(200 if ready else 503, {"ready": ready, **status})
        if path.startswith("/kv/"):
            try:
                key = self._key(path)
            except RequestError as error:
                return self._json(error.status, {"error": str(error)})
            consistency = parse_qs(parsed.query).get("consistency", ["local"])[0]
            if consistency not in ("local", "linearizable"):
                return self._json(400, {"error": "consistency must be local or linearizable"})
            if consistency == "linearizable":
                ok, leader, found, value = self.node.linearizable_get(key)
                if not ok:
                    return self._redirect_or_unavailable(path + "?consistency=linearizable", leader)
                if not found:
                    return self._json(404, {"error": "key not found", "consistency": "linearizable"})
                return self._json(200, {"key": key, "value": value, "consistency": "linearizable"})
            with self.node.lock:
                if key not in self.node.kv: return self._json(404, {"error": "key not found"})
                return self._json(200, {"key": key, "value": self.node.kv[key], "consistency": "local"})
        self._json(404, {"error": "not found"})

    def do_PUT(self):
        path = urlparse(self.path).path
        if not path.startswith("/kv/"): return self._json(404, {"error": "not found"})
        try:
            key, body = self._key(path), self._body()
        except RequestError as error:
            return self._json(error.status, {"error": str(error)})
        if "value" not in body: return self._json(400, {"error": "value is required"})
        ok, leader = self.node.put(key, body["value"])
        if ok: return self._json(200, {"ok": True, "key": key, "value": body["value"]})
        self._redirect_or_unavailable(path, leader)

    def do_DELETE(self):
        path = urlparse(self.path).path
        if not path.startswith("/kv/"): return self._json(404, {"error": "not found"})
        try:
            key = self._key(path)
        except RequestError as error:
            return self._json(error.status, {"error": str(error)})
        ok, leader = self.node.delete(key)
        if ok: return self._json(200, {"ok": True, "key": key})
        self._redirect_or_unavailable(path, leader)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            body = self._body()
            if path == "/raft/vote": return self._json(200, self.node.request_vote(body))
            if path == "/raft/append": return self._json(200, self.node.append_entries(body))
        except (RequestError, KeyError, TypeError, ValueError) as error:
            status = error.status if isinstance(error, RequestError) else 400
            return self._json(status, {"error": str(error) or "invalid Raft RPC"})
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
