import argparse
import json
import urllib.error
import urllib.request
from urllib.parse import quote


def request(nodes, method, path, value=None):
    queue = list(nodes)
    seen = set()
    while queue:
        base = queue.pop(0).rstrip("/")
        if base in seen: continue
        seen.add(base)
        req = urllib.request.Request(base + path,
            None if value is None else json.dumps({"value": value}).encode(),
            {"Content-Type": "application/json"}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=3) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                body = json.load(exc)
                if exc.code in (307, 503) and body.get("leader_url"):
                    queue.insert(0, body["leader_url"])
                elif exc.code in (400, 404, 413, 414):
                    return exc.code, body
            finally:
                exc.close()
        except OSError: pass
    raise RuntimeError("no reachable leader")


def main():
    p = argparse.ArgumentParser(description="Leader-discovering Mini Raft Store client")
    p.add_argument("--nodes", default="http://localhost:8001,http://localhost:8002,http://localhost:8003")
    sub = p.add_subparsers(dest="command", required=True)
    get = sub.add_parser("get"); get.add_argument("key")
    get.add_argument("--local", action="store_true", help="allow a potentially stale local read")
    put = sub.add_parser("put"); put.add_argument("key"); put.add_argument("value")
    delete = sub.add_parser("delete"); delete.add_argument("key")
    args = p.parse_args(); nodes = args.nodes.split(",")
    method = {"get": "GET", "put": "PUT", "delete": "DELETE"}[args.command]
    path = "/kv/" + quote(args.key, safe="")
    if args.command == "get" and not args.local:
        path += "?consistency=linearizable"
    status, body = request(nodes, method, path, getattr(args, "value", None))
    print(json.dumps(body, indent=2)); raise SystemExit(0 if status < 400 else 1)


if __name__ == "__main__": main()
