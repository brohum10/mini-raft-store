import argparse
import json
import urllib.error
import urllib.request


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
            body = json.load(exc)
            if exc.code in (307, 503) and body.get("leader_url"):
                queue.insert(0, body["leader_url"])
            elif exc.code == 404: return exc.code, body
        except OSError: pass
    raise RuntimeError("no reachable leader")


def main():
    p = argparse.ArgumentParser(description="Leader-discovering Mini Raft Store client")
    p.add_argument("--nodes", default="http://localhost:8001,http://localhost:8002,http://localhost:8003")
    sub = p.add_subparsers(dest="command", required=True)
    get = sub.add_parser("get"); get.add_argument("key")
    put = sub.add_parser("put"); put.add_argument("key"); put.add_argument("value")
    args = p.parse_args(); nodes = args.nodes.split(",")
    status, body = request(nodes, "GET" if args.command == "get" else "PUT", "/kv/" + args.key,
                           getattr(args, "value", None))
    print(json.dumps(body, indent=2)); raise SystemExit(0 if status < 400 else 1)


if __name__ == "__main__": main()
