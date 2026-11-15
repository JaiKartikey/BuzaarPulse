"""End-to-end smoke test against a running deployment. Standard library only.

    python3 scripts/smoke_test.py http://localhost:8080

Checks the things a user would notice first: you can sign in, prices are
moving, an order fills, an agent answers, and the books balance.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
import uuid

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8080"


def call(method: str, path: str, token: str | None = None, body: object = None, headers: dict[str, str] | None = None):
    request = urllib.request.Request(BASE + path, method=method, data=None if body is None else json.dumps(body).encode())
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read())


def wait_for(check, what: str, timeout: float = 60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if result := check():
            return result
        time.sleep(1)
    raise SystemExit(f"FAIL: {what} within {timeout:.0f}s")


def main() -> None:
    token = call("POST", "/api/auth/demo")["token"]
    print("ok  signed in")

    first = wait_for(lambda: call("GET", "/api/quotes?symbols=TCS", token).get("TCS"), "a TCS quote")
    wait_for(lambda: call("GET", "/api/quotes?symbols=TCS", token)["TCS"]["ts"] > first["ts"], "prices moving")
    print("ok  prices are streaming")

    order = call(
        "POST", "/api/orders", token, {"symbol": "TCS", "side": "buy", "type": "market", "quantity": 1},
        {"Idempotency-Key": f"smoke-{uuid.uuid4()}"},
    )  # fmt: skip
    wait_for(lambda: any(o["id"] == order["id"] and o["status"] == "filled" for o in call("GET", "/api/orders", token)), "the order to fill")
    print("ok  market order filled")

    run = call("POST", "/api/agent/runs", token, {"kind": "copilot", "message": "How is the market doing?"})
    done = wait_for(lambda: (r := call("GET", f"/api/agent/runs/{run['id']}", token))["status"] in ("completed", "failed") and r, "the agent run")
    if done["status"] != "completed":
        raise SystemExit(f"FAIL: agent run {done['status']}: {done.get('error')}")
    print(f"ok  agent answered with {sum(s['type'] == 'tool_call' for s in done['steps'])} tool calls")

    stats = call("GET", "/api/system/stats", token)
    if not stats["ledger"]["ok"]:
        raise SystemExit("FAIL: the ledger does not balance")
    print(f"ok  ledger balances ({stats['ledger']['transactions']} transactions)")
    print("PASS")


if __name__ == "__main__":
    main()
