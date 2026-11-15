"""Load test: many live browsers and many traders at once.

    cd backend && uv run python ../scripts/loadtest.py --base http://localhost:8080 --sockets 300 --traders 40 --seconds 30

Two things are measured at the same time, because that is how the system is used:

* **Fan-out**: N WebSocket clients each receive the quote stream. Reports frames
  and quotes delivered per second, and how stale each quote was on arrival.
* **Trading**: M users each place market orders in a loop. Reports accepted
  orders per second and the latency of placing one (p50 / p95 / p99).

At the end it checks that the ledger still balances. Numbers depend on the
machine; the point is to compare runs and find where the system bends.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
import uuid

import httpx
import websockets


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * q))]


async def socket_client(url: str, token: str, until: float, stats: dict[str, list[float]]) -> None:
    try:
        async with websockets.connect(url, max_size=2**22, open_timeout=20) as ws:
            await ws.send(json.dumps({"op": "auth", "token": token}))
            while time.time() < until:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, until - time.time()))
                except TimeoutError:
                    break
                message = json.loads(raw)
                if message.get("type") == "quotes":
                    now = time.time()
                    stats["frames"].append(now)
                    stats["quotes"].append(len(message["quotes"]))
                    stats["staleness"].extend(now - q["ts"] for q in message["quotes"] if q.get("ts"))
    except Exception as e:  # count, don't crash the run
        stats["socket_errors"].append(1)
        stats.setdefault("socket_error_kinds", []).append(type(e).__name__)  # type: ignore[arg-type]


async def trader(client: httpx.AsyncClient, token: str, until: float, symbols: list[str], stats: dict[str, list[float]]) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    i = 0
    while time.time() < until:
        symbol = symbols[i % len(symbols)]
        side = "buy" if (i // len(symbols)) % 2 == 0 else "sell"
        i += 1
        started = time.perf_counter()
        try:
            r = await client.post(
                "/api/orders",
                json={"symbol": symbol, "side": side, "type": "market", "quantity": 1},
                headers={**headers, "Idempotency-Key": str(uuid.uuid4())},
            )
            stats["order_latency"].append(time.perf_counter() - started)
            if r.status_code == 201:
                stats["orders_" + r.json()["status"]].append(1)
            else:
                stats[f"http_{r.status_code}"].append(1)
        except httpx.HTTPError:
            stats["order_errors"].append(1)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost:8080")
    parser.add_argument("--sockets", type=int, default=200)
    parser.add_argument("--traders", type=int, default=30)
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()

    limits = httpx.Limits(max_connections=args.traders + 10)
    async with httpx.AsyncClient(base_url=args.base, timeout=30, limits=limits) as client:
        # Each trader is its own user: per-user order limits and row locks then behave as in real use.
        tokens = []
        for _ in range(args.traders):
            r = await client.post(
                "/api/auth/register",
                json={"email": f"load-{uuid.uuid4().hex[:12]}@example.com", "password": "load-test-pass", "display_name": "Load"},
            )
            r.raise_for_status()
            tokens.append(r.json()["token"])
        viewer = tokens[0]
        symbols = [i["symbol"] for i in (await client.get("/api/instruments", headers={"Authorization": f"Bearer {viewer}"})).json()][:6]

        stats: dict[str, list[float]] = {
            k: [] for k in ("frames", "quotes", "staleness", "socket_errors", "order_latency", "order_errors")
        }
        stats = __import__("collections").defaultdict(list, stats)
        ws_url = args.base.replace("http", "ws", 1) + "/ws"
        started = time.time()
        until = started + args.seconds
        print(f"running {args.sockets} sockets and {args.traders} traders for {args.seconds}s against {args.base} ...")
        await asyncio.gather(
            *(socket_client(ws_url, viewer, until, stats) for _ in range(args.sockets)),
            *(trader(client, t, until, symbols, stats) for t in tokens),
        )
        elapsed = time.time() - started

        audit = (await client.get("/api/system/stats", headers={"Authorization": f"Bearer {viewer}"})).json()

    accepted = len(stats["orders_open"]) + len(stats["orders_filled"])
    lat = [v * 1000 for v in stats["order_latency"]]
    stale = [v * 1000 for v in stats["staleness"] if v >= 0]
    print()
    print("Fan-out")
    print(f"  sockets connected            {args.sockets - len(stats['socket_errors'])} / {args.sockets}")
    print(f"  frames delivered / s         {len(stats['frames']) / elapsed:,.0f}")
    print(f"  quotes delivered / s         {sum(stats['quotes']) / elapsed:,.0f}")
    if stale:
        print(f"  quote age on arrival (ms)    p50 {pct(stale, .5):.1f}   p95 {pct(stale, .95):.1f}   p99 {pct(stale, .99):.1f}")
    print("Trading")
    print(f"  orders accepted / s          {accepted / elapsed:,.1f}")
    print(f"  rejected by risk engine      {len(stats['orders_rejected'])}")
    print(f"  HTTP 429 (rate limited)      {len(stats['http_429'])}")
    print(f"  errors                       {len(stats['order_errors']) + sum(len(v) for k, v in stats.items() if k.startswith('http_5'))}")
    if lat:
        print(f"  place-order latency (ms)     p50 {pct(lat, .5):.1f}   p95 {pct(lat, .95):.1f}   p99 {pct(lat, .99):.1f}   mean {statistics.mean(lat):.1f}")
    print("Integrity")
    print(f"  ledger balanced              {audit['ledger']['ok']}  ({audit['ledger']['transactions']:,} transactions)")
    print(f"  tick backlog after the run   {audit['stream_lag']}")


if __name__ == "__main__":
    asyncio.run(main())
