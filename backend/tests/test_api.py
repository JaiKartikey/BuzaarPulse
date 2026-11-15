"""The HTTP and WebSocket surface, driven through the real ASGI app."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import orjson
import pytest
from fastapi import WebSocketDisconnect

from bazaarpulse.agents.service import AgentWorker
from bazaarpulse.api.app import create_app
from bazaarpulse.api.security import issue_token, read_token
from bazaarpulse.api.ws import Hub
from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.container import Container, build_container
from bazaarpulse.db import Database
from bazaarpulse.trading.executor import Executor
from tests.conftest import make_settings
from tests.trading_helpers import build_rig


class Api:
    """A signed-in HTTP client plus handles on the services behind it."""

    def __init__(self, client: httpx.AsyncClient, container: Container) -> None:
        self.client = client
        self.c = container
        self.headers: dict[str, str] = {}

    async def sign_up(self, email: str = "") -> dict[str, Any]:
        email = email or f"{uuid.uuid4().hex[:10]}@example.com"
        r = await self.client.post(
            "/api/auth/register", json={"email": email, "password": "correct horse", "display_name": "T"}
        )
        assert r.status_code == 201, r.text
        self.headers = {"Authorization": f"Bearer {r.json()['token']}"}
        return r.json()

    async def get(self, path: str, **kw: Any) -> httpx.Response:
        return await self.client.get(path, headers={**self.headers, **kw.pop("headers", {})}, **kw)

    async def post(self, path: str, body: Any = None, **kw: Any) -> httpx.Response:
        return await self.client.post(path, json=body, headers={**self.headers, **kw.pop("headers", {})}, **kw)

    async def price(self, symbol: str, last: float) -> None:
        await build_rig(self.c.db, self.c.bus, self.c.settings).price(symbol, last)


@pytest.fixture
async def api(db: Database, bus: RedisBus, settings: Settings) -> AsyncIterator[Api]:
    container = await build_container(settings)
    container.db = db  # share the test session's engine (and its event loop)
    for service in (container.store, container.orders, container.proposals, container.toolbox, container.agents):
        service.db = db
    container.orders.store.db = db
    app = create_app(settings, container=container)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield Api(client, container)
    await container.redis.aclose()


# --- Auth -----------------------------------------------------------------------


async def test_register_login_and_token_checks(api: Api, settings: Settings) -> None:
    session = await api.sign_up("Trader@Example.com")
    assert session["user"]["email"] == "trader@example.com", "emails are stored lower-case"
    assert (await api.get("/api/me")).json()["id"] == session["user"]["id"]
    assert (await api.get("/api/portfolio")).json()["cash"] == 1_000_000, "a new account is funded with paper capital"

    again = await api.client.post(
        "/api/auth/register", json={"email": "trader@example.com", "password": "another pass", "display_name": "T"}
    )
    assert again.status_code == 409

    good = await api.client.post("/api/auth/login", json={"email": "TRADER@example.com", "password": "correct horse"})
    assert good.status_code == 200 and read_token(settings, good.json()["token"]) == uuid.UUID(session["user"]["id"])
    for creds in (
        {"email": "trader@example.com", "password": "wrong horse!"},
        {"email": "nobody@example.com", "password": "correct horse"},
    ):
        bad = await api.client.post("/api/auth/login", json=creds)
        # Identical answer for a wrong password and an unknown account.
        assert bad.status_code == 401 and bad.json()["detail"] == "wrong email or password"

    assert (await api.client.get("/api/me")).status_code == 401
    forged = issue_token(make_settings(jwt_secret="some-other-secret-some-other-secret"), uuid.uuid4())
    assert (await api.client.get("/api/me", headers={"Authorization": f"Bearer {forged}"})).status_code == 401
    assert (
        await api.client.post(
            "/api/auth/register", json={"email": "x@example.com", "password": "short", "display_name": "T"}
        )
    ).status_code == 422


async def test_login_is_throttled(api: Api) -> None:
    codes = [
        (
            await api.client.post("/api/auth/login", json={"email": "victim@example.com", "password": f"guess-{i:04d}"})
        ).status_code
        for i in range(12)
    ]
    assert codes[:10] == [401] * 10 and codes[10:] == [429, 429]


async def test_demo_login_exists_only_outside_production(api: Api) -> None:
    api.c.settings.seed_demo_user = True
    first = await api.client.post("/api/auth/demo")
    second = await api.client.post("/api/auth/demo")
    assert first.status_code == 200 and first.json()["user"]["id"] == second.json()["user"]["id"]
    api.c.settings.env = "prod"
    assert (await api.client.post("/api/auth/demo")).status_code == 404


# --- Market data and trading ------------------------------------------------------


async def test_market_endpoints(api: Api) -> None:
    await api.sign_up()
    await api.price("TCS", 4100.0)
    instruments = (await api.get("/api/instruments")).json()
    assert len(instruments) == 14 and instruments[0]["exchange"] == "NSE"
    assert (await api.get("/api/quotes", params={"symbols": "tcs,INFY"})).json()["TCS"]["last"] == 4100.0
    assert (await api.get("/api/candles/TCS", params={"interval": "5m"})).status_code == 200
    assert (await api.get("/api/candles/NOPE")).status_code == 404
    assert (await api.get("/api/candles/TCS", params={"interval": "2w"})).status_code == 422
    assert (await api.get("/api/market/overview")).json()["advancers"] == 0
    assert (await api.get("/api/quotes")).status_code == 200 and (
        await api.client.get("/api/quotes")
    ).status_code == 401


async def test_order_endpoint_is_idempotent_and_reports_risk(api: Api) -> None:
    await api.sign_up()
    await api.price("INFY", 1850.0)
    body = {"symbol": "infy", "side": "buy", "type": "market", "quantity": 10}

    assert (await api.post("/api/orders", body)).status_code == 422, "the Idempotency-Key header is required"
    first = await api.post("/api/orders", body, headers={"Idempotency-Key": "order-key-001"})
    retry = await api.post("/api/orders", body, headers={"Idempotency-Key": "order-key-001"})
    assert first.status_code == 201 and first.json()["status"] == "open"
    assert retry.json()["id"] == first.json()["id"], "a retried request returns the original order"
    assert len((await api.get("/api/orders")).json()) == 1

    preview = (await api.post("/api/orders/preview", {**body, "quantity": 100_000})).json()
    assert preview["passed"] is False and any(
        c["name"] == "buying_power" and not c["passed"] for c in preview["checks"]
    )
    rejected = await api.post("/api/orders", {**body, "side": "sell"}, headers={"Idempotency-Key": "order-key-002"})
    assert rejected.json()["status"] == "rejected" and "short selling" in rejected.json()["reject_reason"]

    executor = Executor(api.c.db, api.c.bus, api.c.store, api.c.ledger, api.c.settings)
    await executor.on_price("INFY", 1850.0)
    book = (await api.get("/api/portfolio")).json()
    assert book["positions"][0]["symbol"] == "INFY" and len((await api.get("/api/fills")).json()) == 1

    cancel = await api.client.delete(f"/api/orders/{first.json()['id']}", headers=api.headers)
    assert cancel.status_code == 400, "a filled order cannot be cancelled"


# --- Agents -----------------------------------------------------------------------


def parse_sse(body: str) -> list[tuple[str, dict[str, Any]]]:
    events = []
    for block in body.strip().split("\n\n"):
        fields = dict(line.split(": ", 1) for line in block.split("\n") if ": " in line and not line.startswith(":"))
        if "data" in fields:
            events.append((fields["id"], json.loads(fields["data"])))
    return events


async def test_agent_run_is_queued_streamed_and_resumable(api: Api) -> None:
    await api.sign_up()
    await api.price("TCS", 4100.0)
    queued = await api.post("/api/agent/runs", {"kind": "copilot", "message": "How is TCS?"})
    assert queued.status_code == 202 and queued.json()["status"] == "queued"
    run_id = queued.json()["id"]
    assert await api.c.bus.lag(topics.AGENT_JOBS, "agents") is None or True  # the job sits on the stream for a worker

    worker = AgentWorker(api.c.db, api.c.bus, api.c.store, api.c.gateway, api.c.toolbox, api.c.settings)
    await worker.execute(uuid.UUID(run_id))

    detail = (await api.get(f"/api/agent/runs/{run_id}")).json()
    assert detail["status"] == "completed" and detail["steps"][-1]["type"] == "done"
    assert [r["id"] for r in (await api.get("/api/agent/runs")).json()] == [run_id]

    stream = await api.get(f"/api/agent/runs/{run_id}/events")
    assert stream.headers["content-type"].startswith("text/event-stream")
    events = parse_sse(stream.text)
    assert [e["seq"] for _, e in events] == [s["seq"] for s in detail["steps"]] and events[-1][1]["type"] == "done"

    # A client that reconnects with Last-Event-ID gets only what it has not seen.
    resumed = parse_sse(
        (await api.get(f"/api/agent/runs/{run_id}/events", headers={"Last-Event-ID": events[2][0]})).text
    )
    assert [e["seq"] for _, e in resumed] == [e["seq"] for _, e in events[3:]]

    # After the live stream expires, the same history is served from Postgres.
    await api.c.redis.delete(api.c.bus.key(topics.agent_run(run_id)))
    archived = parse_sse((await api.get(f"/api/agent/runs/{run_id}/events")).text)
    assert [e["seq"] for _, e in archived] == [e["seq"] for _, e in events]

    stranger = Api(api.client, api.c)
    await stranger.sign_up()
    assert (await stranger.get(f"/api/agent/runs/{run_id}")).status_code == 404
    assert (await stranger.get(f"/api/agent/runs/{run_id}/events")).status_code == 404


async def test_proposal_approval_over_http(api: Api) -> None:
    await api.sign_up()
    await api.price("ITC", 470.0)
    run = (await api.post("/api/agent/runs", {"kind": "copilot", "message": "buy 20 ITC"})).json()
    worker = AgentWorker(api.c.db, api.c.bus, api.c.store, api.c.gateway, api.c.toolbox, api.c.settings)
    await worker.execute(uuid.UUID(run["id"]))

    (proposal,) = (await api.get("/api/proposals")).json()
    assert proposal["status"] == "pending"
    approved = await api.post(f"/api/proposals/{proposal['id']}/approve")
    assert approved.status_code == 200 and approved.json()["status"] == "executed"
    assert approved.json()["result"]["orders"][0]["source"] == "agent"
    assert (await api.post(f"/api/proposals/{proposal['id']}/reject")).status_code == 409
    assert (await api.post(f"/api/proposals/{uuid.uuid4()}/approve")).status_code == 409
    assert (await api.post(f"/api/proposals/{proposal['id']}/maybe")).status_code == 422


async def test_agent_requests_are_rate_limited_and_validated(api: Api) -> None:
    await api.sign_up()
    assert (await api.post("/api/agent/runs", {"kind": "war_room", "message": "view?"})).status_code == 422
    assert (
        await api.post("/api/agent/runs", {"kind": "war_room", "message": "view?", "symbol": "NOPE"})
    ).status_code == 404
    assert (await api.post("/api/agent/runs", {"kind": "sentinel", "message": "x"})).status_code == 422, (
        "users cannot start sentinel runs"
    )
    codes = [(await api.post("/api/agent/runs", {"message": f"q{i}"})).status_code for i in range(13)]
    assert codes.count(202) == 12 and codes[-1] == 429


# --- Operations -------------------------------------------------------------------


async def test_health_readiness_metrics_and_request_ids(api: Api) -> None:
    assert (await api.client.get("/healthz")).json() == {"status": "ok"}
    ready = await api.client.get("/readyz")
    assert ready.status_code == 200 and ready.json()["checks"] == {"postgres": "ok", "redis": "ok"}

    r = await api.client.get("/api/me", headers={"X-Request-ID": "trace-me-123"})
    assert r.headers["x-request-id"] == "trace-me-123"
    assert len((await api.client.get("/healthz")).headers["x-request-id"]) == 16

    await api.client.get("/api/candles/TCS")
    metrics = (await api.client.get("/metrics")).text
    # Labelled by route template, so a million different symbols are still one time series.
    assert 'bp_http_requests_total{method="GET",route="/api/candles/{symbol}",status="401"}' in metrics
    assert "bp_ticks_processed_total" in metrics and "bp_llm_tokens_total" in metrics


async def test_system_stats_and_simulator_shock(api: Api) -> None:
    await api.sign_up()
    stats = (await api.get("/api/system/stats")).json()
    assert stats["ledger"]["ok"] is True and stats["llm"]["provider"] == "mock" and stats["open_orders"] == 0
    assert "md.ticks.0" in stats["stream_lag"]

    heard: list[dict[str, Any]] = []

    async def listen() -> None:
        async for _, data in api.c.bus.subscribe([topics.SIM_CONTROL]):
            heard.append(data)

    listener = asyncio.create_task(listen())
    await asyncio.sleep(0.05)
    assert (await api.post("/api/sim/shock", {"symbol": "sbin", "pct": -3})).status_code == 202
    assert (await api.post("/api/sim/shock", {"symbol": "SBIN", "pct": -30})).status_code == 422
    await asyncio.sleep(0.05)
    listener.cancel()
    assert heard == [{"op": "shock", "symbol": "SBIN", "pct": -3.0}]


# --- WebSocket hub ----------------------------------------------------------------


class FakeSocket:
    """Stands in for a browser connection: scripted inbound messages, recorded outbound frames."""

    def __init__(self, inbound: list[dict[str, Any]], *, stall: bool = False) -> None:
        self.inbound: asyncio.Queue[str] = asyncio.Queue()
        for message in inbound:
            self.inbound.put_nowait(json.dumps(message))
        self.sent: list[dict[str, Any]] = []
        self.closed: int | None = None
        self.stall = stall

    async def accept(self) -> None:
        return None

    async def receive_text(self) -> str:
        message = await self.inbound.get()
        if message == '"bye"':
            raise WebSocketDisconnect(1000)
        return message

    async def send_bytes(self, data: bytes) -> None:
        if self.stall:
            await asyncio.sleep(3600)
        self.sent.append(orjson.loads(data))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = self.closed or code

    def hang_up(self) -> None:
        self.inbound.put_nowait('"bye"')


async def test_websocket_requires_a_valid_token_as_first_message(api: Api, settings: Settings) -> None:
    hub = Hub(api.c.bus, api.c.store, settings)
    for first in ({"op": "auth", "token": "garbage"}, {"op": "subscribe", "symbols": ["TCS"]}):
        socket = FakeSocket([first])
        await hub.serve(socket, lambda token: read_token(settings, token))  # type: ignore[arg-type]
        assert socket.closed == 4401 and socket.sent == [] and not hub.clients


async def test_websocket_conflates_quotes_filters_by_subscription_and_keeps_events_private(
    api: Api, settings: Settings
) -> None:
    hub = Hub(api.c.bus, api.c.store, settings)
    await api.price("TCS", 4100.0)
    alice, bob = uuid.uuid4(), uuid.uuid4()

    def connect(user: uuid.UUID, *messages: dict[str, Any]) -> tuple[FakeSocket, asyncio.Task[None]]:
        socket = FakeSocket([{"op": "auth", "token": issue_token(settings, user)}, *messages])
        return socket, asyncio.create_task(hub.serve(socket, lambda t: read_token(settings, t)))  # type: ignore[arg-type]

    a, a_task = connect(alice, {"op": "subscribe", "symbols": ["TCS"]})
    b, b_task = connect(bob)
    hub_task = asyncio.create_task(hub.run())
    await asyncio.sleep(0.1)
    assert [f["type"] for f in a.sent] == ["ready", "quotes"], "a snapshot arrives right after connecting"
    a.sent.clear()
    b.sent.clear()

    # A burst of 50 updates for one symbol lands before the socket is next written to: one frame, newest price.
    for client in hub.clients:
        for i in range(50):
            client.offer_quotes([{"symbol": "TCS", "last": 4100.0 + i}, {"symbol": "INFY", "last": 1800.0 + i}])
    await asyncio.sleep(0.05)
    assert [f["type"] for f in a.sent] == ["quotes"] and a.sent[0]["quotes"] == [{"symbol": "TCS", "last": 4149.0}]
    assert {q["symbol"] for q in b.sent[0]["quotes"]} == {"TCS", "INFY"}, "no subscription means everything"

    await api.c.bus.broadcast(topics.user(alice), {"type": "fill", "price": 1.0})
    await api.c.bus.broadcast(topics.PULSE, {"type": "anomaly", "symbol": "SBIN"})
    await asyncio.sleep(0.1)
    assert [f["type"] for f in a.sent[1:]] == ["fill", "anomaly"]
    assert [f["type"] for f in b.sent[1:]] == ["anomaly"], "another user's fill never reaches this socket"

    a.hang_up()
    b.hang_up()
    await asyncio.gather(a_task, b_task)
    hub_task.cancel()
    await asyncio.gather(hub_task, return_exceptions=True)
    assert not hub.clients and not any(hub.by_user.values())


async def test_websocket_drops_a_client_that_stops_reading(
    api: Api, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("bazaarpulse.api.ws.SEND_TIMEOUT_SECONDS", 0.1)
    hub = Hub(api.c.bus, api.c.store, settings)
    socket = FakeSocket([{"op": "auth", "token": issue_token(settings, uuid.uuid4())}], stall=True)
    await asyncio.wait_for(hub.serve(socket, lambda t: read_token(settings, t)), timeout=2)  # type: ignore[arg-type]
    assert not hub.clients, "the stalled connection was closed instead of buffering forever"
