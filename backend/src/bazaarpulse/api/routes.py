"""HTTP routes. Handlers stay thin: parse, authorise, call a service, shape the answer."""

from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Annotated, Any, Literal

import orjson
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from bazaarpulse import __version__
from bazaarpulse.agents.proposals import ProposalError, serialize_proposal
from bazaarpulse.agents.service import GROUP as AGENT_GROUP
from bazaarpulse.agents.service import serialize_run
from bazaarpulse.api.security import RateLimiter, hash_password, issue_token, read_token, verify_password
from bazaarpulse.bus import topics
from bazaarpulse.container import Container
from bazaarpulse.db.models import AgentRun, Order, User
from bazaarpulse.marketdata.indicators import summarize
from bazaarpulse.marketdata.processor import GROUP as PROCESSOR_GROUP
from bazaarpulse.trading.orders import OrderError, serialize_order
from bazaarpulse.trading.portfolio import open_account, portfolio, recent_fills
from bazaarpulse.trading.risk import OrderRequest

DEMO_EMAIL = "demo@bazaarpulse.dev"
router = APIRouter(prefix="/api")
ops = APIRouter()


# --- Dependencies -------------------------------------------------------------


def container(request: Request) -> Container:
    c: Container = request.app.state.container
    return c


C = Annotated[Container, Depends(container)]


async def current_user(request: Request, c: C, authorization: Annotated[str | None, Header()] = None) -> uuid.UUID:
    scheme, _, token = (authorization or "").partition(" ")
    user_id = read_token(c.settings, token) if scheme.lower() == "bearer" else None
    if user_id is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or invalid token", {"WWW-Authenticate": "Bearer"})
    request.state.user_id = user_id
    return user_id


UserId = Annotated[uuid.UUID, Depends(current_user)]


def limiter(c: Container) -> RateLimiter:
    return RateLimiter(c.redis, c.settings.redis_prefix)


async def _symbol(c: Container, raw: str) -> str:
    symbol = raw.upper()
    if symbol not in await c.store.symbols():
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown symbol {symbol}")
    return symbol


# --- Auth -----------------------------------------------------------------------


class Credentials(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class Registration(Credentials):
    display_name: str = Field(min_length=1, max_length=80)


def _session(c: Container, user: User) -> dict[str, Any]:
    return {
        "token": issue_token(c.settings, user.id),
        "user": {"id": str(user.id), "email": user.email, "display_name": user.display_name},
    }


async def create_user(c: Container, email: str, password: str, display_name: str) -> User:
    user = User(
        id=uuid.uuid4(), email=email.lower(), display_name=display_name, password_hash=await hash_password(password)
    )
    try:
        async with c.db.transaction() as session:
            session.add(user)
    except IntegrityError as e:
        raise HTTPException(status.HTTP_409_CONFLICT, "an account with this email already exists") from e
    await open_account(c.db, c.ledger, c.settings, user.id)
    return user


@router.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def register(body: Registration, c: C) -> dict[str, Any]:
    return _session(c, await create_user(c, body.email, body.password, body.display_name))


@router.post("/auth/login")
async def login(body: Credentials, request: Request, c: C) -> dict[str, Any]:
    # Throttled per address and per account: slows password guessing without locking the real owner out for long.
    client = request.client.host if request.client else "unknown"
    for key in (f"login.ip.{client}", f"login.email.{body.email.lower()}"):
        if not await limiter(c).allow(key, limit=10, window=60):
            raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "too many attempts; try again in a minute")
    async with c.db.session() as session:
        user = (await session.execute(select(User).where(User.email == body.email.lower()))).scalar_one_or_none()
    # Same error whether the email or the password was wrong: do not reveal which accounts exist.
    if user is None or not await verify_password(user.password_hash, body.password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "wrong email or password")
    return _session(c, user)


@router.post("/auth/demo")
async def demo_login(c: C) -> dict[str, Any]:
    """One-click entry to a shared demo account. Exists only outside production."""
    if c.settings.env == "prod" or not c.settings.seed_demo_user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "not available")
    async with c.db.session() as session:
        user = (await session.execute(select(User).where(User.email == DEMO_EMAIL))).scalar_one_or_none()
    if user is None:
        user = await create_user(c, DEMO_EMAIL, uuid.uuid4().hex, "Demo Trader")
    return _session(c, user)


@router.get("/me")
async def me(user_id: UserId, c: C) -> dict[str, Any]:
    async with c.db.session() as session:
        user = await session.get(User, user_id)
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "account no longer exists")
    return {"id": str(user.id), "email": user.email, "display_name": user.display_name}


# --- Market data ----------------------------------------------------------------

Interval = Literal["1m", "5m", "15m", "1h"]


@router.get("/instruments")
async def instruments(_: UserId, c: C) -> list[dict[str, Any]]:
    return await c.store.instruments()


@router.get("/quotes")
async def quotes(_: UserId, c: C, symbols: str | None = None) -> dict[str, Any]:
    wanted = [s.strip().upper() for s in symbols.split(",") if s.strip()][:100] if symbols else None
    return await c.store.quotes(wanted)


@router.get("/candles/{symbol}")
async def candles(
    symbol: str, _: UserId, c: C, interval: Interval = "1m", limit: Annotated[int, Query(ge=1, le=1000)] = 300
) -> list[dict[str, Any]]:
    return await c.store.candles(await _symbol(c, symbol), interval, limit)


@router.get("/indicators/{symbol}")
async def indicators(symbol: str, _: UserId, c: C, interval: Interval = "1m") -> dict[str, Any]:
    return summarize(await c.store.candles(await _symbol(c, symbol), interval, 200))


@router.get("/news")
async def news(
    _: UserId, c: C, symbol: str | None = None, q: str | None = None, limit: Annotated[int, Query(ge=1, le=100)] = 20
) -> list[dict[str, Any]]:
    return await c.store.news(symbol=symbol.upper() if symbol else None, query=q, limit=limit)


@router.get("/anomalies")
async def anomalies(_: UserId, c: C, limit: Annotated[int, Query(ge=1, le=200)] = 30) -> list[dict[str, Any]]:
    return await c.store.anomalies(limit)


@router.get("/market/overview")
async def market_overview(_: UserId, c: C) -> dict[str, Any]:
    return await c.store.overview()


# --- Trading --------------------------------------------------------------------


class OrderBody(BaseModel):
    symbol: str = Field(min_length=1, max_length=24)
    side: Literal["buy", "sell"]
    type: Literal["market", "limit", "stop"] = "market"
    quantity: int = Field(gt=0, le=10_000_000)
    limit_price: Decimal | None = Field(default=None, gt=0, max_digits=14, decimal_places=4)
    stop_price: Decimal | None = Field(default=None, gt=0, max_digits=14, decimal_places=4)

    def to_request(self) -> OrderRequest:
        return OrderRequest(
            symbol=self.symbol.upper(),
            side=self.side,
            type=self.type,
            quantity=self.quantity,
            limit_price=self.limit_price if self.type == "limit" else None,
            stop_price=self.stop_price if self.type == "stop" else None,
        )


@router.post("/orders", status_code=status.HTTP_201_CREATED)
async def place_order(
    body: OrderBody,
    user_id: UserId,
    c: C,
    idempotency_key: Annotated[str, Header(min_length=8, max_length=120)],
) -> dict[str, Any]:
    """Place an order. The Idempotency-Key header makes a retried request safe: same key, same order."""
    order = await c.orders.place(user_id, body.to_request(), idempotency_key=idempotency_key)
    return serialize_order(order)


@router.post("/orders/preview")
async def preview_order(body: OrderBody, user_id: UserId, c: C) -> dict[str, Any]:
    return (await c.orders.preview(user_id, body.to_request())).to_dict()


@router.get("/orders")
async def list_orders(
    user_id: UserId, c: C, status_filter: Annotated[str | None, Query(alias="status")] = None
) -> list[dict[str, Any]]:
    return [serialize_order(o) for o in await c.orders.list(user_id, status=status_filter)]


@router.delete("/orders/{order_id}")
async def cancel_order(order_id: uuid.UUID, user_id: UserId, c: C) -> dict[str, Any]:
    return serialize_order(await c.orders.cancel(user_id, order_id))


@router.get("/portfolio")
async def get_portfolio(user_id: UserId, c: C) -> dict[str, Any]:
    return await portfolio(c.db, c.store, c.ledger, user_id)


@router.get("/fills")
async def fills(user_id: UserId, c: C) -> list[dict[str, Any]]:
    return await recent_fills(c.db, user_id)


# --- Agents ---------------------------------------------------------------------


class RunBody(BaseModel):
    kind: Literal["copilot", "war_room"] = "copilot"
    message: str = Field(min_length=1, max_length=2000)
    symbol: str | None = None


@router.post("/agent/runs", status_code=status.HTTP_202_ACCEPTED)
async def start_run(body: RunBody, user_id: UserId, c: C) -> dict[str, Any]:
    """Queue a run and return immediately; follow it on the events stream."""
    if not await limiter(c).allow(f"runs.{user_id}", limit=12, window=60):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "too many agent requests; wait a moment")
    symbol = await _symbol(c, body.symbol) if body.symbol else None
    if body.kind == "war_room" and symbol is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "a war room needs a symbol")
    run = await c.agents.create_run(kind=body.kind, user_id=user_id, input={"message": body.message}, symbol=symbol)
    return serialize_run(run)


@router.get("/agent/runs")
async def list_runs(user_id: UserId, c: C) -> list[dict[str, Any]]:
    return [serialize_run(r) for r in await c.agents.list_runs(user_id)]


async def _run_or_404(c: Container, run_id: uuid.UUID, user_id: uuid.UUID) -> AgentRun:
    run = await c.agents.get_run(run_id, user_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return run


@router.get("/agent/runs/{run_id}")
async def get_run(run_id: uuid.UUID, user_id: UserId, c: C) -> dict[str, Any]:
    run = await _run_or_404(c, run_id, user_id)
    return {**serialize_run(run), "steps": await c.agents.steps(run_id)}


def _sse(event_id: str, data: dict[str, Any]) -> bytes:
    return b"id: " + event_id.encode() + b"\ndata: " + orjson.dumps(data) + b"\n\n"


@router.get("/agent/runs/{run_id}/events")
async def run_events(
    run_id: uuid.UUID, user_id: UserId, c: C, last_event_id: Annotated[str | None, Header()] = None
) -> StreamingResponse:
    """Server-Sent Events: the run's steps as they happen.

    Reads the run's Redis stream starting after ``Last-Event-ID``, so a client
    that reconnects misses nothing and sees nothing twice. Once the stream has
    expired (an hour after the run ends) it replays the copy stored in Postgres.
    """
    await _run_or_404(c, run_id, user_id)
    stream = topics.agent_run(run_id)

    async def events() -> AsyncIterator[bytes]:
        if not await c.redis.exists(c.bus.key(stream)):
            run = await c.agents.get_run(run_id, user_id)
            if run is not None and run.status in ("completed", "failed"):
                for step in await c.agents.steps(run_id):
                    yield _sse(f"db-{step['seq']}", step)
                return
        cursor = last_event_id if last_event_id and "-" in last_event_id and not last_event_id.startswith("db") else "0"
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            batch = await c.bus.read(stream, last_id=cursor, count=100, block_ms=15_000)
            if not batch:
                yield b": keep-alive\n\n"  # a comment line: keeps proxies from closing an idle connection
                continue
            for envelope in batch:
                cursor = envelope.id
                yield _sse(envelope.id, envelope.data)
                if envelope.data.get("type") == "done":
                    return

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}  # the second stops nginx buffering the stream
    return StreamingResponse(events(), media_type="text/event-stream", headers=headers)


@router.get("/proposals")
async def list_proposals(user_id: UserId, c: C) -> list[dict[str, Any]]:
    return await c.proposals.list(user_id)


@router.post("/proposals/{proposal_id}/{decision}")
async def decide_proposal(
    proposal_id: uuid.UUID, decision: Literal["approve", "reject"], user_id: UserId, c: C
) -> dict[str, Any]:
    """The human-in-the-loop gate. Only an authenticated user can turn an agent's plan into orders."""
    return serialize_proposal(await c.proposals.decide(user_id, proposal_id, approve=decision == "approve"))


# --- System ---------------------------------------------------------------------


class ShockBody(BaseModel):
    symbol: str
    pct: float = Field(ge=-8, le=8)


@router.post("/sim/shock", status_code=status.HTTP_202_ACCEPTED)
async def inject_shock(body: ShockBody, _: UserId, c: C) -> dict[str, Any]:
    """Demo control: make the simulated market move, to watch detection and the AI brief end to end."""
    if c.settings.market_provider != "simulator":
        raise HTTPException(status.HTTP_409_CONFLICT, "shocks can only be injected into the simulator")
    symbol = await _symbol(c, body.symbol)
    await c.bus.broadcast(topics.SIM_CONTROL, {"op": "shock", "symbol": symbol, "pct": body.pct})
    return {"symbol": symbol, "pct": body.pct}


@router.get("/system/stats")
async def system_stats(_: UserId, c: C, request: Request) -> dict[str, Any]:
    """Live numbers for the System page: throughput, latency, backlog, books, AI spend."""
    stats = await c.store.pipeline_stats()
    lags = {}
    for partition in range(c.settings.tick_partitions):
        lags[topics.ticks(partition)] = await c.bus.lag(topics.ticks(partition), PROCESSOR_GROUP)
    lags[topics.AGENT_JOBS] = await c.bus.lag(topics.AGENT_JOBS, AGENT_GROUP)
    async with c.db.session() as session:
        audit = await c.ledger.audit(session)
        open_orders = await session.scalar(select(func.count()).select_from(Order).where(Order.status == "open"))
        usage = (
            await session.execute(
                text(
                    "SELECT count(*) AS runs, coalesce(sum(input_tokens), 0) AS input_tokens,"
                    " coalesce(sum(output_tokens), 0) AS output_tokens, coalesce(sum(cost_usd), 0) AS cost_usd"
                    " FROM agent_runs WHERE created_at > now() - interval '24 hours'"
                )
            )
        ).one()
    hub = getattr(request.app.state, "hub", None)
    return {
        "version": __version__,
        "market_provider": c.settings.market_provider,
        "pipeline": stats,
        "stream_lag": lags,
        "websocket_clients": len(hub.clients) if hub else 0,
        "open_orders": open_orders or 0,
        "ledger": audit,
        "llm": {
            "provider": c.gateway.primary.name,
            "model": c.gateway.primary.model,
            "runs_24h": usage.runs,
            "input_tokens_24h": int(usage.input_tokens),
            "output_tokens_24h": int(usage.output_tokens),
            "cost_usd_24h": float(usage.cost_usd),
        },
    }


@ops.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness: the process is up and its event loop is turning. Checks nothing else, on purpose."""
    return {"status": "ok"}


@ops.get("/readyz")
async def readyz(c: C) -> Response:
    """Readiness: can this replica serve traffic right now? Fails if Postgres or Redis is unreachable."""
    checks: dict[str, str] = {}
    try:
        async with c.db.session() as session:
            await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"error: {type(e).__name__}"
    try:
        await c.redis.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {type(e).__name__}"
    ready = all(v == "ok" for v in checks.values())
    return Response(
        orjson.dumps({"status": "ready" if ready else "not ready", "checks": checks}),
        status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        media_type="application/json",
    )


@ops.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


__all__ = ["OrderError", "ProposalError", "ops", "router"]
