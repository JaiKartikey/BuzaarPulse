"""The FastAPI application: wiring, lifecycle, middleware and error mapping."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from bazaarpulse import __version__
from bazaarpulse.agents.proposals import ProposalError
from bazaarpulse.api.routes import ops, router
from bazaarpulse.api.security import read_token
from bazaarpulse.api.ws import Hub
from bazaarpulse.config import Settings, get_settings
from bazaarpulse.container import WORKERS, Container, build_container, seed, supervise
from bazaarpulse.logging import configure_logging, get_logger
from bazaarpulse.metrics import HTTP_LATENCY, HTTP_REQUESTS
from bazaarpulse.trading.orders import OrderError

log = get_logger(__name__)


def create_app(settings: Settings | None = None, container: Container | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level, json=settings.log_json)
        c = container or await build_container(settings)
        app.state.container = c
        app.state.hub = Hub(c.bus, c.store, settings)
        await seed(c)

        tasks = [asyncio.create_task(app.state.hub.run(), name="ws-hub")]
        if settings.embedded_workers == "all":
            # Development mode: the whole system in one process. In production each of
            # these is its own deployment and this list is empty.
            tasks += [asyncio.create_task(supervise(name, c), name=name) for name in WORKERS]
        log.info("api.started", version=__version__, embedded=settings.embedded_workers, llm=c.gateway.primary.name)
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            if container is None:
                await c.aclose()

    app = FastAPI(
        title="BazaarPulse",
        version=__version__,
        lifespan=lifespan,
        description="Real-time market data, paper trading and an agentic research desk.",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID"],
    )

    @app.middleware("http")
    async def observe(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        # One id per request, attached to every log line it produces and returned to the caller.
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        structlog.contextvars.bind_contextvars(request_id=request_id)
        started = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            # Label by route template ("/api/candles/{symbol}"), never by raw path: one series per route.
            route = request.scope.get("route")
            template = getattr(route, "path", "unmatched")
            HTTP_REQUESTS.labels(request.method, template, str(status_code)).inc()
            HTTP_LATENCY.labels(request.method, template).observe(time.perf_counter() - started)
            structlog.contextvars.clear_contextvars()

    @app.exception_handler(OrderError)
    async def order_error(_: Request, exc: OrderError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @app.exception_handler(ProposalError)
    async def proposal_error(_: Request, exc: ProposalError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=409)

    app.include_router(router)
    app.include_router(ops)

    @app.websocket("/ws")
    async def websocket(socket: WebSocket) -> None:
        await app.state.hub.serve(socket, lambda token: read_token(settings, token))

    return app
