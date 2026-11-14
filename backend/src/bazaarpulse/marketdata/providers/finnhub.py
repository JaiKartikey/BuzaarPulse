"""Finnhub: real-time US trades over a WebSocket, plus company news over REST.

Protocol (https://finnhub.io/docs/api/websocket-trades): after connecting we
send one ``{"type": "subscribe", "symbol": ...}`` per symbol, and receive
``{"type": "trade", "data": [{"s": symbol, "p": price, "t": millis, "v": volume}]}``.

A network feed drops; the socket loop reconnects with capped exponential
backoff and jitter, so a blip never needs a human and a provider outage does
not turn into a reconnect storm.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import orjson
import websockets

from bazaarpulse.logging import get_logger
from bazaarpulse.marketdata.instruments import InstrumentSpec
from bazaarpulse.marketdata.types import Bar, Batch, NewsItem, Tick

log = get_logger(__name__)
WS_URL = "wss://ws.finnhub.io"
REST_URL = "https://finnhub.io/api/v1"
NEWS_POLL_SECONDS = 300


def parse_trades(raw: str | bytes, prev_close: dict[str, float]) -> list[Tick]:
    message = orjson.loads(raw)
    if message.get("type") != "trade":
        return []  # pings and subscription acks
    return [
        Tick(d["s"], float(d["p"]), max(1, int(d.get("v") or 0)), d["t"] / 1000, prev_close.get(d["s"]))
        for d in message.get("data", [])
        if d.get("p")
    ]


def parse_news(symbol: str, items: list[dict[str, Any]]) -> list[NewsItem]:
    return [
        NewsItem(
            headline=n["headline"],
            ts=float(n["datetime"]),
            symbol=symbol,
            summary=(n.get("summary") or "")[:600],
            source=n.get("source") or "Finnhub",
            url=n.get("url"),
        )
        for n in items
        if n.get("headline") and n.get("datetime")
    ]


class FinnhubProvider:
    name = "finnhub"

    def __init__(
        self, specs: tuple[InstrumentSpec, ...], api_key: str, *, http: httpx.AsyncClient | None = None
    ) -> None:
        self.specs = specs
        self._api_key = api_key
        self._http = http or httpx.AsyncClient(base_url=REST_URL, timeout=10, headers={"X-Finnhub-Token": api_key})
        self._closed = False

    async def backfill(self, minutes: int) -> list[Bar]:
        return []  # intraday candles are not part of Finnhub's free tier; the chart fills in live

    async def control(self, command: dict[str, Any]) -> None:
        return None

    async def aclose(self) -> None:
        self._closed = True
        await self._http.aclose()

    async def stream(self) -> AsyncIterator[Batch]:
        queue: asyncio.Queue[Batch] = asyncio.Queue(maxsize=1000)
        tasks = [asyncio.create_task(self._trades(queue)), asyncio.create_task(self._news(queue))]
        try:
            while not self._closed:
                yield await queue.get()
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _previous_closes(self) -> dict[str, float]:
        closes: dict[str, float] = {}
        for spec in self.specs:
            try:
                r = await self._http.get("/quote", params={"symbol": spec.symbol})
                r.raise_for_status()
                if pc := r.json().get("pc"):
                    closes[spec.symbol] = float(pc)
            except httpx.HTTPError as e:
                log.warning("finnhub.quote_failed", symbol=spec.symbol, error=str(e))
        return closes

    async def _trades(self, queue: asyncio.Queue[Batch]) -> None:
        prev_close = await self._previous_closes()
        backoff = 1.0
        while not self._closed:
            try:
                async with websockets.connect(f"{WS_URL}?token={self._api_key}", ping_interval=20) as ws:
                    for spec in self.specs:
                        await ws.send(orjson.dumps({"type": "subscribe", "symbol": spec.symbol}).decode())
                    log.info("finnhub.connected", symbols=len(self.specs))
                    backoff = 1.0
                    async for raw in ws:
                        if ticks := parse_trades(raw, prev_close):
                            await queue.put(Batch(ticks))
            except (OSError, websockets.WebSocketException) as e:
                log.warning("finnhub.disconnected", error=str(e), retry_in=backoff)
            await asyncio.sleep(backoff + random.uniform(0, backoff / 2))
            backoff = min(backoff * 2, 30.0)

    async def _news(self, queue: asyncio.Queue[Batch]) -> None:
        while not self._closed:
            today = datetime.now(UTC).date()
            for spec in self.specs:
                try:
                    r = await self._http.get(
                        "/company-news",
                        params={"symbol": spec.symbol, "from": str(today - timedelta(days=2)), "to": str(today)},
                    )
                    r.raise_for_status()
                    if items := parse_news(spec.symbol, r.json()[:15]):
                        await queue.put(Batch(news=items))
                except (httpx.HTTPError, ValueError) as e:
                    log.warning("finnhub.news_failed", symbol=spec.symbol, error=str(e))
                await asyncio.sleep(1.5)  # stay well inside the free tier's 60 calls a minute
            await asyncio.sleep(max(0.0, NEWS_POLL_SECONDS - 1.5 * len(self.specs)))
