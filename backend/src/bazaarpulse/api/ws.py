"""WebSocket gateway: fans live data out to browsers.

One Redis subscription per API process feeds every socket on that process, so
ten thousand browsers cost Redis one subscriber per replica, not ten thousand.

The interesting problem is the slow client. A phone on a bad network cannot
keep up with a busy market, and an unbounded per-client queue would grow until
the server runs out of memory. So quotes are **conflated per client**: the
server keeps only the newest pending quote per symbol. A slow client simply
skips intermediate prices and always receives the latest one. Events that
must not be skipped (order updates) go through a small bounded queue instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections import deque
from typing import Any

import orjson
from fastapi import WebSocket, WebSocketDisconnect

from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.logging import get_logger
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.metrics import WS_CONNECTIONS, WS_MESSAGES

log = get_logger(__name__)
AUTH_TIMEOUT_SECONDS = 10
SEND_TIMEOUT_SECONDS = 5
MAX_PENDING_EVENTS = 200


class Client:
    def __init__(self, socket: WebSocket, user_id: uuid.UUID) -> None:
        self.socket = socket
        self.user_id = user_id
        self.symbols: set[str] | None = None  # None = everything
        self.quotes: dict[str, dict[str, Any]] = {}  # newest pending quote per symbol
        self.events: deque[dict[str, Any]] = deque(maxlen=MAX_PENDING_EVENTS)
        self.wake = asyncio.Event()

    def offer_quotes(self, quotes: list[dict[str, Any]]) -> None:
        for quote in quotes:
            if self.symbols is None or quote["symbol"] in self.symbols:
                self.quotes[quote["symbol"]] = quote  # overwrite: only the latest matters
        self.wake.set()

    def offer_event(self, event: dict[str, Any]) -> None:
        self.events.append(event)
        self.wake.set()

    async def pump(self) -> None:
        """Send whatever is pending, then sleep until there is more."""
        while True:
            await self.wake.wait()
            self.wake.clear()
            if self.quotes:
                batch, self.quotes = list(self.quotes.values()), {}
                await self._send({"type": "quotes", "quotes": batch})
            while self.events:
                await self._send(self.events.popleft())

    async def _send(self, message: dict[str, Any]) -> None:
        # A client that cannot take a frame in five seconds is disconnected rather than waited for.
        await asyncio.wait_for(self.socket.send_bytes(orjson.dumps(message)), SEND_TIMEOUT_SECONDS)
        WS_MESSAGES.labels(message.get("type", "other")).inc()


class Hub:
    def __init__(self, bus: RedisBus, store: MarketStore, settings: Settings) -> None:
        self.bus = bus
        self.store = store
        self.settings = settings
        self.clients: set[Client] = set()
        self.by_user: dict[uuid.UUID, set[Client]] = {}

    async def run(self) -> None:
        """The single subscription feeding every connected client of this process."""
        async for channel, data in self.bus.subscribe([topics.QUOTES, topics.PULSE], patterns=["user.*"]):
            if channel == topics.QUOTES:
                for client in self.clients:
                    client.offer_quotes(data["quotes"])
            elif channel == topics.PULSE:
                for client in self.clients:
                    client.offer_event(data)
            else:
                # user.<uuid>: private events go only to that user's sockets.
                with contextlib.suppress(ValueError):
                    for client in self.by_user.get(uuid.UUID(channel.removeprefix("user.")), ()):
                        client.offer_event(data)

    async def serve(self, socket: WebSocket, authenticate: Any) -> None:
        await socket.accept()
        try:
            # The token arrives as the first message, not in the URL: URLs end up in proxy and server logs.
            hello = orjson.loads(await asyncio.wait_for(socket.receive_text(), AUTH_TIMEOUT_SECONDS))
            user_id = authenticate(str(hello.get("token", ""))) if hello.get("op") == "auth" else None
        except (TimeoutError, orjson.JSONDecodeError, WebSocketDisconnect, KeyError):
            user_id = None
        if user_id is None:
            await socket.close(code=4401, reason="authentication required")
            return

        client = Client(socket, user_id)
        self.clients.add(client)
        self.by_user.setdefault(user_id, set()).add(client)
        WS_CONNECTIONS.inc()
        client.offer_event({"type": "ready"})
        client.offer_quotes(list((await self.store.quotes()).values()))  # a full snapshot to start from
        # Two loops per connection, one per direction. Whichever ends first (the client
        # left, or it stopped accepting data) takes the other down with it.
        loops = {asyncio.create_task(client.pump()), asyncio.create_task(self._receive(client))}
        try:
            await asyncio.wait(loops, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in loops:
                task.cancel()
            for task in loops:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            self.clients.discard(client)
            self.by_user.get(user_id, set()).discard(client)
            WS_CONNECTIONS.dec()
            with contextlib.suppress(Exception):
                await socket.close()

    @staticmethod
    async def _receive(client: Client) -> None:
        try:
            while True:
                message = orjson.loads(await client.socket.receive_text())
                if message.get("op") == "subscribe":
                    symbols = message.get("symbols")
                    client.symbols = None if symbols is None else {str(s).upper() for s in symbols[:200]}
                elif message.get("op") == "ping":
                    client.offer_event({"type": "pong"})
        except (WebSocketDisconnect, orjson.JSONDecodeError, RuntimeError):
            return
