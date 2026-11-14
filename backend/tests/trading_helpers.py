"""A small trading rig for tests: services wired together, plus shortcuts for users and prices."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from decimal import Decimal

import orjson

from bazaarpulse.bus import RedisBus
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import Order, User
from bazaarpulse.marketdata.processor import quote_key
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.trading.executor import Executor
from bazaarpulse.trading.ledger import Ledger
from bazaarpulse.trading.orders import OrderService
from bazaarpulse.trading.portfolio import open_account
from bazaarpulse.trading.risk import OrderRequest


@dataclass
class Rig:
    db: Database
    bus: RedisBus
    settings: Settings
    ledger: Ledger
    store: MarketStore
    orders: OrderService
    executor: Executor

    async def user(self, *, funded: bool = True) -> uuid.UUID:
        user_id = uuid.uuid4()
        async with self.db.transaction() as session:
            session.add(User(id=user_id, email=f"{user_id.hex[:12]}@test.dev", display_name="Test", password_hash="x"))
        if funded:
            await open_account(self.db, self.ledger, self.settings, user_id)
        return user_id

    async def price(self, symbol: str, last: float, *, age: float = 0.0) -> None:
        """Publish a quote the way the stream processor would."""
        quote = {
            "symbol": symbol,
            "last": last,
            "prev_close": last,
            "change": 0.0,
            "change_pct": 0.0,
            "open": last,
            "high": last,
            "low": last,
            "volume": 1000,
            "ts": time.time() - age,
            "bar": None,
        }
        await self.bus.redis.set(self.bus.key(quote_key(symbol)), orjson.dumps(quote))

    async def buy(self, user_id: uuid.UUID, symbol: str, quantity: int, **kw: object) -> Order:
        return await self._place(user_id, symbol, "buy", quantity, **kw)

    async def sell(self, user_id: uuid.UUID, symbol: str, quantity: int, **kw: object) -> Order:
        return await self._place(user_id, symbol, "sell", quantity, **kw)

    async def _place(self, user_id: uuid.UUID, symbol: str, side: str, quantity: int, **kw: object) -> Order:
        limit, stop = kw.get("limit"), kw.get("stop")
        request = OrderRequest(
            symbol=symbol,
            side=side,
            type="limit" if limit is not None else "stop" if stop is not None else "market",
            quantity=quantity,
            limit_price=None if limit is None else Decimal(str(limit)),
            stop_price=None if stop is None else Decimal(str(stop)),
        )
        key = str(kw.get("key") or uuid.uuid4())
        return await self.orders.place(user_id, request, idempotency_key=key)

    async def cash(self, user_id: uuid.UUID) -> Decimal:
        async with self.db.transaction() as session:
            return (await self.ledger.cash_account(session, user_id)).balance

    async def order(self, order_id: uuid.UUID) -> Order:
        async with self.db.session() as session:
            order = await session.get(Order, order_id)
            assert order is not None
            return order


def build_rig(db: Database, bus: RedisBus, settings: Settings) -> Rig:
    ledger = Ledger(settings.currency)
    store = MarketStore(bus, db)
    return Rig(
        db=db,
        bus=bus,
        settings=settings,
        ledger=ledger,
        store=store,
        orders=OrderService(db, bus, store, ledger, settings),
        executor=Executor(db, bus, store, ledger, settings),
    )
