"""Executor: fills open orders against live prices, and relays the outbox.

For each price it sees, it asks Postgres for the open orders that price
satisfies and fills each one in a single database transaction: ledger
postings, the fill record, the position and the order status all commit
together or not at all.

``FOR UPDATE SKIP LOCKED`` is what makes running several executors safe. Each
one locks the orders it is about to fill; the others skip those rows instead
of waiting, so an order is filled by exactly one executor with no coordinator.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import Fill, Instrument, Order, OutboxEvent, Position
from bazaarpulse.logging import get_logger
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.marketdata.types import Tick
from bazaarpulse.metrics import FILLS, ORDER_TO_FILL, ORDERS, OUTBOX_PUBLISHED
from bazaarpulse.trading.ledger import InsufficientFunds, Ledger, Leg
from bazaarpulse.trading.money import bps, cash, snap
from bazaarpulse.trading.orders import serialize_order, user_event

log = get_logger(__name__)
GROUP = "executor"


def fill_price(order: Order, market: Decimal, tick: Decimal, slippage_bps: Decimal) -> Decimal:
    """The price an order fills at, given the market price that triggered it."""
    buying = order.side == "buy"
    if order.type == "limit":
        assert order.limit_price is not None
        # A limit is a worst acceptable price: if the market is better, the trader gets the market.
        return (
            min(order.limit_price, snap(market, tick, up=True))
            if buying
            else max(order.limit_price, snap(market, tick, up=False))
        )
    # Market and triggered stop orders cross the spread; slippage models that cost.
    slipped = market + bps(market, slippage_bps) if buying else market - bps(market, slippage_bps)
    return snap(slipped, tick, up=buying)


def _triggered(price: Decimal) -> Any:
    """SQL condition: the order can execute at this market price."""
    return or_(
        Order.type == "market",
        and_(Order.type == "limit", Order.side == "buy", Order.limit_price >= price),
        and_(Order.type == "limit", Order.side == "sell", Order.limit_price <= price),
        and_(Order.type == "stop", Order.side == "sell", Order.stop_price >= price),  # stop-loss
        and_(Order.type == "stop", Order.side == "buy", Order.stop_price <= price),  # breakout buy
    )


class Executor:
    def __init__(
        self,
        db: Database,
        bus: RedisBus,
        store: MarketStore,
        ledger: Ledger,
        settings: Settings,
        *,
        consumer: str = "executor-0",
    ) -> None:
        self.db = db
        self.bus = bus
        self.store = store
        self.ledger = ledger
        self.settings = settings
        self.consumer = consumer
        self.tick_streams = [topics.ticks(p) for p in range(settings.tick_partitions)]
        self._open_symbols: set[str] = set()
        self._refreshed_at = 0.0

    # --- Matching -----------------------------------------------------------

    async def on_price(self, symbol: str, price: float) -> list[Fill]:
        """Fill every open order on ``symbol`` that ``price`` satisfies."""
        market = Decimal(str(price))
        fills: list[Fill] = []
        async with self.db.transaction() as session:
            orders = (
                (
                    await session.execute(
                        select(Order)
                        .where(Order.symbol == symbol, Order.status == "open", _triggered(market))
                        .order_by(Order.created_at)
                        .limit(200)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            if not orders:
                return []
            instrument = await session.get(Instrument, symbol)
            assert instrument is not None
            for order in orders:
                fill = await self._fill(session, order, instrument, market)
                if fill is not None:
                    fills.append(fill)
        for fill in fills:
            FILLS.labels(fill.side).inc()
        return fills

    async def _fill(self, session: AsyncSession, order: Order, instrument: Instrument, market: Decimal) -> Fill | None:
        price = fill_price(order, market, instrument.tick_size, self.settings.slippage_bps)
        quantity = order.quantity
        notional = cash(price * quantity)
        fee = cash(bps(notional, self.settings.fee_bps))
        currency = self.ledger.currency

        user_cash = await self.ledger.cash_account(session, order.user_id)
        user_shares = await self.ledger.account(session, user_id=order.user_id, kind="position", asset=order.symbol)
        market_cash = await self.ledger.account(session, user_id=None, kind="market", asset=currency)
        market_shares = await self.ledger.account(session, user_id=None, kind="market", asset=order.symbol)
        fees = await self.ledger.account(session, user_id=None, kind="fees", asset=currency)

        sign = 1 if order.side == "buy" else -1
        legs = [
            Leg(user_cash, -sign * notional - fee),
            Leg(market_cash, sign * notional),
            Leg(fees, fee),
            Leg(user_shares, Decimal(sign * quantity)),
            Leg(market_shares, Decimal(-sign * quantity)),
        ]
        try:
            tx_id = await self.ledger.post(
                session,
                kind="trade",
                legs=legs,
                reference=f"order:{order.id}",
                idempotency_key=f"fill:{order.id}",
                meta={"symbol": order.symbol, "side": order.side, "quantity": quantity, "price": str(price)},
            )
        except InsufficientFunds:
            # The price ran away between acceptance and execution. The order dies; no money moved.
            order.status, order.reserved_cash = "rejected", Decimal(0)
            order.reject_reason = (
                f"insufficient {'funds' if order.side == 'buy' else 'shares'} at execution price {price}"
            )
            order.updated_at = datetime.now(UTC)
            session.add(user_event(order.user_id, {"type": "order", "order": serialize_order(order)}))
            ORDERS.labels("rejected", order.source).inc()
            return None

        fill = Fill(
            id=uuid.uuid4(),
            order_id=order.id,
            user_id=order.user_id,
            symbol=order.symbol,
            side=order.side,
            quantity=quantity,
            price=price,
            fee=fee,
            ledger_transaction_id=tx_id,
        )
        session.add(fill)
        await self._update_position(session, order, price, fee)
        order.status, order.filled_quantity, order.avg_fill_price = "filled", quantity, price
        order.reserved_cash, order.updated_at = Decimal(0), datetime.now(UTC)
        session.add(
            user_event(
                order.user_id,
                {"type": "fill", "order": serialize_order(order), "price": float(price), "fee": float(fee)},
            )
        )
        if order.type == "market" and order.created_at:
            ORDER_TO_FILL.observe(max(0.0, time.time() - order.created_at.timestamp()))
        return fill

    async def _update_position(self, session: AsyncSession, order: Order, price: Decimal, fee: Decimal) -> None:
        """Weighted-average cost: buys move the average, sells realise profit against it."""
        position = await session.get(Position, (order.user_id, order.symbol), with_for_update=True)
        if position is None:
            await session.execute(
                insert(Position)
                .values(user_id=order.user_id, symbol=order.symbol, quantity=0, avg_cost=0, realized_pnl=0, fees_paid=0)
                .on_conflict_do_nothing()
            )
            position = await session.get(Position, (order.user_id, order.symbol), with_for_update=True)
            assert position is not None
        if order.side == "buy":
            total = position.quantity + order.quantity
            position.avg_cost = ((position.avg_cost * position.quantity + price * order.quantity) / total).quantize(
                Decimal("0.0001")
            )
            position.quantity = total
        else:
            position.realized_pnl += (price - position.avg_cost) * order.quantity
            position.quantity -= order.quantity
            if position.quantity == 0:
                position.avg_cost = Decimal(0)
        position.fees_paid += fee
        position.updated_at = datetime.now(UTC)

    # --- Outbox relay -------------------------------------------------------

    async def relay_outbox(self, batch: int = 200) -> int:
        """Publish events whose transaction has committed. At-least-once: consumers treat events as snapshots."""
        async with self.db.transaction() as session:
            events = (
                (
                    await session.execute(
                        select(OutboxEvent)
                        .where(OutboxEvent.published_at.is_(None))
                        .order_by(OutboxEvent.id)
                        .limit(batch)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            for event in events:
                await self.bus.broadcast(event.channel, event.payload)
            if events:
                await session.execute(
                    update(OutboxEvent)
                    .where(OutboxEvent.id.in_([e.id for e in events]))
                    .values(published_at=func.now())
                )
        if events:
            OUTBOX_PUBLISHED.inc(len(events))
        return len(events)

    async def prune_outbox(self) -> None:
        async with self.db.transaction() as session:
            await session.execute(
                delete(OutboxEvent).where(OutboxEvent.published_at < datetime.now(UTC) - timedelta(hours=1))
            )

    # --- Loops --------------------------------------------------------------

    async def run(self) -> None:
        for stream in self.tick_streams:
            await self.bus.ensure_group(stream, GROUP, start_id="$")
        tasks = [
            asyncio.create_task(self._tick_loop(), name="executor.ticks"),
            asyncio.create_task(self._wake_loop(), name="executor.wake"),
            asyncio.create_task(self._outbox_loop(), name="executor.outbox"),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

    async def _refresh_open_symbols(self, *, force: bool = False) -> None:
        if not force and time.monotonic() - self._refreshed_at < 0.5:
            return
        async with self.db.session() as session:
            rows = await session.execute(select(Order.symbol).where(Order.status == "open").distinct())
            self._open_symbols = set(rows.scalars())
        self._refreshed_at = time.monotonic()

    async def _tick_loop(self) -> None:
        while True:
            batch = await self.bus.consume(self.tick_streams, GROUP, self.consumer, count=2000, block_ms=500)
            await self._refresh_open_symbols()
            acks: dict[str, list[str]] = {}
            for env in batch:
                acks.setdefault(env.stream, []).append(env.id)
                tick = Tick.from_wire(env.data)
                # Most ticks are for symbols nobody has an order on: skip them without touching the database.
                if tick.symbol in self._open_symbols:
                    try:
                        await self.on_price(tick.symbol, tick.price)
                    except Exception:
                        log.exception("executor.fill_failed", symbol=tick.symbol)
            for stream, ids in acks.items():
                await self.bus.ack(stream, GROUP, ids)

    async def _wake_loop(self) -> None:
        """A new order arrived: try it against the latest quote right away."""
        async for _, message in self.bus.subscribe([topics.ORDER_WAKE]):
            symbol = message.get("symbol")
            if not symbol:
                continue
            self._open_symbols.add(symbol)
            quote = await self.store.quote(symbol)
            if quote and time.time() - quote["ts"] <= self.settings.max_quote_age_seconds:
                try:
                    await self.on_price(symbol, quote["last"])
                except Exception:
                    log.exception("executor.wake_fill_failed", symbol=symbol)

    async def _outbox_loop(self) -> None:
        last_prune = time.monotonic()
        while True:
            try:
                # Drain fast while there is a backlog, idle gently otherwise.
                if await self.relay_outbox() == 0:
                    await asyncio.sleep(0.1)
                if time.monotonic() - last_prune > 600:
                    await self.prune_outbox()
                    last_prune = time.monotonic()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("executor.outbox_failed")
                await asyncio.sleep(1)
