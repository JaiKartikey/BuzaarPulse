"""Order intake: validate, risk-check, reserve, record. Filling happens in the executor.

Two properties matter most here:

* **Idempotency.** Networks retry. Every order carries a client-chosen key,
  unique per user; a retry with the same key returns the original order
  instead of placing a second one.
* **No double spending.** Risk checks read the account and then write an order
  that reserves cash. Two concurrent orders could both read "Rs 1,00,000
  free" and both be accepted. Taking a row lock on the user's cash account
  first makes each user's orders run one at a time, while different users
  never wait on each other.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import Instrument, Order, OutboxEvent, Position
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.metrics import ORDERS
from bazaarpulse.trading.ledger import Ledger
from bazaarpulse.trading.risk import Limits, OrderRequest, RiskContext, RiskReport, evaluate


class OrderError(Exception):
    pass


def limits_from(settings: Settings) -> Limits:
    return Limits(
        fee_bps=settings.fee_bps,
        max_order_notional=settings.max_order_notional,
        max_position_pct=settings.max_position_pct,
        price_band_pct=settings.price_band_pct,
        max_quote_age_seconds=settings.max_quote_age_seconds,
        max_orders_per_minute=settings.max_orders_per_minute,
    )


def serialize_order(order: Order) -> dict[str, Any]:
    return {
        "id": str(order.id),
        "symbol": order.symbol,
        "side": order.side,
        "type": order.type,
        "quantity": order.quantity,
        "limit_price": None if order.limit_price is None else float(order.limit_price),
        "stop_price": None if order.stop_price is None else float(order.stop_price),
        "status": order.status,
        "filled_quantity": order.filled_quantity,
        "avg_fill_price": None if order.avg_fill_price is None else float(order.avg_fill_price),
        "reject_reason": order.reject_reason,
        "risk_report": order.risk_report,
        "source": order.source,
        "proposal_id": str(order.proposal_id) if order.proposal_id else None,
        "created_at": order.created_at.timestamp() if order.created_at else time.time(),
    }


def user_event(user_id: uuid.UUID, payload: dict[str, Any]) -> OutboxEvent:
    """An event for one user's live feed, written in the same transaction as the change it describes."""
    return OutboxEvent(channel=topics.user(user_id), payload=payload)


class OrderService:
    def __init__(self, db: Database, bus: RedisBus, store: MarketStore, ledger: Ledger, settings: Settings) -> None:
        self.db = db
        self.bus = bus
        self.store = store
        self.ledger = ledger
        self.settings = settings
        self.limits = limits_from(settings)

    async def place(
        self,
        user_id: uuid.UUID,
        request: OrderRequest,
        *,
        idempotency_key: str,
        source: str = "user",
        proposal_id: uuid.UUID | None = None,
    ) -> Order:
        async with self.db.transaction() as session:
            await self.ledger.cash_account(session, user_id, lock=True)
            # Looked up *after* taking the lock: a concurrent retry with the same key waits, then finds the order.
            existing = (
                await session.execute(
                    select(Order).where(Order.user_id == user_id, Order.idempotency_key == idempotency_key)
                )
            ).scalar_one_or_none()
            if existing is not None:
                return existing

            report = await self._assess(session, user_id, request)
            order = Order(
                id=uuid.uuid4(),
                user_id=user_id,
                symbol=request.symbol,
                side=request.side,
                type=request.type,
                quantity=request.quantity,
                limit_price=request.limit_price,
                stop_price=request.stop_price,
                status="open" if report.passed else "rejected",
                reserved_cash=report.required_cash if report.passed and request.side == "buy" else Decimal(0),
                reject_reason=None if report.passed else "; ".join(report.failures),
                risk_report=report.to_dict(),
                idempotency_key=idempotency_key,
                source=source,
                proposal_id=proposal_id,
                created_at=datetime.now(UTC),
            )
            session.add(order)
            await session.flush()
            session.add(user_event(user_id, {"type": "order", "order": serialize_order(order)}))

        ORDERS.labels(order.status, source).inc()
        if order.status == "open":
            # A nudge so the executor acts now instead of at the next tick. Losing it is harmless.
            await self.bus.broadcast(topics.ORDER_WAKE, {"symbol": order.symbol, "order_id": str(order.id)})
        return order

    async def preview(self, user_id: uuid.UUID, request: OrderRequest) -> RiskReport:
        """The same checks as ``place``, without placing anything."""
        async with self.db.transaction() as session:
            return await self._assess(session, user_id, request)

    async def cancel(self, user_id: uuid.UUID, order_id: uuid.UUID) -> Order:
        async with self.db.transaction() as session:
            # The status guard makes cancel-vs-fill a clean race: whichever commits first wins, the other is a no-op.
            order = (
                await session.execute(
                    update(Order)
                    .where(Order.id == order_id, Order.user_id == user_id, Order.status == "open")
                    .values(status="cancelled", reserved_cash=Decimal(0), updated_at=func.now())
                    .returning(Order)
                )
            ).scalar_one_or_none()
            if order is None:
                raise OrderError("order not found or no longer open")
            session.add(user_event(user_id, {"type": "order", "order": serialize_order(order)}))
        ORDERS.labels("cancelled", order.source).inc()
        return order

    async def list(self, user_id: uuid.UUID, *, status: str | None = None, limit: int = 100) -> list[Order]:
        stmt = select(Order).where(Order.user_id == user_id).order_by(Order.created_at.desc()).limit(limit)
        if status:
            stmt = stmt.where(Order.status == status)
        async with self.db.session() as session:
            return list((await session.execute(stmt)).scalars())

    # --- Risk context -------------------------------------------------------

    async def _assess(self, session: AsyncSession, user_id: uuid.UUID, request: OrderRequest) -> RiskReport:
        if request.side not in ("buy", "sell") or request.type not in ("market", "limit", "stop"):
            raise OrderError("side must be buy or sell; type must be market, limit or stop")
        instrument = await session.get(Instrument, request.symbol)
        if instrument is None:
            raise OrderError(f"unknown symbol {request.symbol}")

        cash_account = await self.ledger.cash_account(session, user_id)
        reserved = await session.scalar(
            select(func.coalesce(func.sum(Order.reserved_cash), 0)).where(
                Order.user_id == user_id, Order.status == "open", Order.side == "buy"
            )
        )
        open_sells = await session.scalar(
            select(func.coalesce(func.sum(Order.quantity), 0)).where(
                Order.user_id == user_id, Order.status == "open", Order.side == "sell", Order.symbol == request.symbol
            )
        )
        recent = await session.scalar(
            select(func.count())
            .select_from(Order)
            .where(Order.user_id == user_id, Order.created_at > datetime.now(UTC) - timedelta(seconds=60))
        )
        positions = (
            (await session.execute(select(Position).where(Position.user_id == user_id, Position.quantity > 0)))
            .scalars()
            .all()
        )
        quotes = await self.store.quotes(list({request.symbol, *(p.symbol for p in positions)}))

        quote = quotes.get(request.symbol)
        holdings_value = sum(
            (Decimal(str(quotes[p.symbol]["last"])) * p.quantity for p in positions if p.symbol in quotes), Decimal(0)
        )
        held = next((p.quantity for p in positions if p.symbol == request.symbol), 0)
        context = RiskContext(
            instrument_active=instrument.is_active,
            tick_size=instrument.tick_size,
            lot_size=instrument.lot_size,
            last_price=Decimal(str(quote["last"])) if quote else None,
            quote_age_seconds=time.time() - quote["ts"] if quote else None,
            cash=cash_account.balance,
            reserved_cash=Decimal(reserved or 0),
            position_quantity=held,
            open_sell_quantity=int(open_sells or 0),
            equity=cash_account.balance + holdings_value,
            orders_last_minute=int(recent or 0),
        )
        return evaluate(context, request, self.limits)
