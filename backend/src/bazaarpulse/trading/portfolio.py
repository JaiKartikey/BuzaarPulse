"""Portfolio read model: holdings marked to the latest quotes."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select

from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import Fill, Order, Position
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.trading.ledger import Ledger


async def open_account(db: Database, ledger: Ledger, settings: Settings, user_id: uuid.UUID) -> None:
    """Give a new user their paper capital. Idempotent."""
    async with db.transaction() as session:
        await ledger.deposit(session, user_id, settings.starting_cash, idempotency_key=f"deposit:signup:{user_id}")


async def portfolio(db: Database, store: MarketStore, ledger: Ledger, user_id: uuid.UUID) -> dict[str, Any]:
    async with db.transaction() as session:
        cash_account = await ledger.cash_account(session, user_id)
        cash = cash_account.balance
        deposits = await ledger.deposits(session, cash_account.id)
        reserved = Decimal(
            await session.scalar(
                select(func.coalesce(func.sum(Order.reserved_cash), 0)).where(
                    Order.user_id == user_id, Order.status == "open", Order.side == "buy"
                )
            )
            or 0
        )
        positions = (
            (await session.execute(select(Position).where(Position.user_id == user_id).order_by(Position.symbol)))
            .scalars()
            .all()
        )

    quotes = await store.quotes([p.symbol for p in positions if p.quantity > 0])
    rows: list[dict[str, Any]] = []
    market_value = unrealized = Decimal(0)
    realized = sum((p.realized_pnl for p in positions), Decimal(0))
    fees = sum((p.fees_paid for p in positions), Decimal(0))
    for p in positions:
        if p.quantity <= 0:
            continue
        quote = quotes.get(p.symbol)
        last = Decimal(str(quote["last"])) if quote else p.avg_cost
        value = last * p.quantity
        pnl = (last - p.avg_cost) * p.quantity
        market_value += value
        unrealized += pnl
        rows.append(
            {
                "symbol": p.symbol,
                "quantity": p.quantity,
                "avg_cost": float(p.avg_cost),
                "last": float(last),
                "market_value": round(float(value), 2),
                "unrealized_pnl": round(float(pnl), 2),
                "unrealized_pct": round(float((last / p.avg_cost - 1) * 100), 2) if p.avg_cost else 0.0,
                "day_change_pct": quote["change_pct"] if quote else 0.0,
            }
        )
    equity = cash + market_value
    for row in rows:
        row["weight_pct"] = round(row["market_value"] / float(equity) * 100, 1) if equity else 0.0
    return {
        "currency": ledger.currency,
        "cash": round(float(cash), 2),
        "reserved_cash": round(float(reserved), 2),
        "buying_power": round(float(cash - reserved), 2),
        "market_value": round(float(market_value), 2),
        "equity": round(float(equity), 2),
        "deposits": round(float(deposits), 2),
        "total_pnl": round(float(equity - deposits), 2),
        "total_pnl_pct": round(float((equity / deposits - 1) * 100), 2) if deposits else 0.0,
        "unrealized_pnl": round(float(unrealized), 2),
        "realized_pnl": round(float(realized), 2),
        "fees_paid": round(float(fees), 2),
        "positions": rows,
    }


async def recent_fills(db: Database, user_id: uuid.UUID, limit: int = 50) -> list[dict[str, Any]]:
    async with db.session() as session:
        fills = (
            await session.execute(
                select(Fill).where(Fill.user_id == user_id).order_by(Fill.created_at.desc()).limit(limit)
            )
        ).scalars()
        return [
            {
                "id": str(f.id),
                "order_id": str(f.order_id),
                "symbol": f.symbol,
                "side": f.side,
                "quantity": f.quantity,
                "price": float(f.price),
                "fee": float(f.fee),
                "ts": f.created_at.timestamp(),
            }
            for f in fills
        ]
