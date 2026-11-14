"""Order intake and execution against real Postgres: money, positions, races and events."""

from __future__ import annotations

import asyncio
import random
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import func, select, text

from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import Fill, Order, Position
from bazaarpulse.trading.orders import OrderError
from bazaarpulse.trading.portfolio import portfolio, recent_fills
from tests.conftest import make_settings
from tests.trading_helpers import Rig, build_rig

START = Decimal("1000000")


@pytest.fixture
def rig(db: Database, bus: RedisBus, settings: Settings) -> Rig:
    return build_rig(db, bus, settings)


async def test_market_buy_then_sell_moves_money_and_shares_exactly(rig: Rig) -> None:
    user = await rig.user()
    await rig.price("TCS", 4000.00)
    order = await rig.buy(user, "TCS", 10)
    assert order.status == "open" and order.reserved_cash == Decimal("40412.00")  # notional + fee + 1% buffer

    fills = await rig.executor.on_price("TCS", 4000.00)
    assert len(fills) == 1
    # 2 bps slippage on 4000 = 0.80 -> fills at 4000.80; fee = 3 bps of 40,008 = 12.00
    assert fills[0].price == Decimal("4000.80") and fills[0].fee == Decimal("12.00")
    assert await rig.cash(user) == START - Decimal("40008.00") - Decimal("12.00")
    filled = await rig.order(order.id)
    assert filled.status == "filled" and filled.reserved_cash == 0 and filled.avg_fill_price == Decimal("4000.80")

    await rig.price("TCS", 4100.00)
    await rig.sell(user, "TCS", 4)
    (sale,) = await rig.executor.on_price("TCS", 4100.00)
    assert sale.price == Decimal("4099.15")  # 4100 - 2 bps = 4099.18, rounded down to the tick

    book = await portfolio(rig.db, rig.store, rig.ledger, user)
    (position,) = book["positions"]
    assert position["quantity"] == 6 and position["avg_cost"] == 4000.80
    assert book["realized_pnl"] == pytest.approx((4099.15 - 4000.80) * 4)
    assert book["unrealized_pnl"] == pytest.approx((4100 - 4000.80) * 6)
    assert book["equity"] == pytest.approx(book["cash"] + 6 * 4100)
    assert book["total_pnl"] == pytest.approx(book["equity"] - 1_000_000)
    assert [f["side"] for f in await recent_fills(rig.db, user)] == ["sell", "buy"]

    async with rig.db.session() as session:
        assert (await rig.ledger.audit(session))["ok"]


async def test_limit_order_waits_for_its_price_and_takes_a_better_one(rig: Rig) -> None:
    user = await rig.user()
    await rig.price("INFY", 1850.00)
    order = await rig.buy(user, "INFY", 20, limit=1840)
    assert order.reserved_cash == Decimal("36811.04")  # exact: a limit order's worst case is known

    assert await rig.executor.on_price("INFY", 1846.00) == []  # still above the limit
    (fill,) = await rig.executor.on_price("INFY", 1838.52)  # gapped through the limit: buyer gets the better price
    assert fill.price == Decimal("1838.55")
    assert await rig.executor.on_price("INFY", 1830.00) == []  # already filled, never twice


async def test_stop_loss_sells_when_the_price_falls_to_the_stop(rig: Rig) -> None:
    user = await rig.user()
    await rig.price("SBIN", 800.00)
    await rig.buy(user, "SBIN", 100)
    await rig.executor.on_price("SBIN", 800.00)

    stop = await rig.sell(user, "SBIN", 100, stop=780)
    assert await rig.executor.on_price("SBIN", 795.00) == []
    (fill,) = await rig.executor.on_price("SBIN", 779.00)
    assert fill.side == "sell" and fill.price == Decimal("778.80")
    assert (await rig.order(stop.id)).status == "filled"


async def test_rejected_orders_are_recorded_with_the_reason(rig: Rig) -> None:
    user = await rig.user()
    await rig.price("ITC", 470.00)
    short = await rig.sell(user, "ITC", 5)
    assert short.status == "rejected" and "short selling is not allowed" in (short.reject_reason or "")
    assert short.risk_report and short.risk_report["passed"] is False

    huge = await rig.buy(user, "ITC", 5000)  # Rs 23.5 lakh against Rs 10 lakh of cash
    assert huge.status == "rejected" and "buying_power" in (huge.reject_reason or "")
    assert await rig.executor.on_price("ITC", 470.00) == []
    with pytest.raises(OrderError, match="unknown symbol"):
        await rig.buy(user, "NOPE", 1)


async def test_same_idempotency_key_places_one_order_even_when_requests_race(rig: Rig) -> None:
    user = await rig.user()
    await rig.price("LT", 3600.00)
    placed = await asyncio.gather(*(rig.buy(user, "LT", 5, key="retry-me") for _ in range(8)))
    assert len({o.id for o in placed}) == 1
    async with rig.db.session() as session:
        assert await session.scalar(select(func.count()).select_from(Order)) == 1


async def test_concurrent_orders_cannot_spend_the_same_cash_twice(rig: Rig) -> None:
    user = await rig.user()
    symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "ITC", "LT"]
    for s in symbols:
        await rig.price(s, 1000.00)
    # Each order needs ~Rs 3.03 lakh of a Rs 10 lakh account: exactly three can be accepted.
    placed = await asyncio.gather(*(rig.buy(user, s, 300) for s in symbols))
    accepted = [o for o in placed if o.status == "open"]
    assert len(accepted) == 3
    assert sum(o.reserved_cash for o in accepted) <= START
    assert all("buying_power" in (o.reject_reason or "") for o in placed if o.status == "rejected")


async def test_cancel_releases_the_reservation_and_loses_to_a_fill(rig: Rig) -> None:
    user = await rig.user()
    await rig.price("MARUTI", 12500.00)
    order = await rig.buy(user, "MARUTI", 30, limit=12400)
    assert (await portfolio(rig.db, rig.store, rig.ledger, user))["buying_power"] < 630_000

    cancelled = await rig.orders.cancel(user, order.id)
    assert cancelled.status == "cancelled"
    assert (await portfolio(rig.db, rig.store, rig.ledger, user))["buying_power"] == 1_000_000
    assert await rig.executor.on_price("MARUTI", 12000.00) == []  # a cancelled order never fills

    with pytest.raises(OrderError):
        await rig.orders.cancel(user, order.id)
    with pytest.raises(OrderError):
        await rig.orders.cancel(await rig.user(), order.id)  # someone else's order


async def test_two_executors_fill_an_order_exactly_once(rig: Rig) -> None:
    user = await rig.user()
    await rig.price("BAJFINANCE", 7000.00)
    for _ in range(6):
        await rig.buy(user, "BAJFINANCE", 2)
    twin = build_rig(rig.db, rig.bus, rig.settings).executor

    results = await asyncio.gather(*(e.on_price("BAJFINANCE", 7000.00) for e in (rig.executor, twin) * 3))
    assert sum(len(r) for r in results) == 6
    async with rig.db.session() as session:
        assert await session.scalar(select(func.count()).select_from(Fill)) == 6
        assert (await session.get(Position, (user, "BAJFINANCE"))).quantity == 12  # type: ignore[union-attr]
        assert (await rig.ledger.audit(session))["ok"]


async def test_price_running_away_rejects_at_execution_without_moving_money(db: Database, bus: RedisBus) -> None:
    rig = build_rig(db, bus, make_settings(redis_prefix=bus.prefix, max_position_pct=Decimal(100)))
    user = await rig.user()
    await rig.price("TATAMOTORS", 950.00)
    order = await rig.buy(user, "TATAMOTORS", 520, limit=None)  # Rs 4.94 lakh
    other = await rig.buy(user, "TATAMOTORS", 500)  # Rs 4.75 lakh: together they fit, just
    assert order.status == other.status == "open"

    # The price doubles before either fills: only the first can still be afforded... and not even that.
    fills = await rig.executor.on_price("TATAMOTORS", 1990.00)
    statuses = sorted([(await rig.order(order.id)).status, (await rig.order(other.id)).status])
    assert statuses == ["filled", "rejected"] and len(fills) == 1
    rejected = await rig.order(order.id if (await rig.order(order.id)).status == "rejected" else other.id)
    assert "insufficient funds at execution price" in (rejected.reject_reason or "")
    assert await rig.cash(user) >= 0
    async with rig.db.session() as session:
        assert (await rig.ledger.audit(session))["ok"]


async def test_outbox_delivers_order_and_fill_events_to_the_owner(rig: Rig) -> None:
    user = await rig.user()
    received: list[dict[str, object]] = []

    async def listen() -> None:
        async for channel, data in rig.bus.subscribe([topics.user(user)]):
            received.append({"channel": channel, **data})

    listener = asyncio.create_task(listen())
    await asyncio.sleep(0.05)
    await rig.price("HDFCBANK", 1700.00)
    await rig.buy(user, "HDFCBANK", 10)
    await rig.executor.on_price("HDFCBANK", 1700.00)

    assert received == [], "nothing is published until the relay runs: events wait in the outbox"
    assert await rig.executor.relay_outbox() == 2
    assert await rig.executor.relay_outbox() == 0
    await asyncio.sleep(0.05)
    listener.cancel()
    assert [e["type"] for e in received] == ["order", "fill"]
    assert received[1]["order"]["status"] == "filled" and received[1]["price"] == 1700.35  # type: ignore[index]


async def test_random_trading_never_breaks_the_books(db: Database, bus: RedisBus) -> None:
    """A few hundred random orders from several users at moving prices: the invariants must survive all of it."""
    # The per-user order rate limit would (correctly) reject most of this burst, so lift it for the soak.
    rig = build_rig(db, bus, make_settings(redis_prefix=bus.prefix, max_orders_per_minute=100_000))
    rng = random.Random(42)
    users = [await rig.user() for _ in range(3)]
    symbols = ["RELIANCE", "TCS", "INFY", "SBIN", "ITC"]
    prices = {"RELIANCE": 2900.0, "TCS": 4100.0, "INFY": 1850.0, "SBIN": 820.0, "ITC": 470.0}
    for s, p in prices.items():
        await rig.price(s, p)

    for step in range(240):
        symbol = rng.choice(symbols)
        prices[symbol] = round(prices[symbol] * (1 + rng.uniform(-0.01, 0.01)) / 0.05) * 0.05
        await rig.price(symbol, prices[symbol])
        user, quantity = rng.choice(users), rng.randint(1, 120)
        kind = rng.choice(["market", "market", "limit"])
        limit = round(prices[symbol] * (1 + rng.uniform(-0.004, 0.004)) / 0.05) * 0.05 if kind == "limit" else None
        placer = rig.buy if rng.random() < 0.6 else rig.sell
        await placer(user, symbol, quantity, limit=limit)
        if step % 3 == 0:
            await rig.executor.on_price(symbol, prices[symbol])
    for s, p in prices.items():
        await rig.executor.on_price(s, p)

    async with rig.db.session() as session:
        audit = await rig.ledger.audit(session)
        assert audit["ok"], audit
        assert audit["transactions"] > 60, "the scenario should actually trade"
        # The positions read model must agree with the ledger, share for share.
        mismatches = (
            await session.execute(
                text(
                    """
                    SELECT p.user_id, p.symbol, p.quantity, a.balance
                      FROM positions p
                      JOIN ledger_accounts a ON a.user_id = p.user_id AND a.kind = 'position' AND a.asset = p.symbol
                     WHERE p.quantity <> a.balance
                    """
                )
            )
        ).all()
        assert mismatches == []
        negative = await session.scalar(
            text("SELECT count(*) FROM ledger_accounts WHERE user_id IS NOT NULL AND balance < 0")
        )
        assert negative == 0
        # Every filled order has exactly one fill, and no other order has any.
        orphans = await session.scalar(
            text(
                "SELECT count(*) FROM orders o LEFT JOIN fills f ON f.order_id = o.id"
                " WHERE (o.status = 'filled') <> (f.id IS NOT NULL)"
            )
        )
        assert orphans == 0
    for user in users:
        book = await portfolio(rig.db, rig.store, rig.ledger, user)
        assert book["cash"] >= 0 and book["buying_power"] <= book["cash"] + 0.01
        assert uuid.UUID(str(user))
