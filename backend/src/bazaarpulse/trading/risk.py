"""Pre-trade risk checks.

A pure function from "what the account looks like" plus "what is being asked"
to a list of named pass/fail checks. No I/O, so it is easy to test exhaustively
and to explain: the same report is stored on the order, shown in the UI and
handed to the AI agent when it drafts a trade. Human and agent orders go
through exactly this code; there is no second path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from bazaarpulse.trading.money import bps, cash, on_grid

# Extra cash held for a market buy, because the fill price is not known yet.
MARKET_BUFFER_BPS = Decimal(100)


@dataclass(frozen=True, slots=True)
class OrderRequest:
    symbol: str
    side: str  # buy | sell
    type: str  # market | limit | stop
    quantity: int
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None


@dataclass(frozen=True, slots=True)
class Limits:
    fee_bps: Decimal
    max_order_notional: Decimal
    max_position_pct: Decimal
    price_band_pct: Decimal
    max_quote_age_seconds: float
    max_orders_per_minute: int


@dataclass(frozen=True, slots=True)
class RiskContext:
    """Everything the checks need, gathered under the user's account lock."""

    instrument_active: bool
    tick_size: Decimal
    lot_size: int
    last_price: Decimal | None
    quote_age_seconds: float | None
    cash: Decimal
    reserved_cash: Decimal  # already promised to this user's other open buy orders
    position_quantity: int
    open_sell_quantity: int  # already promised to this user's other open sell orders
    equity: Decimal
    orders_last_minute: int


@dataclass(slots=True)
class Check:
    name: str
    passed: bool
    detail: str


@dataclass(slots=True)
class RiskReport:
    checks: list[Check] = field(default_factory=list)
    reference_price: Decimal | None = None
    notional: Decimal = Decimal(0)
    estimated_fee: Decimal = Decimal(0)
    required_cash: Decimal = Decimal(0)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failures(self) -> list[str]:
        return [f"{c.name}: {c.detail}" for c in self.checks if not c.passed]

    def add(self, name: str, passed: bool, detail: str) -> None:
        self.checks.append(Check(name, passed, detail))

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "reference_price": None if self.reference_price is None else str(self.reference_price),
            "notional": str(self.notional),
            "estimated_fee": str(self.estimated_fee),
            "required_cash": str(self.required_cash),
            "checks": [{"name": c.name, "passed": c.passed, "detail": c.detail} for c in self.checks],
        }


def evaluate(ctx: RiskContext, order: OrderRequest, limits: Limits) -> RiskReport:
    report = RiskReport()
    report.add("instrument", ctx.instrument_active, "tradable" if ctx.instrument_active else "instrument is not active")

    quantity_ok = order.quantity > 0 and order.quantity % ctx.lot_size == 0
    report.add(
        "quantity",
        quantity_ok,
        f"{order.quantity} shares" if quantity_ok else f"must be a positive multiple of {ctx.lot_size}",
    )

    rate_ok = ctx.orders_last_minute < limits.max_orders_per_minute
    report.add(
        "order_rate",
        rate_ok,
        f"{ctx.orders_last_minute} orders in the last minute (limit {limits.max_orders_per_minute})",
    )

    fresh = (
        ctx.last_price is not None
        and ctx.quote_age_seconds is not None
        and ctx.quote_age_seconds <= limits.max_quote_age_seconds
    )
    if ctx.last_price is None:
        report.add("market_data", False, "no price available for this symbol")
        return report
    # A market order fills at "whatever the price is"; refusing a stale price is refusing to trade blind.
    if order.type != "limit":
        report.add("market_data", fresh, "price is live" if fresh else "last price is stale; market is not updating")

    price = order.limit_price or order.stop_price
    if order.type == "market":
        reference = ctx.last_price
    elif price is None or price <= 0:
        report.add("price", False, f"a {order.type} order needs a positive price")
        return report
    else:
        reference = price if order.type == "limit" else ctx.last_price
        band = bps(ctx.last_price, limits.price_band_pct * 100)
        within = ctx.last_price - band <= price <= ctx.last_price + band
        grid = on_grid(price, ctx.tick_size)
        report.add("tick_size", grid, "on the tick grid" if grid else f"price must be a multiple of {ctx.tick_size}")
        report.add(
            "price_band",
            within,
            f"within {limits.price_band_pct}% of last price {ctx.last_price}"
            if within
            else f"more than {limits.price_band_pct}% away from last price {ctx.last_price}",
        )

    notional = cash(reference * order.quantity)
    fee = cash(bps(notional, limits.fee_bps))
    report.reference_price, report.notional, report.estimated_fee = reference, notional, fee
    report.add(
        "order_size",
        notional <= limits.max_order_notional,
        f"order value {notional} against a per-order limit of {limits.max_order_notional}",
    )

    if order.side == "buy":
        buffer = bps(notional, MARKET_BUFFER_BPS) if order.type != "limit" else Decimal(0)
        required = cash(notional + fee + buffer)
        available = ctx.cash - ctx.reserved_cash
        report.required_cash = required
        report.add("buying_power", required <= available, f"needs {required}, available {cash(available)}")

        position_after = (ctx.position_quantity + order.quantity) * reference
        cap = ctx.equity * limits.max_position_pct / 100
        report.add(
            "concentration",
            position_after <= cap,
            f"position would be {cash(position_after)}, limit is {limits.max_position_pct}% of equity ({cash(cap)})",
        )
    else:
        sellable = ctx.position_quantity - ctx.open_sell_quantity
        report.add(
            "holdings",
            order.quantity <= sellable,
            f"selling {order.quantity}, {sellable} available (short selling is not allowed)",
        )
    return report
