"""Risk checks and fill pricing: example cases plus properties that must hold for any input."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

from hypothesis import given
from hypothesis import strategies as st

from bazaarpulse.trading.executor import fill_price
from bazaarpulse.trading.money import on_grid, snap
from bazaarpulse.trading.risk import Limits, OrderRequest, RiskContext, evaluate

TICK = Decimal("0.05")
LIMITS = Limits(
    fee_bps=Decimal(3),
    max_order_notional=Decimal(500_000),
    max_position_pct=Decimal(40),
    price_band_pct=Decimal(10),
    max_quote_age_seconds=20,
    max_orders_per_minute=30,
)
CTX = RiskContext(
    instrument_active=True,
    tick_size=TICK,
    lot_size=1,
    last_price=Decimal("1000"),
    quote_age_seconds=1.0,
    cash=Decimal(1_000_000),
    reserved_cash=Decimal(0),
    position_quantity=0,
    open_sell_quantity=0,
    equity=Decimal(1_000_000),
    orders_last_minute=0,
)


def failed(ctx: RiskContext, order: OrderRequest) -> set[str]:
    return {c.name for c in evaluate(ctx, order, LIMITS).checks if not c.passed}


def test_a_sensible_market_buy_passes_and_reserves_cash_with_a_buffer() -> None:
    report = evaluate(CTX, OrderRequest("TCS", "buy", "market", 100), LIMITS)
    assert report.passed
    assert report.notional == Decimal("100000.00") and report.estimated_fee == Decimal("30.00")
    # notional + fee + 1% buffer, because a market order's price is not known until it fills
    assert report.required_cash == Decimal("101030.00")


def test_each_limit_trips_its_own_check() -> None:
    buy = OrderRequest("TCS", "buy", "market", 100)
    assert failed(replace(CTX, cash=Decimal(50_000)), buy) == {"buying_power"}
    assert failed(replace(CTX, reserved_cash=Decimal(950_000)), buy) == {"buying_power"}
    assert failed(CTX, OrderRequest("TCS", "buy", "market", 450)) == {"concentration"}  # 45% of equity
    assert failed(replace(CTX, equity=Decimal(5_000_000), cash=Decimal(5_000_000)), replace(buy, quantity=600)) == {
        "order_size"
    }
    assert failed(replace(CTX, quote_age_seconds=60), buy) == {"market_data"}
    assert failed(replace(CTX, last_price=None), buy) == {"market_data"}
    assert failed(replace(CTX, orders_last_minute=30), buy) == {"order_rate"}
    assert failed(replace(CTX, instrument_active=False), buy) == {"instrument"}
    assert failed(CTX, replace(buy, quantity=0)) >= {"quantity"}
    assert failed(replace(CTX, lot_size=25), buy) == set() and failed(
        replace(CTX, lot_size=25), replace(buy, quantity=30)
    ) == {"quantity"}


def test_limit_price_must_be_on_the_grid_and_inside_the_band() -> None:
    def limit(price: str) -> OrderRequest:
        return OrderRequest("TCS", "buy", "limit", 10, limit_price=Decimal(price))

    assert failed(CTX, limit("990.05")) == set()
    assert failed(CTX, limit("990.03")) == {"tick_size"}
    assert failed(CTX, limit("850")) == {"price_band"}  # 15% below the market: a fat finger
    assert failed(CTX, OrderRequest("TCS", "buy", "limit", 10)) == {"price"}
    # A resting limit order does not need a live quote: it names its own price.
    assert failed(replace(CTX, quote_age_seconds=3600), limit("990")) == set()
    assert evaluate(CTX, limit("990"), LIMITS).required_cash == Decimal("9902.97")  # no buffer for a limit order


def test_selling_is_limited_to_shares_not_already_promised() -> None:
    sell = OrderRequest("TCS", "sell", "market", 60)
    assert failed(CTX, sell) == {"holdings"}  # no shorting
    assert failed(replace(CTX, position_quantity=100), sell) == set()
    assert failed(replace(CTX, position_quantity=100, open_sell_quantity=50), sell) == {"holdings"}
    assert evaluate(replace(CTX, position_quantity=100), sell, LIMITS).required_cash == 0


contexts = st.builds(
    RiskContext,
    instrument_active=st.booleans(),
    tick_size=st.just(TICK),
    lot_size=st.sampled_from([1, 5, 25]),
    last_price=st.one_of(st.none(), st.decimals("1", "20000", places=2)),
    quote_age_seconds=st.one_of(st.none(), st.floats(0, 120)),
    cash=st.decimals("0", "5000000", places=2),
    reserved_cash=st.decimals("0", "5000000", places=2),
    position_quantity=st.integers(0, 5000),
    open_sell_quantity=st.integers(0, 5000),
    equity=st.decimals("0", "9000000", places=2),
    orders_last_minute=st.integers(0, 60),
)
orders = st.builds(
    OrderRequest,
    symbol=st.just("TCS"),
    side=st.sampled_from(["buy", "sell"]),
    type=st.sampled_from(["market", "limit", "stop"]),
    quantity=st.integers(-5, 5000),
    limit_price=st.one_of(st.none(), st.decimals("0.05", "20000", places=2)),
    stop_price=st.one_of(st.none(), st.decimals("0.05", "20000", places=2)),
)


@given(ctx=contexts, order=orders)
def test_an_accepted_order_never_exceeds_what_the_account_has(ctx: RiskContext, order: OrderRequest) -> None:
    report = evaluate(ctx, order, LIMITS)
    assert report.passed == (not report.failures)
    if not report.passed:
        return
    assert order.quantity > 0 and order.quantity % ctx.lot_size == 0 and ctx.instrument_active
    assert report.notional <= LIMITS.max_order_notional
    if order.side == "buy":
        assert report.required_cash >= report.notional + report.estimated_fee
        assert report.required_cash <= ctx.cash - ctx.reserved_cash
    else:
        assert order.quantity <= ctx.position_quantity - ctx.open_sell_quantity


prices = st.decimals("1", "20000", places=4)


@given(market=prices, limit=st.decimals("1", "20000", places=2), side=st.sampled_from(["buy", "sell"]))
def test_a_limit_order_never_fills_worse_than_its_limit(market: Decimal, limit: Decimal, side: str) -> None:
    limit = snap(limit, TICK, up=False)
    order = SimpleNamespace(side=side, type="limit", limit_price=limit)
    price = fill_price(order, market, TICK, Decimal(2))  # type: ignore[arg-type]
    assert on_grid(price, TICK)
    assert price <= limit if side == "buy" else price >= limit


@given(market=prices, side=st.sampled_from(["buy", "sell"]), kind=st.sampled_from(["market", "stop"]))
def test_market_fills_cost_the_trader_slippage_never_the_reverse(market: Decimal, side: str, kind: str) -> None:
    order = SimpleNamespace(side=side, type=kind, limit_price=None)
    price = fill_price(order, market, TICK, Decimal(2))  # type: ignore[arg-type]
    assert on_grid(price, TICK)
    assert price >= market if side == "buy" else price <= max(market, TICK)
