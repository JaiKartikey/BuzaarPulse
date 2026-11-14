"""Decimal helpers. Everything that is money goes through here; floats stop at this boundary."""

from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal

PAISA = Decimal("0.01")
BPS = Decimal(10_000)


def cash(value: Decimal) -> Decimal:
    """Round to the smallest currency unit."""
    return value.quantize(PAISA, rounding=ROUND_HALF_UP)


def snap(price: float | Decimal, tick: Decimal, *, up: bool) -> Decimal:
    """Snap a price onto the instrument's tick grid, rounding in the direction that hurts the trader.

    A buyer's price rounds up and a seller's rounds down, so rounding can never
    hand anyone a better fill than the market offered.
    """
    value = price if isinstance(price, Decimal) else Decimal(str(price))
    steps = (value / tick).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR)
    return max(tick, steps * tick)


def on_grid(price: Decimal, tick: Decimal) -> bool:
    return price % tick == 0


def bps(amount: Decimal, basis_points: Decimal) -> Decimal:
    return amount * basis_points / BPS
