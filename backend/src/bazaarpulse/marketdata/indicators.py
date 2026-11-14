"""Technical indicators as pure functions over lists of floats (oldest first).

No pandas or numpy: the inputs are a few hundred candles, and plain Python
keeps the worker image small and the maths readable.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Sequence
from typing import Any


def sma(values: Sequence[float], period: int) -> float | None:
    if len(values) < period or period <= 0:
        return None
    return sum(values[-period:]) / period


def ema(values: Sequence[float], period: int) -> float | None:
    """Exponential moving average, seeded with the SMA of the first ``period`` values."""
    if len(values) < period or period <= 0:
        return None
    k = 2 / (period + 1)
    avg = sum(values[:period]) / period
    for v in values[period:]:
        avg = v * k + avg * (1 - k)
    return avg


def rsi(closes: Sequence[float], period: int = 14) -> float | None:
    """Wilder's RSI: 100 = only gains over the window, 0 = only losses."""
    if len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for prev, cur in itertools.pairwise(closes):
        change = cur - prev
        gains += max(change, 0.0)
        losses += max(-change, 0.0)
    avg_gain, avg_loss = gains / period, losses / period
    for prev, cur in zip(closes[period:], closes[period + 1 :], strict=False):
        change = cur - prev
        avg_gain = (avg_gain * (period - 1) + max(change, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-change, 0.0)) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


def atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14) -> float | None:
    """Average true range: typical bar-to-bar movement, in price units."""
    if len(closes) < period + 1:
        return None
    true_ranges = [
        max(h - low, abs(h - prev_close), abs(low - prev_close))
        for h, low, prev_close in zip(highs[1:], lows[1:], closes[:-1], strict=False)
    ]
    value = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        value = (value * (period - 1) + tr) / period
    return value


def returns(closes: Sequence[float]) -> list[float]:
    return [cur / prev - 1 for prev, cur in itertools.pairwise(closes) if prev]


def stdev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / (len(values) - 1))


def pct_change(closes: Sequence[float], bars_back: int) -> float | None:
    if len(closes) <= bars_back or not closes[-1 - bars_back]:
        return None
    return (closes[-1] / closes[-1 - bars_back] - 1) * 100


def max_drawdown(equity: Sequence[float]) -> float:
    """Largest peak-to-trough fall of an equity curve, as a positive percentage."""
    peak, worst = -math.inf, 0.0
    for v in equity:
        peak = max(peak, v)
        if peak > 0:
            worst = max(worst, (peak - v) / peak)
    return worst * 100


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(value, digits)


def summarize(candles: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """A compact indicator snapshot from chart candles (``{"o","h","l","c","v","vw"}``, oldest first)."""
    if not candles:
        return {"bars": 0}
    closes = [float(c["c"]) for c in candles]
    highs = [float(c["h"]) for c in candles]
    lows = [float(c["l"]) for c in candles]
    volumes = [float(c["v"]) for c in candles]
    last = closes[-1]

    ema_fast, ema_slow = ema(closes, 9), ema(closes, 21)
    rsi14 = rsi(closes, 14)
    atr14 = atr(highs, lows, closes, 14)
    total_volume = sum(volumes)
    vwap = sum(float(c.get("vw", c["c"])) * float(c["v"]) for c in candles) / total_volume if total_volume else last

    if ema_fast is None or ema_slow is None:
        trend = "unknown"
    elif ema_fast > ema_slow * 1.0005:
        trend = "up"
    elif ema_fast < ema_slow * 0.9995:
        trend = "down"
    else:
        trend = "sideways"

    avg_volume = sum(volumes[-21:-1]) / len(volumes[-21:-1]) if len(volumes) > 1 else 0.0
    return {
        "bars": len(candles),
        "last": round(last, 2),
        "trend": trend,
        "ema9": _round(ema_fast),
        "ema21": _round(ema_slow),
        "sma20": _round(sma(closes, 20)),
        "rsi14": _round(rsi14, 1),
        "rsi_state": None
        if rsi14 is None
        else ("overbought" if rsi14 >= 70 else "oversold" if rsi14 <= 30 else "neutral"),
        "atr14_pct": _round(atr14 / last * 100 if atr14 and last else None, 3),
        "volatility_pct_per_bar": _round(stdev(returns(closes[-60:])) * 100, 3),
        "vwap": round(vwap, 2),
        "vs_vwap_pct": round((last / vwap - 1) * 100, 2) if vwap else None,
        "change_5_bars_pct": _round(pct_change(closes, 5)),
        "change_15_bars_pct": _round(pct_change(closes, 15)),
        "change_60_bars_pct": _round(pct_change(closes, 60)),
        "range_high": round(max(highs), 2),
        "range_low": round(min(lows), 2),
        "volume_vs_avg": round(volumes[-1] / avg_volume, 2) if avg_volume else None,
    }
