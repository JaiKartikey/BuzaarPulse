"""A small, honest backtester the agent can call as a tool.

Long-only, all-in or all-out, one decision per bar, fills at the *next* bar's
open. That last rule is the one that matters: deciding on a bar's close and
also trading at that close would be using information from the future
("look-ahead bias"), the most common way backtests lie.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from bazaarpulse.marketdata.indicators import ema, max_drawdown, rsi

STRATEGIES = ("sma_cross", "rsi_reversion", "buy_hold")


def _signal_sma_cross(closes: Sequence[float]) -> bool | None:
    fast, slow = ema(closes, 9), ema(closes, 21)
    return None if fast is None or slow is None else fast > slow


def _signal_rsi_reversion(closes: Sequence[float], *, holding: bool) -> bool | None:
    value = rsi(closes, 14)
    if value is None:
        return None
    if value < 30:
        return True  # oversold: buy
    if value > 55:
        return False  # recovered: sell
    return holding


def run_backtest(candles: Sequence[dict[str, Any]], strategy: str, *, fee_bps: float = 3.0) -> dict[str, Any]:
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy {strategy!r}; choose one of {', '.join(STRATEGIES)}")
    if len(candles) < 30:
        return {"strategy": strategy, "bars": len(candles), "error": "not enough history (need at least 30 bars)"}

    closes = [float(c["c"]) for c in candles]
    opens = [float(c["o"]) for c in candles]
    fee = fee_bps / 10_000
    decide: Callable[[Sequence[float], bool], bool | None]
    if strategy == "sma_cross":
        decide = lambda history, holding: _signal_sma_cross(history)  # noqa: E731
    elif strategy == "rsi_reversion":
        decide = lambda history, holding: _signal_rsi_reversion(history, holding=holding)  # noqa: E731
    else:
        decide = lambda history, holding: True  # noqa: E731

    cash, shares, entry = 1.0, 0.0, 0.0
    trades = wins = bars_in_market = 0
    equity: list[float] = []
    for i in range(len(candles)):
        # Act on yesterday's decision at today's open.
        if i > 0:
            want = decide(closes[:i], shares > 0)
            if want is True and shares == 0:
                shares, cash, entry = cash * (1 - fee) / opens[i], 0.0, opens[i]
                trades += 1
            elif want is False and shares > 0:
                cash, shares = shares * opens[i] * (1 - fee), 0.0
                wins += opens[i] > entry
        bars_in_market += shares > 0
        equity.append(cash + shares * closes[i])

    closed_trades = trades if shares == 0 else trades - 1
    return {
        "strategy": strategy,
        "bars": len(candles),
        "total_return_pct": round((equity[-1] - 1) * 100, 2),
        "buy_hold_return_pct": round((closes[-1] / opens[0] - 1) * 100, 2),
        "trades": trades,
        "win_rate_pct": round(wins / closed_trades * 100, 1) if closed_trades else None,
        "max_drawdown_pct": round(max_drawdown(equity), 2),
        "time_in_market_pct": round(bars_in_market / len(candles) * 100, 1),
        "fee_bps_per_trade": fee_bps,
        "note": "Past simulated performance on a short window; not a prediction.",
    }
