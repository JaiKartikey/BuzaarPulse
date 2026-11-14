"""Per-symbol streaming state: the bar being built and the day's running statistics.

Pure logic with no I/O, so it is trivially testable and its whole state can
be checkpointed as one small JSON document (see ``StreamProcessor``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bazaarpulse.marketdata.types import Bar, Tick

BAR_SECONDS = 60
_DAY = 86_400


@dataclass(slots=True)
class SymbolState:
    symbol: str
    prev_close: float
    bar: Bar | None = None
    day: int = 0  # epoch day (UTC) of the session the stats below belong to
    day_open: float = 0.0
    day_high: float = 0.0
    day_low: float = 0.0
    day_volume: int = 0
    last: float = 0.0
    last_ts: float = 0.0
    # Stream id of the last tick folded in. Redelivered ticks at or below it
    # are skipped, which is what makes at-least-once delivery safe here.
    last_id: str = ""

    def apply(self, tick: Tick) -> Bar | None:
        """Fold one tick in. Returns the previous bar if this tick started a new one."""
        closed: Bar | None = None
        # UTC midnight is 05:30 IST and 19:00/20:00 New York: outside both
        # markets' sessions, so the UTC date is a safe session boundary.
        day = int(tick.ts // _DAY)
        if day != self.day:
            if self.day and self.last:
                self.prev_close = self.last
            self.day, self.day_open, self.day_high, self.day_low, self.day_volume = (
                day,
                tick.price,
                tick.price,
                tick.price,
                0,
            )
        if tick.prev_close:
            self.prev_close = tick.prev_close

        start = int(tick.ts // BAR_SECONDS) * BAR_SECONDS
        if self.bar is not None and start > self.bar.start:
            closed, self.bar = self.bar, None
        if self.bar is None:
            self.bar = Bar(self.symbol, start, tick.price, tick.price, tick.price, tick.price)
        # A late tick (older than the open bar) is folded into the open bar rather than dropped.
        self.bar.add(tick.price, tick.size)

        self.day_high = max(self.day_high, tick.price)
        self.day_low = min(self.day_low, tick.price)
        self.day_volume += tick.size
        self.last, self.last_ts = tick.price, tick.ts
        return closed

    def close_if_stale(self, now: float, grace: float = 2.0) -> Bar | None:
        """Close the open bar once its minute is over, even if the symbol went quiet."""
        if self.bar is not None and now >= self.bar.start + BAR_SECONDS + grace:
            closed, self.bar = self.bar, None
            return closed
        return None

    def quote(self) -> dict[str, Any]:
        change = self.last - self.prev_close
        return {
            "symbol": self.symbol,
            "last": round(self.last, 2),
            "prev_close": round(self.prev_close, 2),
            "change": round(change, 2),
            "change_pct": round(change / self.prev_close * 100, 2) if self.prev_close else 0.0,
            "open": round(self.day_open, 2),
            "high": round(self.day_high, 2),
            "low": round(self.day_low, 2),
            "volume": self.day_volume,
            "ts": self.last_ts,
            "bar": self.bar.to_chart() if self.bar else None,
        }

    # --- Checkpointing ------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol, "prev_close": self.prev_close, "bar": self.bar.to_dict() if self.bar else None,
            "day": self.day, "day_open": self.day_open, "day_high": self.day_high, "day_low": self.day_low,
            "day_volume": self.day_volume, "last": self.last, "last_ts": self.last_ts, "last_id": self.last_id,
        }  # fmt: skip

    @classmethod
    def restore(cls, snap: dict[str, Any]) -> SymbolState:
        bar = snap.pop("bar", None)
        state = cls(**snap)
        state.bar = Bar(**bar) if bar else None
        return state
