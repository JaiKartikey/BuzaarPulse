"""Streaming price-move detector.

Idea: a move only matters relative to how much the stock normally moves. So
prices are sampled into short buckets (5 s by default), the usual size of a
bucket-to-bucket return is tracked, and a move over the last few buckets is
flagged when it is both large in percent *and* many standard deviations
beyond normal. The percent floor stops a sleepy stock's tiny wiggle from
qualifying on z-score alone; the z-score stops a volatile stock's ordinary
swing from qualifying on percent alone.
"""

from __future__ import annotations

import itertools
import math
from collections import deque
from dataclasses import dataclass, field

from bazaarpulse.marketdata.indicators import stdev
from bazaarpulse.marketdata.types import AnomalyEvent, Tick

LOOKBACK_BUCKETS = 6  # a move is measured over this many buckets (30 s at the default size)
MIN_HISTORY = 20  # buckets of history needed before judging anything


@dataclass(slots=True)
class _Series:
    bucket: int = -1
    closes: deque[float] = field(default_factory=deque)
    volumes: deque[int] = field(default_factory=deque)
    price: float = 0.0
    volume: int = 0
    last_alert: float = -math.inf


class AnomalyDetector:
    def __init__(
        self,
        *,
        bar_seconds: int = 5,
        window_bars: int = 60,
        zscore: float = 4.0,
        min_move_pct: float = 0.6,
        cooldown_seconds: float = 90,
    ) -> None:
        self.bar_seconds = bar_seconds
        self.window = window_bars
        self.zscore = zscore
        self.min_move_pct = min_move_pct
        self.cooldown = cooldown_seconds
        self._series: dict[str, _Series] = {}

    def update(self, tick: Tick) -> AnomalyEvent | None:
        s = self._series.get(tick.symbol)
        if s is None:
            s = self._series[tick.symbol] = _Series(
                closes=deque(maxlen=self.window + LOOKBACK_BUCKETS), volumes=deque(maxlen=self.window)
            )
        bucket = int(tick.ts // self.bar_seconds)
        if bucket != s.bucket:
            if s.bucket >= 0:
                s.closes.append(s.price)
                s.volumes.append(s.volume)
            s.bucket, s.volume = bucket, 0
        s.price = tick.price
        s.volume += tick.size

        if len(s.closes) < MIN_HISTORY or tick.ts - s.last_alert < self.cooldown:
            return None

        closes = list(s.closes)
        reference = closes[-LOOKBACK_BUCKETS]
        move_pct = (tick.price / reference - 1) * 100
        if abs(move_pct) < self.min_move_pct:
            return None

        # "Normal" is estimated from buckets *before* the move, so the move cannot inflate its own yardstick.
        history = closes[:-LOOKBACK_BUCKETS]
        typical = stdev([b / a - 1 for a, b in itertools.pairwise(history)]) * 100
        expected = max(typical * math.sqrt(LOOKBACK_BUCKETS), 1e-9)
        z = move_pct / expected
        if abs(z) < self.zscore:
            return None

        s.last_alert = tick.ts
        avg_volume = sum(s.volumes) / len(s.volumes) if s.volumes else 0
        size = abs(move_pct)
        return AnomalyEvent(
            symbol=tick.symbol,
            kind="price_spike" if move_pct > 0 else "price_drop",
            severity="high" if size >= 2.0 else "medium" if size >= 1.0 else "low",
            ts=tick.ts,
            move_pct=round(move_pct, 2),
            zscore=round(min(abs(z), 99.0), 1),
            window_seconds=LOOKBACK_BUCKETS * self.bar_seconds,
            price=tick.price,
            volume_ratio=round(s.volume / avg_volume, 1) if avg_volume else 0.0,
        )
