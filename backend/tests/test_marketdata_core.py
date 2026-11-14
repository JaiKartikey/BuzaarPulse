"""Pure market-data logic: indicators, candle building, anomaly detection, the simulator."""

from __future__ import annotations

import math

import pytest

from bazaarpulse.marketdata import indicators as ind
from bazaarpulse.marketdata.anomaly import AnomalyDetector
from bazaarpulse.marketdata.candles import SymbolState
from bazaarpulse.marketdata.instruments import NSE_UNIVERSE
from bazaarpulse.marketdata.providers.simulator import SimulatedMarket
from bazaarpulse.marketdata.types import Tick

T0 = 1_800_000_000.0 - (1_800_000_000 % 86_400) + 4 * 3600  # 04:00 UTC on some day, minute-aligned


# --- Indicators -------------------------------------------------------------


def test_sma_and_ema() -> None:
    assert ind.sma([1, 2, 3, 4], 2) == 3.5
    assert ind.sma([1, 2], 3) is None
    assert ind.ema([5, 5, 5, 5, 5], 3) == pytest.approx(5)
    # EMA weights recent values more: after a jump it sits above the SMA.
    series = [10.0] * 10 + [20.0] * 3
    assert ind.ema(series, 5) > ind.sma(series, 5) - 1e-9  # type: ignore[operator]


def test_rsi_extremes_and_midpoint() -> None:
    assert ind.rsi([float(i) for i in range(30)]) == 100.0
    assert ind.rsi([float(30 - i) for i in range(30)]) == pytest.approx(0.0)
    assert ind.rsi([10.0] * 30) == 50.0
    assert ind.rsi([1.0, 2.0]) is None
    zigzag = [100 + (1 if i % 2 else -1) for i in range(40)]
    assert 40 < ind.rsi(zigzag) < 60  # type: ignore[operator]


def test_atr_and_drawdown() -> None:
    n = 20
    highs, lows, closes = [11.0] * n, [9.0] * n, [10.0] * n
    assert ind.atr(highs, lows, closes) == pytest.approx(2.0)
    assert ind.max_drawdown([100, 120, 90, 130, 117]) == pytest.approx(25.0)
    assert ind.max_drawdown([1, 2, 3]) == 0.0


def test_summarize_reads_a_trend() -> None:
    up = [{"o": p, "h": p + 1, "l": p - 1, "c": p + 0.5, "v": 100, "vw": p} for p in range(100, 160)]
    s = ind.summarize(up)
    assert s["trend"] == "up" and s["rsi_state"] == "overbought" and s["bars"] == 60
    assert s["change_5_bars_pct"] > 0 and s["vs_vwap_pct"] > 0
    assert ind.summarize([]) == {"bars": 0}


# --- Candle building --------------------------------------------------------


def test_ticks_fold_into_minute_bars() -> None:
    st = SymbolState("TCS", prev_close=100.0)
    assert st.apply(Tick("TCS", 101.0, 10, T0 + 1)) is None
    assert st.apply(Tick("TCS", 103.0, 5, T0 + 20)) is None
    assert st.apply(Tick("TCS", 99.0, 5, T0 + 59)) is None
    closed = st.apply(Tick("TCS", 100.0, 1, T0 + 61))  # first tick of the next minute closes the bar

    assert closed is not None
    assert (closed.open, closed.high, closed.low, closed.close) == (101.0, 103.0, 99.0, 99.0)
    assert closed.volume == 20 and closed.trades == 3 and closed.start == T0
    assert closed.vwap == pytest.approx((101 * 10 + 103 * 5 + 99 * 5) / 20)

    q = st.quote()
    assert q["last"] == 100.0 and q["change_pct"] == 0.0 and q["high"] == 103.0 and q["low"] == 99.0
    assert q["volume"] == 21 and q["bar"]["t"] == T0 + 60


def test_quiet_symbol_bar_closes_on_time() -> None:
    st = SymbolState("ITC", prev_close=470.0)
    st.apply(Tick("ITC", 471.0, 3, T0 + 5))
    assert st.close_if_stale(T0 + 59) is None
    closed = st.close_if_stale(T0 + 63)
    assert closed is not None and closed.close == 471.0 and st.bar is None


def test_new_session_rolls_previous_close() -> None:
    st = SymbolState("SBIN", prev_close=800.0)
    st.apply(Tick("SBIN", 820.0, 1, T0))
    st.apply(Tick("SBIN", 830.0, 1, T0 + 86_400))  # next day
    q = st.quote()
    assert q["prev_close"] == 820.0 and q["open"] == 830.0 and q["volume"] == 1


def test_state_survives_a_checkpoint_round_trip() -> None:
    st = SymbolState("LT", prev_close=3600.0)
    st.apply(Tick("LT", 3610.0, 7, T0 + 3))
    st.last_id = "1700-3"
    restored = SymbolState.restore(st.snapshot())
    assert restored.quote() == st.quote() and restored.last_id == "1700-3"
    # Both continue identically from here.
    assert restored.apply(Tick("LT", 3620.0, 1, T0 + 70)) == st.apply(Tick("LT", 3620.0, 1, T0 + 70))


# --- Anomaly detection ------------------------------------------------------


def _calm(detector: AnomalyDetector, symbol: str, start: float, seconds: int, price: float = 1000.0) -> float:
    """Feed a gently oscillating price, one tick per second; returns the time after the last tick."""
    for i in range(seconds):
        wobble = 0.0002 * math.sin(i / 3)
        assert detector.update(Tick(symbol, price * (1 + wobble), 100, start + i)) is None
    return start + seconds


def test_sharp_drop_is_flagged_once() -> None:
    d = AnomalyDetector(cooldown_seconds=60)
    t = _calm(d, "INFY", T0, 300)
    events = []
    for i in range(10):  # -2.5% over ten seconds
        e = d.update(Tick("INFY", 1000.0 * (1 - 0.0025 * (i + 1)), 900, t + i))
        if e:
            events.append(e)
    assert len(events) == 1, "cooldown should collapse one move into one alert"
    e = events[0]
    assert e.kind == "price_drop" and e.move_pct <= -0.6 and e.zscore >= 4 and e.severity in {"low", "medium", "high"}


def test_no_alert_without_history_or_for_a_small_move() -> None:
    d = AnomalyDetector()
    # A big move with no history to compare against is not judged.
    assert d.update(Tick("X", 100.0, 1, T0)) is None
    assert d.update(Tick("X", 110.0, 1, T0 + 1)) is None
    # A move below the percent floor is ignored however unusual it is statistically.
    t = _calm(d, "Y", T0, 300)
    assert d.update(Tick("Y", 1003.0, 100, t)) is None


def test_volatile_stock_needs_a_bigger_move() -> None:
    d = AnomalyDetector()
    price, t = 1000.0, T0
    for i in range(300):  # routinely swings about 0.5% between buckets
        price = 1000.0 * (1 + (0.005 if (i // 5) % 2 else -0.005))
        d.update(Tick("WILD", price, 100, t + i))
    # 1% is above the percent floor, but ordinary for this stock: z-score says no.
    assert d.update(Tick("WILD", price * 1.01, 100, t + 300)) is None


# --- Simulator --------------------------------------------------------------


def test_simulator_is_deterministic_and_sane() -> None:
    def run(seed: int) -> list[Tick]:
        m = SimulatedMarket(NSE_UNIVERSE, seed=seed, shocks_per_hour=0)
        return [t for i in range(400) for t in m.step(T0 + i * 0.25)]

    a, b = run(1), run(1)
    assert a == b and a != run(2)
    assert {t.symbol for t in a} == {s.symbol for s in NSE_UNIVERSE}
    refs = {s.symbol: float(s.ref_price) for s in NSE_UNIVERSE}
    assert all(abs(t.price / refs[t.symbol] - 1) < 0.02 for t in a), "100 calm seconds should not move 2%"
    assert all(round(t.price / 0.05) * 0.05 == pytest.approx(t.price) for t in a), "prices sit on the tick grid"


def test_shock_moves_price_publishes_news_and_trips_the_detector() -> None:
    m = SimulatedMarket(NSE_UNIVERSE, seed=3, shocks_per_hour=0)
    d = AnomalyDetector()
    t = T0
    for _ in range(4 * 200):  # 200 calm seconds
        for tick in m.step(t):
            d.update(tick)
        t += 0.25
    before = m.prices["TATAMOTORS"]

    news = m.shock("TATAMOTORS", -3.0, t)
    assert news.symbol == "TATAMOTORS" and news.is_simulated and news.sentiment == -0.7
    assert "Tata Motors" in news.headline and "3.0%" in news.headline

    alerts = []
    for _ in range(4 * 15):
        for tick in m.step(t):
            if (e := d.update(tick)) is not None:
                alerts.append(e)
        t += 0.25
    assert m.prices["TATAMOTORS"] / before - 1 == pytest.approx(-0.03, abs=0.006)
    assert [a.symbol for a in alerts] == ["TATAMOTORS"] and alerts[0].kind == "price_drop"


def test_history_produces_continuous_minute_bars() -> None:
    m = SimulatedMarket(NSE_UNIVERSE, seed=5, shocks_per_hour=0)
    bars = m.history(T0 + 3600, minutes=30)
    tcs = [b for b in bars if b.symbol == "TCS"]
    assert len(tcs) == 30 and [b.start for b in tcs] == sorted(b.start for b in tcs)
    assert all(b.low <= b.open <= b.high and b.low <= b.close <= b.high and b.volume > 0 for b in bars)
    assert tcs[-1].start == T0 + 3600 - 60
    # Live trading continues from where history ended.
    assert m.prices["TCS"] == pytest.approx(tcs[-1].close, rel=0.002)
