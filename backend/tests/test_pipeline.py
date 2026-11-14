"""The market-data pipeline end to end: ingestor -> stream -> processor -> Postgres/Redis -> store."""

from __future__ import annotations

import asyncio
import time

import pytest
from sqlalchemy import func, select

from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import Anomaly, Candle, News
from bazaarpulse.marketdata.ingestor import Ingestor
from bazaarpulse.marketdata.instruments import NSE_UNIVERSE
from bazaarpulse.marketdata.processor import GROUP, StreamProcessor
from bazaarpulse.marketdata.providers.simulator import SimulatedMarket, SimulatorProvider
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.marketdata.types import Batch, NewsItem, Tick
from tests.conftest import make_settings


def minute_start() -> float:
    """Start of a recent 5-minute bucket: inside the store's query window, and stable for 5m-candle assertions."""
    return (int(time.time()) // 300 - 2) * 300.0


async def drain(processor: StreamProcessor, bus: RedisBus, now: float) -> None:
    while batch := await bus.consume(processor.streams, GROUP, processor.consumer, count=5000, block_ms=None):
        await processor.handle(batch, now=now)


@pytest.fixture
def pipeline(bus: RedisBus, db: Database, settings: Settings):  # type: ignore[no-untyped-def]
    provider = SimulatorProvider(NSE_UNIVERSE, seed=1, shocks_per_hour=0)
    return Ingestor(provider, bus, settings), StreamProcessor(bus, db, settings), MarketStore(bus, db)


async def test_ticks_become_quotes_and_candles(pipeline, bus: RedisBus, db: Database) -> None:  # type: ignore[no-untyped-def]
    ingestor, processor, store = pipeline
    await processor.start()
    t0 = minute_start()
    await ingestor.publish(
        Batch(
            [
                Tick("TCS", 4100.0, 10, t0 + 1),
                Tick("TCS", 4120.0, 20, t0 + 30),
                Tick("TCS", 4090.0, 10, t0 + 59),
                Tick("TCS", 4110.0, 5, t0 + 61),  # opens the next minute, closing the first
                Tick("INFY", 1860.0, 7, t0 + 2),
            ]
        )
    )
    await drain(processor, bus, now=t0 + 62)

    quote = await store.quote("TCS")
    assert quote is not None
    assert quote["last"] == 4110.0 and quote["high"] == 4120.0 and quote["low"] == 4090.0 and quote["volume"] == 45
    assert quote["prev_close"] == 4100.0 and quote["change_pct"] == pytest.approx(0.24)

    candles = await store.candles("TCS", "1m")
    assert [(c["o"], c["h"], c["l"], c["c"], c["v"]) for c in candles] == [
        (4100.0, 4120.0, 4090.0, 4090.0, 40),  # closed, read from Postgres
        (4110.0, 4110.0, 4110.0, 4110.0, 5),  # still open, merged in from Redis
    ]
    five = await store.candles("TCS", "5m")
    assert len(five) == 1 and five[0]["o"] == 4100.0 and five[0]["c"] == 4110.0 and five[0]["v"] == 45

    overview = await store.overview()
    assert overview["advancers"] == 2 and overview["top_gainers"][0]["symbol"] == "INFY"
    stats = await store.pipeline_stats()
    assert stats["ticks_total"] == 5


async def test_redelivered_ticks_are_not_double_counted(pipeline, bus: RedisBus) -> None:  # type: ignore[no-untyped-def]
    ingestor, processor, store = pipeline
    await processor.start()
    t0 = minute_start()
    await ingestor.publish(Batch([Tick("SBIN", 820.0, 100, t0 + 1), Tick("SBIN", 821.0, 50, t0 + 2)]))

    batch = await bus.consume(processor.streams, GROUP, processor.consumer, block_ms=None)
    await processor.handle(batch, now=t0 + 3)
    await processor.handle(batch, now=t0 + 3)  # the same messages again, as after a lost ack

    assert (await store.quote("SBIN"))["volume"] == 150  # type: ignore[index]
    assert (await store.pipeline_stats())["ticks_total"] == 2


async def test_restart_resumes_from_checkpoint(pipeline, bus: RedisBus, db: Database, settings: Settings) -> None:  # type: ignore[no-untyped-def]
    ingestor, processor, store = pipeline
    await processor.start()
    t0 = minute_start()
    await ingestor.publish(Batch([Tick("ITC", 470.0, 10, t0 + 1), Tick("ITC", 472.0, 10, t0 + 2)]))
    await drain(processor, bus, now=t0 + 3)

    # The process dies. A new one must rebuild the open bar from the checkpoint, not start it from scratch.
    await ingestor.publish(Batch([Tick("ITC", 468.0, 5, t0 + 20), Tick("ITC", 471.0, 5, t0 + 65)]))
    reborn = StreamProcessor(bus, db, settings)
    await reborn.start()
    assert reborn.states["ITC"].bar is not None and reborn.states["ITC"].bar.volume == 20
    await drain(reborn, bus, now=t0 + 66)

    candles = await store.candles("ITC", "1m")
    assert (candles[0]["o"], candles[0]["h"], candles[0]["l"], candles[0]["c"], candles[0]["v"]) == (
        470.0, 472.0, 468.0, 468.0, 25,
    )  # fmt: skip
    assert await bus.consume(reborn.streams, GROUP, reborn.consumer, block_ms=None, pending=True) == []


async def test_backfill_runs_once_and_fills_history(pipeline, bus: RedisBus, db: Database) -> None:  # type: ignore[no-untyped-def]
    ingestor, processor, store = pipeline
    ingestor.settings = make_settings(redis_prefix=bus.prefix, history_backfill_minutes=30)
    await processor.start()
    assert await ingestor.backfill() == 30 * len(NSE_UNIVERSE)
    assert await ingestor.backfill() == 0, "a second ingestor (or a restart) must not backfill again"
    await drain(processor, bus, now=time.time())

    async with db.session() as session:
        count = (await session.execute(select(func.count()).select_from(Candle))).scalar_one()
    assert count == 30 * len(NSE_UNIVERSE)
    assert len(await store.candles("RELIANCE", "1m", limit=100)) == 30
    assert len(await store.candles("RELIANCE", "5m", limit=100)) in (6, 7)


async def test_shock_produces_news_anomaly_and_pulse_events(pipeline, bus: RedisBus, db: Database) -> None:  # type: ignore[no-untyped-def]
    ingestor, processor, store = pipeline
    await processor.start()
    await bus.ensure_group(topics.ANOMALIES, "sentinel", start_id="0")
    pulses: list[dict[str, object]] = []

    async def listen() -> None:
        async for _, data in bus.subscribe([topics.PULSE]):
            pulses.append(data)

    listener = asyncio.create_task(listen())
    await asyncio.sleep(0.05)

    market = SimulatedMarket(NSE_UNIVERSE, seed=11, shocks_per_hour=0)
    t = minute_start() - 300
    for step in range(4 * 230):
        if step == 4 * 200:
            market.shock("TATAMOTORS", -3.0, t)
        news, market.pending_news = market.pending_news, []
        await ingestor.publish(Batch(market.step(t), news))
        t += 0.25
    await drain(processor, bus, now=t)
    await asyncio.sleep(0.05)
    listener.cancel()

    async with db.session() as session:
        anomalies = (await session.execute(select(Anomaly))).scalars().all()
        stories = (await session.execute(select(News))).scalars().all()
    assert [(a.symbol, a.kind) for a in anomalies] == [("TATAMOTORS", "price_drop")]
    assert len(stories) == 1 and stories[0].symbol == "TATAMOTORS" and stories[0].is_simulated

    # The anomaly is on the durable stream for the sentinel agent, and both events reached the live feed.
    queued = await bus.consume([topics.ANOMALIES], "sentinel", "s1", block_ms=None)
    assert [e.data["symbol"] for e in queued] == ["TATAMOTORS"] and queued[0].data["id"] == str(anomalies[0].id)
    assert {p["type"] for p in pulses} == {"news", "anomaly"}

    assert (await store.anomalies())[0]["move_pct"] <= -0.6  # fires as the move crosses the floor
    found = await store.news(query="tata motors")
    assert len(found) == 1 and found[0]["symbol"] == "TATAMOTORS"
    assert await store.news(query="monsoon rainfall") == []


async def test_duplicate_news_is_stored_once(pipeline, bus: RedisBus, db: Database) -> None:  # type: ignore[no-untyped-def]
    ingestor, processor, _ = pipeline
    await processor.start()
    item = NewsItem("RBI holds rates", ts=minute_start(), symbol=None, source="Wire")
    await ingestor.publish(Batch(news=[item, item]))
    await ingestor.publish(Batch(news=[item]))
    await drain(processor, bus, now=time.time())
    async with db.session() as session:
        assert (await session.execute(select(func.count()).select_from(News))).scalar_one() == 1


async def test_quotes_are_conflated_for_the_ui(pipeline, bus: RedisBus) -> None:  # type: ignore[no-untyped-def]
    ingestor, processor, _ = pipeline
    await processor.start()
    frames: list[dict[str, object]] = []

    async def listen() -> None:
        async for _, data in bus.subscribe([topics.QUOTES]):
            frames.append(data)

    listener = asyncio.create_task(listen())
    await asyncio.sleep(0.05)
    t0 = minute_start()
    # 60 ticks for one symbol inside 150 ms of "now": the UI should not get 60 frames.
    for i in range(6):
        await ingestor.publish(Batch([Tick("LT", 3600.0 + i + j / 10, 1, t0 + i) for j in range(10)]))
        await drain(processor, bus, now=t0 + 10 + i * 0.03)
    await drain(processor, bus, now=t0 + 11)
    await ingestor.publish(Batch([Tick("LT", 3700.0, 1, t0 + 12)]))
    await drain(processor, bus, now=t0 + 12)
    await asyncio.sleep(0.05)
    listener.cancel()

    assert 2 <= len(frames) <= 3
    last = frames[-1]["quotes"][0]  # type: ignore[index]
    assert last["symbol"] == "LT" and last["last"] == 3700.0
