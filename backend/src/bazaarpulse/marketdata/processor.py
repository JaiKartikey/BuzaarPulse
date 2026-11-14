"""Stream processor: ticks in; candles, quotes and anomalies out.

Delivery is at-least-once (a consumer group redelivers anything not
acknowledged), so correctness comes from making every effect idempotent:

* Each symbol's state remembers the stream id of the last tick it absorbed.
  A redelivered tick at or below that id is skipped.
* That state is checkpointed to Redis in the *same atomic transaction* that
  acknowledges the batch. Either both happen or neither does, so after a crash
  the state and the stream position always agree.
* Closed candles are upserted to Postgres *before* the checkpoint. If we crash
  in between, replay rebuilds the identical candle and upserts it again.

Net effect: every tick counts exactly once, with no distributed transaction.

Scaling out: ticks are split over partitions by symbol, and each partition is
owned by one processor at a time through a lease (see ``bus/leases.py``). A
processor that takes a partition over loads that partition's checkpoints and
adopts the previous owner's unacknowledged ticks before reading new ones.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from collections import deque
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import orjson
from redis.exceptions import WatchError
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

from bazaarpulse.bus import Envelope, RedisBus, topics
from bazaarpulse.bus.leases import PartitionLeases
from bazaarpulse.bus.redis_bus import stream_id_key
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import Anomaly, Candle, Instrument, News
from bazaarpulse.logging import get_logger
from bazaarpulse.marketdata.anomaly import AnomalyDetector
from bazaarpulse.marketdata.candles import SymbolState
from bazaarpulse.marketdata.types import AnomalyEvent, Bar, Tick
from bazaarpulse.metrics import (
    ANOMALIES_DETECTED,
    CANDLES_WRITTEN,
    PIPELINE_LATENCY,
    STREAM_LAG,
    TICKS_DUPLICATE,
    TICKS_PROCESSED,
)

log = get_logger(__name__)
GROUP = "processor"
STATS_KEY = "stats.pipeline"


def quote_key(symbol: str) -> str:
    return f"quote.{symbol}"


def _state_key(symbol: str) -> str:
    return f"agg.{symbol}"


def _price(value: float) -> Decimal:
    return Decimal(str(round(value, 4)))


def _candle_row(bar: Bar) -> dict[str, Any]:
    return {
        "symbol": bar.symbol,
        "interval": "1m",
        "ts": datetime.fromtimestamp(bar.start, UTC),
        "open": _price(bar.open),
        "high": _price(bar.high),
        "low": _price(bar.low),
        "close": _price(bar.close),
        "volume": bar.volume,
        "vwap": _price(bar.vwap),
        "trades": bar.trades,
    }


class StreamProcessor:
    def __init__(
        self,
        bus: RedisBus,
        db: Database,
        settings: Settings,
        *,
        partitions: list[int] | None = None,
        consumer: str = "processor-0",
        leases: PartitionLeases | None = None,
    ) -> None:
        self.bus = bus
        self.db = db
        self.settings = settings
        self.consumer = consumer
        # With leases, ownership is dynamic and starts empty. Without (tests, single process), it is fixed.
        self.leases = leases
        self.partitions = (
            [] if leases else (partitions if partitions is not None else list(range(settings.tick_partitions)))
        )
        self.tick_streams = [topics.ticks(p) for p in self.partitions]
        self.streams = [*self.tick_streams, topics.NEWS, topics.BARS]
        self.states: dict[str, SymbolState] = {}
        self.detector = AnomalyDetector(
            bar_seconds=settings.anomaly_bar_seconds,
            window_bars=settings.anomaly_window_bars,
            zscore=settings.anomaly_zscore,
            min_move_pct=settings.anomaly_min_move_pct,
            cooldown_seconds=settings.anomaly_cooldown_seconds,
        )
        self._ref_prices: dict[str, float] = {}
        self._to_broadcast: set[str] = set()
        self._last_broadcast = 0.0
        self._latencies: deque[float] = deque(maxlen=500)
        self._recent: deque[tuple[float, int]] = deque()  # (time, ticks) for the throughput figure
        self._last_stats = 0.0
        self._stopping = False

    # --- Lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        every_stream = [*(topics.ticks(p) for p in range(self.settings.tick_partitions)), topics.NEWS, topics.BARS]
        for stream in {*every_stream, *self.streams}:
            await self.bus.ensure_group(stream, GROUP, start_id="0")
        async with self.db.transaction() as session:
            await session.execute(text("SELECT ensure_candle_partitions(2)"))
            rows = (await session.execute(select(Instrument.symbol, Instrument.ref_price))).all()
        self._ref_prices = {symbol: float(ref) for symbol, ref in rows}
        await self._load_checkpoints(list(self._ref_prices))

    async def run(self) -> None:
        await self.start()
        if self.leases is None:
            await self._adopt_pending(self.streams, min_idle_ms=0)
        log.info("processor.live", consumer=self.consumer, leased=self.leases is not None)
        last_refresh = 0.0
        try:
            while not self._stopping:
                if self.leases is not None and time.monotonic() - last_refresh > self.leases.ttl_ms / 3000:
                    await self._sync_leases()
                    last_refresh = time.monotonic()
                batch = await self.bus.consume(
                    self.streams, GROUP, self.consumer, count=2000, block_ms=self.settings.quote_conflate_ms
                )
                await self.handle(batch)
        finally:
            if self.leases is not None:
                await self.leases.release_all()

    async def _sync_leases(self) -> None:
        assert self.leases is not None
        acquired, lost = await self.leases.refresh()
        for partition in lost:
            self._drop(partition)
        for partition in sorted(acquired):
            await self._take_over(partition)
        if acquired or lost:
            log.info("processor.rebalanced", consumer=self.consumer, held=sorted(self.leases.held))

    def _set_partitions(self, partitions: set[int]) -> None:
        self.partitions = sorted(partitions)
        self.tick_streams = [topics.ticks(p) for p in self.partitions]
        self.streams = [*self.tick_streams, topics.NEWS, topics.BARS]

    def _symbols_of(self, partition: int) -> list[str]:
        total = self.settings.tick_partitions
        return [s for s in self._ref_prices if topics.partition_for(s, total) == partition]

    async def _take_over(self, partition: int) -> None:
        """Become the owner: adopt the last owner's state and its unfinished work, then read new ticks."""
        await self._load_checkpoints(self._symbols_of(partition))
        self._set_partitions({*self.partitions, partition})
        await self._adopt_pending([topics.ticks(partition)], min_idle_ms=0)

    def _drop(self, partition: int) -> None:
        """No longer the owner: forget this partition's state so a stale copy is never used again."""
        for symbol in self._symbols_of(partition):
            self.states.pop(symbol, None)
            self._to_broadcast.discard(symbol)
        self._set_partitions(set(self.partitions) - {partition})

    async def _adopt_pending(self, streams: list[str], *, min_idle_ms: int) -> None:
        """Process ticks that were delivered to some consumer but never acknowledged (it crashed mid-batch)."""
        for stream in streams:
            while claimed := await self.bus.claim_stale(
                stream, GROUP, self.consumer, min_idle_ms=min_idle_ms, count=1000
            ):
                await self.handle(claimed)
                if len(claimed) < 1000:
                    break

    def stop(self) -> None:
        self._stopping = True

    async def _load_checkpoints(self, symbols: list[str]) -> None:
        if not symbols:
            return
        for raw in await self.bus.redis.mget([self.bus.key(_state_key(s)) for s in symbols]):
            if raw:
                state = SymbolState.restore(orjson.loads(raw))
                self.states[state.symbol] = state

    # --- One batch ----------------------------------------------------------

    async def handle(self, batch: list[Envelope], now: float | None = None) -> None:
        now = time.time() if now is None else now
        closed: list[Bar] = []
        history: list[Bar] = []
        news: list[dict[str, Any]] = []
        anomalies: list[AnomalyEvent] = []
        touched: set[str] = set()
        ack: dict[str, list[str]] = {}
        applied = 0

        for env in batch:
            ack.setdefault(env.stream, []).append(env.id)
            if env.stream == topics.NEWS:
                news.append(env.data)
            elif env.stream == topics.BARS:
                history.extend(Bar(**b) for b in env.data["bars"])
            else:
                tick = Tick.from_wire(env.data)
                state = self._state(tick.symbol)
                if state.last_id and stream_id_key(env.id) <= stream_id_key(state.last_id):
                    TICKS_DUPLICATE.inc()
                    continue
                if (bar := state.apply(tick)) is not None:
                    closed.append(bar)
                if (event := self.detector.update(tick)) is not None:
                    anomalies.append(event)
                state.last_id = env.id
                touched.add(tick.symbol)
                applied += 1
                self._latencies.append(max(0.0, now - tick.ts))

        for state in self.states.values():
            if (bar := state.close_if_stale(now)) is not None:
                closed.append(bar)
                touched.add(state.symbol)

        # Durable effects first, then the atomic checkpoint+ack, then best-effort fan-out.
        if closed or history:
            await self._write_candles([*history, *closed])
        if news:
            await self._write_news(news)
        if anomalies:
            await self._write_anomalies(anomalies)
        if not await self._checkpoint_and_ack(touched, ack, applied):
            return  # lost the lease mid-batch: the new owner will redo this work from its own checkpoint

        self._to_broadcast |= touched
        await self._broadcast_quotes(now)
        if applied:
            TICKS_PROCESSED.inc(applied)
            PIPELINE_LATENCY.observe(self._latencies[-1])
            self._recent.append((now, applied))
        await self._publish_stats(now)

    def _state(self, symbol: str) -> SymbolState:
        state = self.states.get(symbol)
        if state is None:
            state = self.states[symbol] = SymbolState(symbol, prev_close=self._ref_prices.get(symbol, 0.0))
        return state

    async def _checkpoint_and_ack(self, touched: set[str], ack: dict[str, list[str]], applied: int) -> bool:
        """Save state and acknowledge the batch in one atomic step. False if this processor was fenced off."""
        if not touched and not ack:
            return True
        total = self.settings.tick_partitions
        involved = {topics.partition_for(s, total) for s in touched} & set(self.partitions)
        async with self.bus.redis.pipeline(transaction=True) as pipe:
            try:
                if self.leases is not None and involved:
                    lease_keys = [self.leases.key(p) for p in sorted(involved)]
                    # WATCH: if any of these keys changes before EXEC, Redis aborts the transaction.
                    await pipe.watch(*lease_keys)
                    owners = await pipe.mget(lease_keys)
                    if any(
                        o is None or (o.decode() if isinstance(o, bytes) else o) != self.leases.owner for o in owners
                    ):
                        raise WatchError("lease is no longer ours")
                pipe.multi()  # type: ignore[no-untyped-call]  # MULTI/EXEC: all or nothing
                for symbol in touched:
                    state = self.states[symbol]
                    pipe.set(self.bus.key(_state_key(symbol)), orjson.dumps(state.snapshot()))
                    pipe.set(self.bus.key(quote_key(symbol)), orjson.dumps(state.quote()))
                for stream, ids in ack.items():
                    pipe.xack(self.bus.key(stream), GROUP, *ids)
                if applied:
                    pipe.hincrby(self.bus.key(STATS_KEY), "ticks_total", applied)
                await pipe.execute()
            except WatchError:
                assert self.leases is not None
                log.warning("processor.fenced", consumer=self.consumer, partitions=sorted(involved))
                for partition in involved:
                    self.leases.held.discard(partition)
                    self._drop(partition)
                return False
        return True

    async def _broadcast_quotes(self, now: float) -> None:
        """Conflation: however many ticks arrive, the UI gets one update per symbol per interval."""
        if not self._to_broadcast or (now - self._last_broadcast) * 1000 < self.settings.quote_conflate_ms:
            return
        quotes = [self.states[s].quote() for s in sorted(self._to_broadcast)]
        self._to_broadcast.clear()
        self._last_broadcast = now
        await self.bus.broadcast(topics.QUOTES, {"type": "quotes", "quotes": quotes})

    # --- Durable writes -----------------------------------------------------

    async def _write_candles(self, bars: list[Bar]) -> None:
        # One statement cannot upsert the same key twice, so keep the last bar per key.
        rows = list({(b.symbol, b.start): _candle_row(b) for b in bars}.values())
        async with self.db.transaction() as session:
            for i in range(0, len(rows), 1000):
                stmt = insert(Candle).values(rows[i : i + 1000])
                stmt = stmt.on_conflict_do_update(
                    index_elements=[Candle.symbol, Candle.interval, Candle.ts],
                    set_={c: stmt.excluded[c] for c in ("open", "high", "low", "close", "volume", "vwap", "trades")},
                )
                await session.execute(stmt)
        CANDLES_WRITTEN.inc(len(rows))

    async def _write_news(self, items: list[dict[str, Any]]) -> None:
        rows = []
        for n in items:
            identity = f"{n.get('source')}|{n.get('symbol')}|{n['ts']}|{n['headline']}"
            rows.append(
                {
                    "symbol": n.get("symbol"),
                    "headline": n["headline"],
                    "summary": n.get("summary") or "",
                    "source": n.get("source") or "unknown",
                    "url": n.get("url"),
                    "sentiment": None if n.get("sentiment") is None else Decimal(str(n["sentiment"])),
                    "is_simulated": bool(n.get("is_simulated")),
                    "published_at": datetime.fromtimestamp(float(n["ts"]), UTC),
                    "dedupe_key": hashlib.sha256(identity.encode()).hexdigest(),
                }
            )
        async with self.db.transaction() as session:
            stmt = insert(News).values(rows).on_conflict_do_nothing(index_elements=[News.dedupe_key])
            inserted = set((await session.execute(stmt.returning(News.dedupe_key))).scalars())
        for row in rows:
            if row["dedupe_key"] in inserted:
                await self.bus.broadcast(
                    topics.PULSE,
                    {
                        "type": "news",
                        "symbol": row["symbol"],
                        "headline": row["headline"],
                        "source": row["source"],
                        "sentiment": None if row["sentiment"] is None else float(row["sentiment"]),
                        "ts": row["published_at"].timestamp(),
                        "is_simulated": row["is_simulated"],
                    },
                )

    async def _write_anomalies(self, events: list[AnomalyEvent]) -> None:
        for event in events:
            anomaly_id = uuid.uuid4()
            async with self.db.transaction() as session:
                session.add(
                    Anomaly(
                        id=anomaly_id,
                        symbol=event.symbol,
                        kind=event.kind,
                        severity=event.severity,
                        details=event.to_wire(),
                        detected_at=datetime.fromtimestamp(event.ts, UTC),
                    )
                )
            payload = {"id": str(anomaly_id), **event.to_wire()}
            await self.bus.publish(topics.ANOMALIES, payload, maxlen=5_000)  # durable: the sentinel agent reads this
            await self.bus.broadcast(topics.PULSE, {"type": "anomaly", **payload})
            ANOMALIES_DETECTED.labels(event.kind).inc()
            log.info("processor.anomaly", symbol=event.symbol, kind=event.kind, move_pct=event.move_pct)

    # --- Operational stats (shown on the System page) ------------------------

    async def _publish_stats(self, now: float) -> None:
        if now - self._last_stats < 1.0:
            return
        self._last_stats = now
        while self._recent and now - self._recent[0][0] > 5.0:
            self._recent.popleft()
        ordered = sorted(self._latencies)
        stats: dict[str, str | int | float] = {
            "ticks_per_second": round(sum(n for _, n in self._recent) / 5.0, 1),
            "latency_p50_ms": round(ordered[len(ordered) // 2] * 1000, 1) if ordered else 0,
            "latency_p95_ms": round(ordered[int(len(ordered) * 0.95)] * 1000, 1) if ordered else 0,
            "symbols": len(self.states),
            "updated_at": now,
        }
        for stream in self.tick_streams:
            lag = await self.bus.lag(stream, GROUP)
            if lag is not None:
                STREAM_LAG.labels(stream, GROUP).set(lag)
                stats[f"lag.{stream}"] = lag
        await self.bus.redis.hset(self.bus.key(STATS_KEY), mapping=stats)  # type: ignore[arg-type]


async def run_processor(bus: RedisBus, db: Database, settings: Settings, consumer: str = "processor-0") -> None:
    leases = PartitionLeases(bus.redis, bus.prefix, group=GROUP, owner=consumer, partitions=settings.tick_partitions)
    processor = StreamProcessor(bus, db, settings, consumer=consumer, leases=leases)
    try:
        await processor.run()
    except asyncio.CancelledError:
        processor.stop()
        raise
