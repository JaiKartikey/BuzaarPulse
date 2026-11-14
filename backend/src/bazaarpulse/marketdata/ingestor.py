"""Ingestor: the only process that talks to the market-data source.

It does no computation. It turns whatever the provider yields into messages on
durable streams and returns to reading. Keeping it this thin means a slow
database or a busy processor can never make us fall behind the feed: the
stream absorbs the burst and the processors catch up.
"""

from __future__ import annotations

import asyncio
import contextlib

from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.logging import get_logger
from bazaarpulse.marketdata.providers import MarketDataProvider
from bazaarpulse.marketdata.types import Batch
from bazaarpulse.metrics import TICKS_INGESTED

log = get_logger(__name__)
BACKFILL_FLAG = "md.backfilled"
_BARS_PER_MESSAGE = 500


class Ingestor:
    def __init__(self, provider: MarketDataProvider, bus: RedisBus, settings: Settings) -> None:
        self.provider = provider
        self.bus = bus
        self.settings = settings

    async def run(self) -> None:
        control = asyncio.create_task(self._control_loop())
        try:
            await self.backfill()
            log.info("ingestor.streaming", provider=self.provider.name)
            async for batch in self.provider.stream():
                await self.publish(batch)
        finally:
            control.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await control
            await self.provider.aclose()

    async def publish(self, batch: Batch) -> None:
        partitions = self.settings.tick_partitions
        await self.bus.publish_many(
            ((topics.ticks(topics.partition_for(t.symbol, partitions)), t.to_wire()) for t in batch.ticks),
            maxlen=self.settings.tick_stream_maxlen,
        )
        for item in batch.news:
            await self.bus.publish(topics.NEWS, item.to_wire(), maxlen=10_000)
        TICKS_INGESTED.labels(self.provider.name).inc(len(batch.ticks))

    async def backfill(self) -> int:
        """Load recent history once, so charts and indicators are not empty on a fresh install."""
        minutes = self.settings.history_backfill_minutes
        # SET NX: with several ingestor replicas, exactly one of them backfills.
        if minutes <= 0 or not await self.bus.redis.set(self.bus.key(BACKFILL_FLAG), "1", nx=True):
            return 0
        bars = await self.provider.backfill(minutes)
        for i in range(0, len(bars), _BARS_PER_MESSAGE):
            chunk = [b.to_dict() for b in bars[i : i + _BARS_PER_MESSAGE]]
            await self.bus.publish(topics.BARS, {"bars": chunk})
        log.info("ingestor.backfilled", bars=len(bars), minutes=minutes)
        return len(bars)

    async def _control_loop(self) -> None:
        async for _, command in self.bus.subscribe([topics.SIM_CONTROL]):
            try:
                await self.provider.control(command)
            except Exception:  # a bad operator command must not take the feed down
                log.exception("ingestor.control_failed", command=command)
