"""Partition leases: balance, failover, and fencing a processor that lost its lease."""

from __future__ import annotations

import asyncio
import time

from bazaarpulse.bus import RedisBus
from bazaarpulse.bus.leases import PartitionLeases
from bazaarpulse.bus.topics import partition_for, ticks
from bazaarpulse.db import Database
from bazaarpulse.marketdata.ingestor import Ingestor
from bazaarpulse.marketdata.instruments import NSE_UNIVERSE
from bazaarpulse.marketdata.processor import GROUP, StreamProcessor
from bazaarpulse.marketdata.providers.simulator import SimulatorProvider
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.marketdata.types import Batch, Tick
from tests.conftest import make_settings

PARTITIONS = 4


def leases(bus: RedisBus, owner: str, ttl: float = 15.0) -> PartitionLeases:
    return PartitionLeases(bus.redis, bus.prefix, group=GROUP, owner=owner, partitions=PARTITIONS, ttl_seconds=ttl)


async def test_partitions_are_shared_fairly_and_never_owned_twice(bus: RedisBus) -> None:
    a, b = leases(bus, "a"), leases(bus, "b")
    assert await a.refresh() == ({0, 1, 2, 3}, set()), "alone, a processor takes everything"

    # A second processor appears. It can take nothing until the first notices and hands half back.
    assert await b.refresh() == (set(), set())
    acquired, lost = await a.refresh()
    assert (acquired, len(lost)) == (set(), 2) and len(a.held) == 2
    assert (await b.refresh())[0] == lost
    assert a.held | b.held == {0, 1, 2, 3} and not a.held & b.held

    # Stable from here on: renewing changes nothing.
    assert await a.refresh() == (set(), set()) and await b.refresh() == (set(), set())


async def test_a_dead_processors_partitions_are_taken_over_after_its_lease_expires(bus: RedisBus) -> None:
    a, b = leases(bus, "a", ttl=0.3), leases(bus, "b", ttl=0.3)
    await a.refresh()
    await b.refresh()
    await a.refresh()
    await b.refresh()
    assert len(a.held) == len(b.held) == 2

    await asyncio.sleep(0.2)
    await b.refresh()  # b keeps renewing; a has died and does not
    await asyncio.sleep(0.2)
    acquired, _ = await b.refresh()
    assert acquired == a.held and b.held == {0, 1, 2, 3}

    # A clean shutdown does not make anyone wait for the expiry.
    c = leases(bus, "c", ttl=0.3)
    await b.release_all()
    assert (await c.refresh())[0] == {0, 1, 2, 3}


async def test_takeover_resumes_from_checkpoint_and_finishes_the_dead_owners_batch(bus: RedisBus, db: Database) -> None:
    settings = make_settings(redis_prefix=bus.prefix, tick_partitions=PARTITIONS)
    ingestor = Ingestor(SimulatorProvider(NSE_UNIVERSE, seed=1, shocks_per_hour=0), bus, settings)
    store = MarketStore(bus, db)
    partition = partition_for("TCS", PARTITIONS)
    t0 = (int(time.time()) // 300 - 2) * 300.0

    first = StreamProcessor(bus, db, settings, consumer="first", leases=leases(bus, "first", ttl=0.3))
    await first.start()
    await first._sync_leases()
    assert partition in first.partitions

    await ingestor.publish(Batch([Tick("TCS", 4100.0, 10, t0 + 1), Tick("TCS", 4110.0, 10, t0 + 2)]))
    await first.handle(await bus.consume(first.streams, GROUP, "first", block_ms=None))
    # It is handed two more ticks ... and dies before processing or acknowledging them.
    await ingestor.publish(Batch([Tick("TCS", 4090.0, 5, t0 + 3), Tick("TCS", 4120.0, 5, t0 + 4)]))
    assert len(await bus.consume(first.streams, GROUP, "first", block_ms=None)) == 2

    second = StreamProcessor(bus, db, settings, consumer="second", leases=leases(bus, "second", ttl=0.3))
    await second.start()
    await second._sync_leases()
    assert partition not in second.partitions, "the lease is still held; nothing to take yet"
    await asyncio.sleep(0.4)
    await second._sync_leases()
    assert partition in second.partitions

    quote = await store.quote("TCS")
    assert quote is not None
    # All four ticks, each exactly once: two from the checkpoint, two adopted from the dead owner.
    assert (quote["volume"], quote["high"], quote["low"], quote["last"]) == (30, 4120.0, 4090.0, 4120.0)
    assert await bus.claim_stale(ticks(partition), GROUP, "anyone", min_idle_ms=0) == []


async def test_a_processor_that_lost_its_lease_cannot_overwrite_the_new_owners_state(
    bus: RedisBus, db: Database
) -> None:
    settings = make_settings(redis_prefix=bus.prefix, tick_partitions=PARTITIONS)
    ingestor = Ingestor(SimulatorProvider(NSE_UNIVERSE, seed=1, shocks_per_hour=0), bus, settings)
    store = MarketStore(bus, db)
    partition = partition_for("INFY", PARTITIONS)
    t0 = (int(time.time()) // 300 - 2) * 300.0

    zombie = StreamProcessor(bus, db, settings, consumer="zombie", leases=leases(bus, "zombie"))
    await zombie.start()
    await zombie._sync_leases()
    await ingestor.publish(Batch([Tick("INFY", 1850.0, 10, t0 + 1)]))
    await zombie.handle(await bus.consume(zombie.streams, GROUP, "zombie", block_ms=None))

    # The zombie freezes (a long GC pause, a stalled VM). Its lease passes to someone else meanwhile.
    await ingestor.publish(Batch([Tick("INFY", 1900.0, 7, t0 + 2)]))
    stale_batch = await bus.consume(zombie.streams, GROUP, "zombie", block_ms=None)
    await bus.redis.set(zombie.leases.key(partition), "successor")  # type: ignore[union-attr]

    # It wakes up and tries to finish its batch. Redis refuses the checkpoint; nothing is acknowledged.
    await zombie.handle(stale_batch)
    assert partition not in zombie.partitions and "INFY" not in zombie.states
    assert (await store.quote("INFY"))["volume"] == 10, "the stale write did not land"  # type: ignore[index]

    await bus.redis.delete(zombie.leases.key(partition))  # type: ignore[union-attr]
    successor = StreamProcessor(bus, db, settings, consumer="successor", leases=leases(bus, "successor"))
    await successor.start()
    await successor._sync_leases()
    quote = await store.quote("INFY")
    assert quote is not None and (quote["volume"], quote["last"]) == (17, 1900.0), (
        "the tick was processed once, by the owner"
    )
