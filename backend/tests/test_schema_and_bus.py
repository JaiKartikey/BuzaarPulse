from __future__ import annotations

import asyncio

from sqlalchemy import inspect, text

from bazaarpulse.bus import RedisBus
from bazaarpulse.bus.redis_bus import stream_id_key
from bazaarpulse.bus.topics import partition_for
from bazaarpulse.db import Base, Database


async def test_orm_models_match_migrated_schema(db: Database) -> None:
    """The migration is hand-written SQL, so guard against the models drifting away from it."""

    def columns(sync_conn) -> dict[str, set[str]]:  # type: ignore[no-untyped-def]
        insp = inspect(sync_conn)
        return {t: {c["name"] for c in insp.get_columns(t)} for t in insp.get_table_names()}

    async with db.engine.connect() as conn:
        actual = await conn.run_sync(columns)

    for table in Base.metadata.sorted_tables:
        assert table.name in actual, f"model {table.name} has no table"
        missing = {c.name for c in table.columns} - actual[table.name]
        assert not missing, f"{table.name}: model columns missing in database: {missing}"
        unmapped = actual[table.name] - {c.name for c in table.columns} - {"tsv"}
        assert not unmapped, f"{table.name}: database columns missing in model: {unmapped}"


async def test_candles_are_partitioned_by_month(db: Database) -> None:
    async with db.engine.connect() as conn:
        partitions = (
            (
                await conn.execute(
                    text("SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'candles'::regclass")
                )
            )
            .scalars()
            .all()
        )
        await conn.execute(
            text("INSERT INTO candles VALUES ('TCS', '1m', now(), 1, 2, 1, 2, 10, 1.5, 3) ON CONFLICT DO NOTHING")
        )
        landed_in = (await conn.execute(text("SELECT tableoid::regclass::text FROM candles LIMIT 1"))).scalar_one()
        await conn.rollback()
    assert "candles_default" in partitions and len(partitions) >= 4
    assert landed_in != "candles_default", "current rows should land in this month's partition"


async def test_stream_consumer_group_delivers_once_and_redelivers_unacked(bus: RedisBus) -> None:
    await bus.ensure_group("jobs", "workers", start_id="0")
    await bus.ensure_group("jobs", "workers", start_id="0")  # idempotent
    for i in range(3):
        await bus.publish("jobs", {"n": i})

    first = await bus.consume(["jobs"], "workers", "w1", block_ms=None)
    assert [e.data["n"] for e in first] == [0, 1, 2]
    assert await bus.consume(["jobs"], "workers", "w2", block_ms=None) == []  # nothing new for the group

    await bus.ack("jobs", "workers", [first[0].id])
    # A restarted w1 re-reads what it was handed but never acknowledged.
    pending = await bus.consume(["jobs"], "workers", "w1", block_ms=None, pending=True)
    assert [e.data["n"] for e in pending] == [1, 2]
    # ... and if w1 never comes back, another worker can take its messages over.
    claimed = await bus.claim_stale("jobs", "workers", "w2", min_idle_ms=0)
    assert [e.data["n"] for e in claimed] == [1, 2]


async def test_plain_read_replays_from_any_id(bus: RedisBus) -> None:
    ids = [await bus.publish("log", {"n": i}) for i in range(4)]
    assert [e.data["n"] for e in await bus.read("log")] == [0, 1, 2, 3]
    assert [e.data["n"] for e in await bus.read("log", last_id=ids[1])] == [2, 3]
    assert sorted(ids, key=stream_id_key) == ids


async def test_pubsub_reaches_current_subscribers_only(bus: RedisBus) -> None:
    await bus.broadcast("quotes", {"lost": True})  # nobody is listening yet
    received: list[dict[str, object]] = []

    async def listen() -> None:
        async for channel, data in bus.subscribe(["quotes"], patterns=["user.*"]):
            received.append({"channel": channel, **data})
            if len(received) == 2:
                return

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.05)
    await bus.broadcast("quotes", {"n": 1})
    await bus.broadcast("user.42", {"n": 2})
    await asyncio.wait_for(task, timeout=2)
    assert received == [{"channel": "quotes", "n": 1}, {"channel": "user.42", "n": 2}]


def test_partition_mapping_is_stable_and_in_range() -> None:
    assert partition_for("RELIANCE", 4) == partition_for("RELIANCE", 4)
    assert {partition_for(s, 4) for s in ("RELIANCE", "TCS", "INFY", "SBIN", "ITC", "LT")} <= {0, 1, 2, 3}
    assert partition_for("TCS", 1) == 0
