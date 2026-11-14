"""A thin event bus over Redis Streams (durable) and pub/sub (ephemeral).

Why Redis and not Kafka: at this scale one Redis gives consumer groups,
acknowledgements, replay and pub/sub with a single dependency that is already
needed for quotes and rate limits. The interface below is deliberately the
subset Kafka also offers (publish, consume in a group, commit), so swapping
the transport later would not touch the workers.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from typing import Any

import orjson
from redis.asyncio import Redis
from redis.exceptions import ResponseError

_FIELD = b"d"


@dataclass(frozen=True, slots=True)
class Envelope:
    id: str  # stream id, "<milliseconds>-<sequence>"
    stream: str  # logical name, without the key prefix
    data: dict[str, Any]


def stream_id_key(stream_id: str) -> tuple[int, int]:
    """Stream ids compare as (milliseconds, sequence), not as strings."""
    ms, _, seq = stream_id.partition("-")
    return int(ms), int(seq or 0)


class RedisBus:
    def __init__(self, redis: Redis, prefix: str = "bp:") -> None:
        self.redis = redis
        self.prefix = prefix

    def key(self, name: str) -> str:
        return f"{self.prefix}{name}"

    def _logical(self, key: bytes | str) -> str:
        text = key.decode() if isinstance(key, bytes) else key
        return text.removeprefix(self.prefix)

    # --- Streams ------------------------------------------------------------

    async def publish(self, stream: str, data: dict[str, Any], *, maxlen: int | None = None) -> str:
        # approximate=True lets Redis trim whole macro-nodes: far cheaper than an exact cap.
        raw = await self.redis.xadd(self.key(stream), {_FIELD: orjson.dumps(data)}, maxlen=maxlen, approximate=True)
        return raw.decode() if isinstance(raw, bytes) else str(raw)

    async def publish_many(self, items: Iterable[tuple[str, dict[str, Any]]], *, maxlen: int | None = None) -> int:
        """Append many messages in one round trip."""
        pipe = self.redis.pipeline(transaction=False)
        count = 0
        for stream, data in items:
            pipe.xadd(self.key(stream), {_FIELD: orjson.dumps(data)}, maxlen=maxlen, approximate=True)
            count += 1
        if count:
            await pipe.execute()
        return count

    async def ensure_group(self, stream: str, group: str, *, start_id: str = "$") -> None:
        """Create the consumer group if missing. ``$`` = only new messages, ``0`` = from the beginning."""
        try:
            await self.redis.xgroup_create(self.key(stream), group, id=start_id, mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def consume(
        self,
        streams: list[str],
        group: str,
        consumer: str,
        *,
        count: int = 200,
        block_ms: int | None = 1000,
        pending: bool = False,
    ) -> list[Envelope]:
        """Read as ``consumer`` in ``group``.

        ``pending=True`` re-reads messages this consumer was given but never
        acknowledged (what a restarted worker does first); otherwise it reads
        messages no consumer in the group has seen.
        """
        cursor = "0" if pending else ">"
        resp = await self.redis.xreadgroup(
            group, consumer, dict.fromkeys((self.key(s) for s in streams), cursor), count=count, block=block_ms
        )
        return self._parse(resp)

    async def ack(self, stream: str, group: str, ids: list[str]) -> None:
        if ids:
            await self.redis.xack(self.key(stream), group, *ids)

    async def claim_stale(
        self, stream: str, group: str, consumer: str, *, min_idle_ms: int, count: int = 50
    ) -> list[Envelope]:
        """Take over messages another consumer received but has not acknowledged for a while (it died)."""
        resp = await self.redis.xautoclaim(self.key(stream), group, consumer, min_idle_time=min_idle_ms, count=count)
        return self._parse([(self.key(stream), resp[1])])

    async def read(
        self, stream: str, *, last_id: str = "0", count: int = 100, block_ms: int | None = None
    ) -> list[Envelope]:
        """Plain tail of a stream from ``last_id``, no group: every reader sees everything."""
        resp = await self.redis.xread({self.key(stream): last_id}, count=count, block=block_ms)
        return self._parse(resp)

    async def lag(self, stream: str, group: str) -> int | None:
        """Messages not yet delivered to ``group``; None if the stream or group does not exist."""
        try:
            groups = await self.redis.xinfo_groups(self.key(stream))
        except ResponseError:
            return None
        for g in groups:
            name = g.get("name")
            if (name.decode() if isinstance(name, bytes) else name) == group:
                lag = g.get("lag")
                return int(lag) if lag is not None else None
        return None

    def _parse(self, resp: Any) -> list[Envelope]:
        out: list[Envelope] = []
        for key, entries in resp or []:
            stream = self._logical(key)
            for entry_id, fields in entries:
                if not fields:  # the entry was trimmed while it sat in a pending list
                    continue
                eid = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
                out.append(Envelope(eid, stream, orjson.loads(fields[_FIELD])))
        return out

    # --- Pub/sub ------------------------------------------------------------

    async def broadcast(self, channel: str, data: dict[str, Any]) -> None:
        await self.redis.publish(self.key(channel), orjson.dumps(data))

    async def subscribe(
        self, channels: Iterable[str] = (), *, patterns: Iterable[str] = ()
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Yield ``(channel, data)`` forever. Messages published while nobody listens are gone."""
        pubsub = self.redis.pubsub()
        try:
            chans = [self.key(c) for c in channels]
            pats = [self.key(p) for p in patterns]
            if chans:
                await pubsub.subscribe(*chans)
            if pats:
                await pubsub.psubscribe(*pats)
            async for message in pubsub.listen():
                if message["type"] in ("message", "pmessage"):
                    yield self._logical(message["channel"]), orjson.loads(message["data"])
        finally:
            await pubsub.aclose()  # type: ignore[no-untyped-call]
