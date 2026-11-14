"""Partition leases: which processor owns which tick partition, right now.

A stream processor keeps per-symbol state in memory, so two processors must
never work on the same partition at once. Instead of a fixed assignment (which
needs a human when a pod dies), each partition has a lease: a Redis key holding
the owner's name, with a short expiry the owner keeps renewing.

* **Failover.** If an owner dies it stops renewing; the key expires and another
  processor takes the partition over, resuming from the last checkpoint.
* **Balance.** Each processor counts how many are alive and holds at most its
  fair share, releasing extras so a newcomer gets work.
* **Fencing.** A processor frozen long enough to lose its lease may wake up
  believing it still owns the partition. The checkpoint transaction WATCHes the
  lease key, so a stale owner's write is rejected by Redis instead of
  overwriting the new owner's state.

This is the job Kafka's group coordinator does for its consumers.
"""

from __future__ import annotations

import math

from redis.asyncio import Redis

# Compare-and-act must be atomic, so it runs inside Redis as a script.
_EXTEND = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end"
)
_RELEASE = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"


class PartitionLeases:
    def __init__(
        self, redis: Redis, prefix: str, *, group: str, owner: str, partitions: int, ttl_seconds: float = 15.0
    ) -> None:
        self.redis = redis
        self.owner = owner
        self.partitions = partitions
        self.ttl_ms = int(ttl_seconds * 1000)
        self.held: set[int] = set()
        self._base = f"{prefix}lease.{group}"

    def key(self, partition: int) -> str:
        return f"{self._base}.p{partition}"

    def _alive_key(self, owner: str) -> str:
        return f"{self._base}.alive.{owner}"

    async def _alive(self) -> int:
        await self.redis.set(self._alive_key(self.owner), "1", px=self.ttl_ms)
        return len([k async for k in self.redis.scan_iter(match=self._alive_key("*"), count=200)])

    async def refresh(self) -> tuple[set[int], set[int]]:
        """Renew, rebalance and acquire. Returns ``(newly acquired, lost)`` partitions."""
        fair_share = math.ceil(self.partitions / max(1, await self._alive()))
        lost: set[int] = set()
        for partition in sorted(self.held):
            if not await self.redis.eval(_EXTEND, 1, self.key(partition), self.owner, self.ttl_ms):
                lost.add(partition)
        self.held -= lost

        # Holding more than a fair share: hand the extras back (highest numbers first, for determinism).
        for partition in sorted(self.held, reverse=True)[: max(0, len(self.held) - fair_share)]:
            await self.redis.eval(_RELEASE, 1, self.key(partition), self.owner)
            self.held.discard(partition)
            lost.add(partition)

        acquired: set[int] = set()
        for partition in range(self.partitions):
            if len(self.held) >= fair_share:
                break
            if partition not in self.held and await self.redis.set(
                self.key(partition), self.owner, nx=True, px=self.ttl_ms
            ):
                self.held.add(partition)
                acquired.add(partition)
        return acquired - lost, lost - acquired

    async def release_all(self) -> None:
        """Clean shutdown: give partitions up at once instead of making others wait for the expiry."""
        for partition in list(self.held):
            await self.redis.eval(_RELEASE, 1, self.key(partition), self.owner)
        self.held.clear()
        await self.redis.delete(self._alive_key(self.owner))
