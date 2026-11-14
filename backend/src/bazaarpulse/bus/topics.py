"""Names of every stream and channel, in one place.

Two kinds of transport, chosen per message type:

* **Streams** are durable logs read through consumer groups. A message stays
  until a consumer acknowledges it, so a crashed worker picks up where it
  stopped. Used when losing a message would be a bug (ticks, jobs, anomalies).
* **Channels** are fire-and-forget pub/sub. Whoever is listening right now gets
  the message; nobody else ever will. Used for UI fan-out, where the next
  update replaces the last one anyway.
"""

from __future__ import annotations

import zlib

# --- Streams ----------------------------------------------------------------
NEWS = "md.news"
BARS = "md.bars"  # historical 1-minute bars from a provider backfill
ANOMALIES = "md.anomalies"
AGENT_JOBS = "agent.jobs"


def ticks(partition: int) -> str:
    return f"md.ticks.{partition}"


def partition_for(symbol: str, partitions: int) -> int:
    """Stable symbol -> partition mapping, so one symbol's ticks stay in order on one stream.

    crc32 rather than ``hash()``: Python salts ``hash()`` per process, and every
    producer and consumer has to agree on the answer.
    """
    return zlib.crc32(symbol.encode()) % partitions


def agent_run(run_id: object) -> str:
    """Per-run event log, replayable from any id (this is what the SSE endpoint tails)."""
    return f"agent.run.{run_id}"


# --- Channels ---------------------------------------------------------------
QUOTES = "quotes"  # conflated quote batches for the WebSocket gateways
PULSE = "pulse"  # anomalies and AI briefs, shown to everyone
SIM_CONTROL = "sim.control"  # API -> simulator: inject a shock
ORDER_WAKE = "orders.wake"  # API -> executor: a new order is waiting


def user(user_id: object) -> str:
    return f"user.{user_id}"
