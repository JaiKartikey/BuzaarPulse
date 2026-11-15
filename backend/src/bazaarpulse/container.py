"""Composition root: every long-lived object is built here, once, and passed down.

The API process and each worker process call ``build_container`` and then use
only the parts they need. Nothing else in the codebase constructs a service or
reads a global, which is what lets tests assemble the same graph against a
test database.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import orjson
from redis.asyncio import Redis

from bazaarpulse.agents.llm import LLMGateway, build_gateway
from bazaarpulse.agents.proposals import ProposalService
from bazaarpulse.agents.service import AgentService, AgentWorker
from bazaarpulse.agents.tools import Toolbox
from bazaarpulse.bus import RedisBus
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.seed import seed_instruments
from bazaarpulse.logging import get_logger
from bazaarpulse.marketdata.ingestor import Ingestor
from bazaarpulse.marketdata.instruments import universe_for
from bazaarpulse.marketdata.processor import quote_key, run_processor
from bazaarpulse.marketdata.providers import build_provider
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.trading.executor import Executor
from bazaarpulse.trading.ledger import Ledger
from bazaarpulse.trading.orders import OrderService

log = get_logger(__name__)


@dataclass
class Container:
    settings: Settings
    db: Database
    redis: Redis
    bus: RedisBus
    store: MarketStore
    ledger: Ledger
    orders: OrderService
    proposals: ProposalService
    toolbox: Toolbox
    gateway: LLMGateway
    agents: AgentService
    instance: str  # unique per process; names this process's stream consumers and leases

    async def aclose(self) -> None:
        await self.gateway.aclose()
        await self.redis.aclose()
        await self.db.dispose()


async def build_container(settings: Settings) -> Container:
    db = Database(settings.database_url, pool_size=settings.db_pool_size)
    redis = Redis.from_url(settings.redis_url)
    bus = RedisBus(redis, settings.redis_prefix)
    store = MarketStore(bus, db)
    ledger = Ledger(settings.currency)
    orders = OrderService(db, bus, store, ledger, settings)
    proposals = ProposalService(db, orders, settings)
    toolbox = Toolbox(store=store, db=db, ledger=ledger, proposals=proposals, settings=settings)
    universe = {spec.symbol: spec.name for spec in universe_for(settings.market_provider)}
    return Container(
        settings=settings,
        db=db,
        redis=redis,
        bus=bus,
        store=store,
        ledger=ledger,
        orders=orders,
        proposals=proposals,
        toolbox=toolbox,
        gateway=build_gateway(settings, redis, universe),
        agents=AgentService(db, bus),
        instance=socket.gethostname(),
    )


async def seed(container: Container) -> None:
    async with container.db.transaction() as session:
        await seed_instruments(session, container.settings.market_provider)


# --- Worker entry points ------------------------------------------------------


async def run_ingestor(c: Container) -> None:
    # A restarted simulator continues from the last published prices instead of jumping back to its start.
    symbols = [spec.symbol for spec in universe_for(c.settings.market_provider)]
    raw = await c.redis.mget([c.bus.key(quote_key(s)) for s in symbols])
    quotes = {s: orjson.loads(r) for s, r in zip(symbols, raw, strict=True) if r}
    provider = build_provider(
        c.settings,
        start_prices={s: q["last"] for s, q in quotes.items()} or None,
        prev_closes={s: q["prev_close"] for s, q in quotes.items()} or None,
    )
    await Ingestor(provider, c.bus, c.settings).run()


async def run_stream_processor(c: Container) -> None:
    await run_processor(c.bus, c.db, c.settings, consumer=f"processor-{c.instance}")


async def run_executor(c: Container) -> None:
    await Executor(c.db, c.bus, c.store, c.ledger, c.settings, consumer=f"executor-{c.instance}").run()


async def run_agent_worker(c: Container) -> None:
    await AgentWorker(c.db, c.bus, c.store, c.gateway, c.toolbox, c.settings, consumer=f"agent-{c.instance}").run()


WORKERS: dict[str, Callable[[Container], Awaitable[None]]] = {
    "ingestor": run_ingestor,
    "processor": run_stream_processor,
    "executor": run_executor,
    "agents": run_agent_worker,
}


async def supervise(name: str, c: Container) -> None:
    """Run a worker forever, restarting it with capped backoff if it crashes.

    A background loop that dies quietly is the worst failure mode: the process
    looks healthy while a whole feature has stopped. Crashes are logged loudly
    and the loop is brought back.
    """
    delay = 1.0
    while True:
        started = asyncio.get_running_loop().time()
        try:
            await WORKERS[name](c)
            log.warning("worker.exited", worker=name)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("worker.crashed", worker=name, restart_in=delay)
        if asyncio.get_running_loop().time() - started > 60:
            delay = 1.0  # it ran fine for a while: treat this as a fresh failure
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30.0)
