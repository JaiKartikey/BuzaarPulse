"""An agent desk for tests: the trading rig plus market history, news, tools and a worker on the offline model."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any

from bazaarpulse.agents.llm import LLMGateway, LLMProvider
from bazaarpulse.agents.llm.mock_provider import MockProvider
from bazaarpulse.agents.proposals import ProposalService
from bazaarpulse.agents.service import AgentService, AgentWorker
from bazaarpulse.agents.tools import Toolbox
from bazaarpulse.bus import topics
from bazaarpulse.db.models import AgentRun
from bazaarpulse.marketdata.instruments import NSE_UNIVERSE
from bazaarpulse.marketdata.processor import StreamProcessor
from bazaarpulse.marketdata.providers.simulator import SimulatedMarket
from tests.trading_helpers import Rig

SYMBOLS = {s.symbol: s.name for s in NSE_UNIVERSE}


@dataclass
class Desk:
    rig: Rig
    proposals: ProposalService
    toolbox: Toolbox
    gateway: LLMGateway
    worker: AgentWorker
    agents: AgentService

    async def ask(
        self, user_id: uuid.UUID | None, message: str, *, kind: str = "copilot", symbol: str | None = None
    ) -> AgentRun:
        """Queue a run and execute it in-process, the way a worker would after reading the job."""
        run = await self.agents.create_run(kind=kind, user_id=user_id, input={"message": message}, symbol=symbol)
        await self.worker.execute(run.id)
        finished = await self.agents.get_run(run.id, user_id)
        assert finished is not None
        return finished

    async def steps(self, run: AgentRun) -> list[dict[str, Any]]:
        return await self.agents.steps(run.id)


async def build_desk(rig: Rig, providers: list[LLMProvider] | None = None, **gateway_kw: Any) -> Desk:
    """Seed an hour of candles, live quotes and one headline, then wire the agent stack on top of the rig."""
    market = SimulatedMarket(NSE_UNIVERSE, seed=21, shocks_per_hour=0)
    bars = market.history(time.time(), minutes=60)
    processor = StreamProcessor(rig.bus, rig.db, rig.settings)
    await processor._write_candles(bars)
    await processor._write_news(
        [
            {
                "symbol": "TCS",
                "headline": "Tata Consultancy Services rallies 2.1% on a brokerage upgrade",
                "summary": "Analysts raised their target price, citing improving demand.",
                "source": "SimWire",
                "sentiment": 0.7,
                "is_simulated": True,
                "ts": time.time() - 120,
            }
        ]
    )
    for symbol, price in market.prices.items():
        await rig.price(symbol, round(price, 2))

    proposals = ProposalService(rig.db, rig.orders, rig.settings)
    toolbox = Toolbox(store=rig.store, db=rig.db, ledger=rig.ledger, proposals=proposals, settings=rig.settings)
    gateway = LLMGateway(
        providers or [MockProvider(SYMBOLS)],
        redis=rig.bus.redis,
        prefix=rig.bus.prefix,
        prices=rig.settings.llm_prices,
        daily_token_budget=gateway_kw.pop("daily_token_budget", rig.settings.agent_daily_token_budget),
        retries=gateway_kw.pop("retries", 0),
    )
    worker = AgentWorker(rig.db, rig.bus, rig.store, gateway, toolbox, rig.settings)
    return Desk(rig, proposals, toolbox, gateway, worker, AgentService(rig.db, rig.bus))


async def stream_events(desk: Desk, run: AgentRun) -> list[dict[str, Any]]:
    return [e.data for e in await desk.rig.bus.read(topics.agent_run(run.id), count=500)]
