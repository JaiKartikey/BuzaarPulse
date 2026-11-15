"""Agent runs as durable background jobs.

The API never runs a model inside a request. It writes a ``queued`` run row,
drops a job on a stream and returns at once; a worker picks the job up. That
keeps request latency flat, lets runs outlive a browser tab, and means agent
capacity scales by adding workers, independent of the API.

Three kinds of run share one runtime:

* **copilot**   one agent with every tool, answering a user's question.
* **war_room**  three specialists analyse a stock in parallel, a lead synthesises.
* **sentinel**  no user at all: an anomaly triggers a short brief for the live feed.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, update

from bazaarpulse.agents import prompts
from bazaarpulse.agents.llm import LLMGateway
from bazaarpulse.agents.runtime import AgentRuntime, RunRecorder
from bazaarpulse.agents.tools import Toolbox, ToolContext
from bazaarpulse.bus import Envelope, RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import AgentRun, AgentStep, Anomaly
from bazaarpulse.logging import get_logger
from bazaarpulse.marketdata.store import MarketStore
from bazaarpulse.metrics import AGENT_RUN_SECONDS, AGENT_RUNS

log = get_logger(__name__)
GROUP = "agents"
SENTINEL_GROUP = "sentinel"
_SIGNAL = re.compile(r"SIGNAL:\s*(bullish|bearish|neutral)\s*CONFIDENCE:\s*(low|medium|high)", re.I)

READ_TOOLS = ["get_quote", "get_candles", "get_indicators", "search_news", "get_market_overview", "run_backtest"]
COPILOT_TOOLS = [*READ_TOOLS, "get_portfolio", "propose_trade_plan"]
SPECIALISTS: list[tuple[str, str, list[str]]] = [
    ("technical", prompts.TECHNICAL, ["get_quote", "get_candles", "get_indicators", "run_backtest"]),
    ("news", prompts.NEWS, ["search_news", "get_market_overview"]),
    ("risk", prompts.RISK, ["get_quote", "get_indicators", "get_market_overview", "get_portfolio"]),
]
SENTINEL_TOOLS = ["get_quote", "get_indicators", "search_news", "get_market_overview"]


def parse_signal(text: str) -> dict[str, str | None]:
    match = _SIGNAL.search(text)
    return (
        {"signal": match.group(1).lower(), "confidence": match.group(2).lower()}
        if match
        else {"signal": None, "confidence": None}
    )


def serialize_run(run: AgentRun) -> dict[str, Any]:
    return {
        "id": str(run.id),
        "kind": run.kind,
        "status": run.status,
        "symbol": run.symbol,
        "input": run.input,
        "output": run.output,
        "provider": run.provider,
        "model": run.model,
        "input_tokens": run.input_tokens,
        "output_tokens": run.output_tokens,
        "cost_usd": float(run.cost_usd),
        "error": run.error,
        "created_at": run.created_at.timestamp(),
        "finished_at": run.finished_at.timestamp() if run.finished_at else None,
    }


class AgentService:
    """What the API uses: enqueue runs and read them back."""

    def __init__(self, db: Database, bus: RedisBus) -> None:
        self.db = db
        self.bus = bus

    async def create_run(
        self, *, kind: str, user_id: uuid.UUID | None, input: dict[str, Any], symbol: str | None = None
    ) -> AgentRun:
        run = AgentRun(
            id=uuid.uuid4(),
            user_id=user_id,
            kind=kind,
            status="queued",
            symbol=symbol,
            input=input,
            created_at=datetime.now(UTC),
        )
        async with self.db.transaction() as session:
            session.add(run)
        await self.bus.publish(topics.AGENT_JOBS, {"run_id": str(run.id)}, maxlen=10_000)
        return run

    async def get_run(self, run_id: uuid.UUID, user_id: uuid.UUID | None = None) -> AgentRun | None:
        async with self.db.session() as session:
            run = await session.get(AgentRun, run_id)
        # Sentinel runs belong to nobody and are public; a user's runs are visible to that user only.
        if run is None or (run.user_id is not None and run.user_id != user_id):
            return None
        return run

    async def list_runs(self, user_id: uuid.UUID, limit: int = 30) -> list[AgentRun]:
        async with self.db.session() as session:
            rows = await session.execute(
                select(AgentRun).where(AgentRun.user_id == user_id).order_by(AgentRun.created_at.desc()).limit(limit)
            )
            return list(rows.scalars())

    async def steps(self, run_id: uuid.UUID, after_seq: int = 0) -> list[dict[str, Any]]:
        async with self.db.session() as session:
            rows = await session.execute(
                select(AgentStep).where(AgentStep.run_id == run_id, AgentStep.seq > after_seq).order_by(AgentStep.seq)
            )
            return [
                {"seq": s.seq, "agent": s.agent, "type": s.type, "payload": s.payload, "ts": s.created_at.timestamp()}
                for s in rows.scalars()
            ]


class AgentWorker:
    def __init__(
        self,
        db: Database,
        bus: RedisBus,
        store: MarketStore,
        gateway: LLMGateway,
        toolbox: Toolbox,
        settings: Settings,
        *,
        consumer: str = "agent-0",
    ) -> None:
        self.db = db
        self.bus = bus
        self.store = store
        self.settings = settings
        self.consumer = consumer
        self.runtime = AgentRuntime(
            gateway, toolbox, max_steps=settings.agent_max_steps, max_output_tokens=settings.llm_max_output_tokens
        )
        self.service = AgentService(db, bus)
        self._slots = asyncio.Semaphore(settings.agent_concurrency)
        self._tasks: set[asyncio.Task[None]] = set()

    # --- Job loop -----------------------------------------------------------

    async def run(self) -> None:
        await self.bus.ensure_group(topics.AGENT_JOBS, GROUP, start_id="0")
        await self.bus.ensure_group(topics.ANOMALIES, SENTINEL_GROUP, start_id="$")
        sentinel = asyncio.create_task(self._sentinel_loop(), name="agents.sentinel")
        last_claim = 0.0
        try:
            while True:
                jobs = await self.bus.consume([topics.AGENT_JOBS], GROUP, self.consumer, count=8, block_ms=1000)
                if time.monotonic() - last_claim > 30:
                    # Jobs a dead worker took and never finished: take them over after five idle minutes.
                    jobs += await self.bus.claim_stale(topics.AGENT_JOBS, GROUP, self.consumer, min_idle_ms=300_000)
                    last_claim = time.monotonic()
                for job in jobs:
                    await self._slots.acquire()  # backpressure: stop reading when every slot is busy
                    task = asyncio.create_task(self._handle(job))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)
        finally:
            sentinel.cancel()
            for task in list(self._tasks):
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sentinel

    async def _handle(self, job: Envelope) -> None:
        try:
            await self.execute(uuid.UUID(job.data["run_id"]))
        except Exception:
            log.exception("agent.job_crashed", job=job.data)
        finally:
            # Acknowledge even on failure: the run row records the error, and retrying a
            # failed LLM run automatically would spend money without anyone asking.
            await self.bus.ack(topics.AGENT_JOBS, GROUP, [job.id])
            self._slots.release()

    # --- One run ------------------------------------------------------------

    async def execute(self, run_id: uuid.UUID) -> AgentRun | None:
        now = datetime.now(UTC)
        async with self.db.transaction() as session:
            # Claim the run. A redelivered job for a run that already started is ignored.
            run = (
                await session.execute(
                    update(AgentRun)
                    .where(AgentRun.id == run_id, AgentRun.status == "queued")
                    .values(status="running", started_at=now, attempts=AgentRun.attempts + 1)
                    .returning(AgentRun)
                )
            ).scalar_one_or_none()
        if run is None:
            return None

        recorder = RunRecorder(self.db, self.bus, run.id)
        ctx = ToolContext(run_id=run.id, user_id=run.user_id)
        started = time.perf_counter()
        status, output, error = "completed", None, None
        try:
            await recorder.emit("system", "status", {"status": "running", "kind": run.kind})
            if run.kind == "war_room":
                output = await self._war_room(recorder, ctx, run)
            elif run.kind == "sentinel":
                output = await self._sentinel_brief(recorder, ctx, run)
            else:
                output = await self._copilot(recorder, ctx, run)
        except asyncio.CancelledError:
            status, error = "failed", "worker shut down before the run finished"
            raise
        except Exception as e:
            status, error = "failed", f"{type(e).__name__}: {e}"
            log.warning("agent.run_failed", run_id=str(run.id), kind=run.kind, error=error)
        finally:
            totals = recorder.totals
            async with self.db.transaction() as session:
                await session.execute(
                    update(AgentRun)
                    .where(AgentRun.id == run.id)
                    .values(
                        status=status,
                        output=output,
                        error=error,
                        provider=totals.provider,
                        model=totals.model,
                        input_tokens=totals.input_tokens,
                        output_tokens=totals.output_tokens,
                        cost_usd=Decimal(str(round(totals.cost_usd, 6))),
                        finished_at=datetime.now(UTC),
                    )
                )
            with contextlib.suppress(Exception):
                await recorder.emit(
                    "system",
                    "done",
                    {
                        "status": status,
                        "error": error,
                        "output": output,
                        "llm_calls": totals.llm_calls,
                        "tool_calls": totals.tool_calls,
                        "input_tokens": totals.input_tokens,
                        "output_tokens": totals.output_tokens,
                        "cost_usd": round(totals.cost_usd, 6),
                        "model": totals.model,
                        "provider": totals.provider,
                    },
                )
                await recorder.close()
            AGENT_RUNS.labels(run.kind, status).inc()
            AGENT_RUN_SECONDS.labels(run.kind).observe(time.perf_counter() - started)
        return await self.service.get_run(run.id, run.user_id)

    async def _system(self, template: str) -> str:
        universe = {i["symbol"]: i["name"] for i in await self.store.instruments()}
        return prompts.render(template, currency=self.settings.currency, universe=universe)

    async def _copilot(self, recorder: RunRecorder, ctx: ToolContext, run: AgentRun) -> dict[str, Any]:
        text = await self.runtime.run(
            recorder,
            ctx,
            agent="copilot",
            system=await self._system(prompts.COPILOT),
            prompt=str(run.input.get("message", "")),
            tools=COPILOT_TOOLS,
        )
        return {"text": text}

    async def _war_room(self, recorder: RunRecorder, ctx: ToolContext, run: AgentRun) -> dict[str, Any]:
        symbol = run.symbol or ""
        question = str(run.input.get("message") or "Give your view on this stock.")
        brief = f"Symbol: {symbol}\nQuestion from the user: {question}"

        async def specialist(name: str, template: str, tools: list[str]) -> str:
            return await self.runtime.run(
                recorder,
                ctx,
                agent=name,
                system=await self._system(template),
                prompt=brief,
                tools=tools,
                final_type="report",
            )

        # Fan out: the three analysts work at the same time and do not see each other's work,
        # so one cannot anchor the others. return_exceptions keeps one failure from sinking the rest.
        results = await asyncio.gather(*(specialist(*s) for s in SPECIALISTS), return_exceptions=True)
        reports: dict[str, str] = {}
        for (name, _, _), result in zip(SPECIALISTS, results, strict=True):
            if isinstance(result, BaseException):
                reports[name] = f"(report unavailable: {type(result).__name__}: {result})"
                await recorder.emit(name, "error", {"text": reports[name]})
            else:
                reports[name] = result
        if all(isinstance(r, BaseException) for r in results):
            raise RuntimeError("every analyst failed; no desk view can be formed")

        # Fan in: the lead sees only the three reports and has no tools of its own.
        sections = "\n\n".join(f"--- {name.upper()} ANALYST ---\n{text}" for name, text in reports.items())
        verdict = await self.runtime.run(
            recorder,
            ctx,
            agent="lead",
            system=await self._system(prompts.LEAD),
            prompt=f"{brief}\n\n{sections}",
            tools=[],
        )
        return {
            "text": verdict,
            **parse_signal(verdict),
            "reports": {name: {"text": text, **parse_signal(text)} for name, text in reports.items()},
        }

    # --- Sentinel -----------------------------------------------------------

    async def _sentinel_loop(self) -> None:
        """Turn anomalies into brief-writing runs, within a strict spending cap."""
        while True:
            try:
                events = await self.bus.consume(
                    [topics.ANOMALIES], SENTINEL_GROUP, self.consumer, count=10, block_ms=2000
                )
                for event in events:
                    if self.settings.sentinel_enabled and await self._sentinel_allowed(event.data["symbol"]):
                        await self.service.create_run(
                            kind="sentinel", user_id=None, symbol=event.data["symbol"], input={"anomaly": event.data}
                        )
                    await self.bus.ack(topics.ANOMALIES, SENTINEL_GROUP, [event.id])
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("agent.sentinel_loop_failed")
                await asyncio.sleep(2)

    async def _sentinel_allowed(self, symbol: str) -> bool:
        """An autonomous agent needs a budget it cannot argue with: per-symbol cooldown plus an hourly cap."""
        redis, key = self.bus.redis, self.bus.key
        if not await redis.set(
            key(f"sentinel.cooldown.{symbol}"), "1", nx=True, ex=self.settings.sentinel_cooldown_seconds
        ):
            return False
        hour = key(f"sentinel.hour.{int(time.time() // 3600)}")
        count = await redis.incr(hour)
        await redis.expire(hour, 7200)
        return int(count) <= self.settings.sentinel_max_briefs_per_hour

    async def _sentinel_brief(self, recorder: RunRecorder, ctx: ToolContext, run: AgentRun) -> dict[str, Any]:
        anomaly = run.input.get("anomaly", {})
        await asyncio.sleep(self.settings.sentinel_settle_seconds)
        a = {k: anomaly.get(k) for k in ("kind", "move_pct", "window_seconds", "zscore", "volume_ratio", "price")}
        prompt = (
            f"Symbol: {run.symbol}\n"
            f"Detector output: kind={a['kind']}, move_pct: {a['move_pct']} over {a['window_seconds']} seconds, "
            f"z-score {a['zscore']}, volume {a['volume_ratio']}x its recent average, price {a['price']}."
        )
        text = await self.runtime.run(
            recorder,
            ctx,
            agent="sentinel",
            system=await self._system(prompts.SENTINEL),
            prompt=prompt,
            tools=SENTINEL_TOOLS,
        )
        if anomaly.get("id"):
            async with self.db.transaction() as session:
                await session.execute(
                    update(Anomaly).where(Anomaly.id == uuid.UUID(anomaly["id"])).values(brief=text, run_id=run.id)
                )
        await self.bus.broadcast(
            topics.PULSE,
            {
                "type": "brief",
                "anomaly_id": anomaly.get("id"),
                "symbol": run.symbol,
                "text": text,
                "run_id": str(run.id),
                "ts": time.time(),
            },
        )
        return {"text": text}
