"""The agent stack end to end on the offline model: tool loop, persistence, proposals, desk, sentinel."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, update

from bazaarpulse.agents.llm import LLMError, LLMRefusal, LLMResponse, ToolCall, ToolSpec, Usage
from bazaarpulse.agents.llm.mock_provider import LABEL, MockProvider
from bazaarpulse.agents.llm.types import Message
from bazaarpulse.agents.proposals import ProposalError
from bazaarpulse.agents.service import COPILOT_TOOLS, SENTINEL_TOOLS, parse_signal
from bazaarpulse.agents.tools import ToolContext
from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import Anomaly, Order, TradeProposal
from tests.agent_helpers import SYMBOLS, Desk, build_desk, stream_events
from tests.conftest import make_settings
from tests.trading_helpers import build_rig


@pytest.fixture
async def desk(db: Database, bus: RedisBus, settings: Settings) -> Desk:
    return await build_desk(build_rig(db, bus, settings))


# --- Copilot ----------------------------------------------------------------


async def test_copilot_answers_from_parallel_tool_calls_and_records_every_step(desk: Desk) -> None:
    user = await desk.rig.user()
    run = await desk.ask(user, "How is TCS doing right now, and is there any news?")

    assert run.status == "completed" and run.provider == "mock" and run.cost_usd == 0
    assert run.input_tokens > 0 and run.output_tokens > 0
    text = run.output["text"]  # type: ignore[index]
    assert "TCS is at" in text and "brokerage upgrade" in text and "RSI" in text and LABEL in text

    steps = await desk.steps(run)
    assert [s["seq"] for s in steps] == list(range(1, len(steps) + 1)), "sequence numbers are gap-free"
    kinds = [(s["type"], s["payload"].get("name")) for s in steps]
    # One model turn asked for three lookups at once; all three results came back before the answer.
    assert kinds[:2] == [("status", None), ("message", None)]
    assert sorted(k for k in kinds if k[0] == "tool_call") == [
        ("tool_call", "get_indicators"),
        ("tool_call", "get_quote"),
        ("tool_call", "search_news"),
    ]
    assert all(s["payload"]["ok"] for s in steps if s["type"] == "tool_result")
    assert [k[0] for k in kinds[-2:]] == ["final", "done"]

    # The same steps are on the replayable stream the browser tails, in the same order.
    events = await stream_events(desk, run)
    assert [(e["seq"], e["type"]) for e in events] == [(s["seq"], s["type"]) for s in steps]
    assert events[-1]["payload"]["status"] == "completed" and events[-1]["payload"]["tool_calls"] == 3


async def test_runs_are_private_to_their_owner(desk: Desk) -> None:
    owner, stranger = await desk.rig.user(), await desk.rig.user()
    run = await desk.ask(owner, "How is the market?")
    assert await desk.agents.get_run(run.id, owner) is not None
    assert await desk.agents.get_run(run.id, stranger) is None
    assert [r.id for r in await desk.agents.list_runs(owner)] == [run.id]
    assert await desk.agents.list_runs(stranger) == []


# --- Human-in-the-loop trading ----------------------------------------------


async def test_agent_can_only_propose_and_the_user_decides(desk: Desk) -> None:
    user = await desk.rig.user()
    run = await desk.ask(user, "Please buy 10 TCS for me")
    assert "waiting for your approval" in run.output["text"]  # type: ignore[index]

    (proposal,) = await desk.proposals.list(user)
    assert proposal["status"] == "pending" and proposal["run_id"] == str(run.id)
    assert proposal["orders"][0] | {"estimated_value": 0} == {
        "symbol": "TCS", "side": "buy", "order_type": "market", "quantity": 10,
        "limit_price": None, "estimated_value": 0,
    }  # fmt: skip
    assert proposal["risk_report"]["passed"] is True
    async with desk.rig.db.session() as session:
        assert await session.scalar(select(func.count()).select_from(Order)) == 0, "drafting a plan places nothing"

    decided = await desk.proposals.decide(user, uuid.UUID(proposal["id"]), approve=True)
    assert decided.status == "executed"
    (order,) = await desk.rig.orders.list(user)
    assert (order.source, order.proposal_id, order.status) == ("agent", decided.id, "open")
    assert len(await desk.rig.executor.on_price("TCS", (await desk.rig.store.quote("TCS"))["last"])) == 1  # type: ignore[index]

    with pytest.raises(ProposalError, match="already executed"):
        await desk.proposals.decide(user, decided.id, approve=True)


async def test_proposal_cannot_be_decided_twice_or_by_someone_else_or_after_expiry(desk: Desk) -> None:
    user, stranger = await desk.rig.user(), await desk.rig.user()
    await desk.ask(user, "buy 5 INFY")
    await desk.ask(user, "buy 5 ITC")
    first, second = (uuid.UUID(p["id"]) for p in await desk.proposals.list(user))

    with pytest.raises(ProposalError, match="not found"):
        await desk.proposals.decide(stranger, first, approve=True)

    # Approve and reject clicked at the same moment: exactly one of them takes effect.
    outcomes = await asyncio.gather(
        desk.proposals.decide(user, first, approve=True),
        desk.proposals.decide(user, first, approve=False),
        return_exceptions=True,
    )
    assert sorted(type(o).__name__ for o in outcomes) == ["ProposalError", "TradeProposal"]

    async with desk.rig.db.transaction() as session:
        await session.execute(
            update(TradeProposal)
            .where(TradeProposal.id == second)
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    with pytest.raises(ProposalError, match="expired"):
        await desk.proposals.decide(user, second, approve=True)
    statuses = {p["id"]: p["status"] for p in await desk.proposals.list(user)}
    assert statuses[str(second)] == "expired"


async def test_a_plan_that_fails_risk_is_not_drafted_and_the_agent_says_why(desk: Desk) -> None:
    user = await desk.rig.user()
    run = await desk.ask(user, "buy 4000 RELIANCE")  # far beyond a Rs 10 lakh account
    assert "did not pass the risk checks" in run.output["text"]  # type: ignore[index]
    assert "buying_power" in run.output["text"]  # type: ignore[index]
    assert await desk.proposals.list(user) == []


async def test_rebalance_proposes_trimming_a_concentrated_position(desk: Desk) -> None:
    user = await desk.rig.user()
    last = (await desk.rig.store.quote("SBIN"))["last"]  # type: ignore[index]
    await desk.rig.buy(user, "SBIN", int(380_000 / last))
    await desk.rig.executor.on_price("SBIN", last)

    await desk.ask(user, "Can you rebalance my portfolio?")
    (proposal,) = await desk.proposals.list(user)
    order = proposal["orders"][0]
    assert (order["symbol"], order["side"]) == ("SBIN", "sell") and 0 < order["quantity"] < int(380_000 / last)
    assert "concentration" in proposal["rationale"]


# --- Tool boundary ----------------------------------------------------------


async def test_tools_validate_input_and_never_expose_another_users_data(desk: Desk) -> None:
    user = await desk.rig.user()
    ctx = ToolContext(run_id=uuid.uuid4(), user_id=user)
    anonymous = ToolContext(run_id=uuid.uuid4(), user_id=None)

    text, failed = await desk.toolbox.call("get_quote", {"symbols": ["TCS"]}, ctx)
    assert not failed and '"TCS"' in text
    for bad_input, expected in [
        ({"symbols": "TCS"}, "must be of type array"),
        ({}, "symbols is required"),
        ({"symbols": ["TCS"], "user_id": "someone-else"}, "not a recognised field"),
        ({"symbols": ["NOPE"]}, "unknown symbol"),
        ({"__invalid_json__": "{oops"}, "not valid JSON"),
    ]:
        text, failed = await desk.toolbox.call("get_quote", bad_input, ctx)
        assert failed and expected in text, text
    assert (await desk.toolbox.call("delete_everything", {}, ctx))[1] is True

    # Runs with no user are not even offered user-scoped tools, and calling one anyway is refused.
    offered = {s.name for s in desk.toolbox.specs(COPILOT_TOOLS, anonymous)}
    assert "get_portfolio" not in offered and "propose_trade_plan" not in offered and "get_quote" in offered
    text, failed = await desk.toolbox.call("get_portfolio", {}, anonymous)
    assert failed and "not available" in text
    # Every schema is closed (no extra fields) with all fields required: the shape strict tool use needs.
    for spec in desk.toolbox.specs(COPILOT_TOOLS, ctx):
        assert spec.input_schema["additionalProperties"] is False
        assert set(spec.input_schema["required"]) == set(spec.input_schema["properties"])


class LoopingProvider:
    """A model that never stops asking for tools."""

    name, model = "looper", "looper-1"

    def __init__(self) -> None:
        self.tools_offered: list[int] = []

    async def aclose(self) -> None:
        return None

    async def complete(
        self, *, system: str, messages: list[Message], tools: list[ToolSpec], max_tokens: int
    ) -> LLMResponse:
        self.tools_offered.append(len(tools))
        if not tools:
            return LLMResponse("Here is what I found so far.", [], "end_turn", [], Usage(5, 5), self.model, self.name)
        call = ToolCall(f"c{len(self.tools_offered)}", "get_market_overview", {})
        content = [{"type": "tool_use", "id": call.id, "name": call.name, "input": {}}]
        return LLMResponse("", [call], "tool_use", content, Usage(5, 5), self.model, self.name)


async def test_step_budget_ends_a_run_that_would_loop_forever(db: Database, bus: RedisBus) -> None:
    looper = LoopingProvider()
    rig = build_rig(db, bus, make_settings(redis_prefix=bus.prefix, agent_max_steps=3))
    desk = await build_desk(rig, providers=[looper])
    run = await desk.ask(await rig.user(), "anything")

    assert run.status == "completed" and run.output == {"text": "Here is what I found so far."}
    # Three turns with tools, then a final turn where the tools are withheld so it must answer.
    assert looper.tools_offered[:3] == [len(COPILOT_TOOLS)] * 3 and looper.tools_offered[3] == 0
    assert sum(1 for s in await desk.steps(run) if s["type"] == "tool_call") == 3


# --- War room ---------------------------------------------------------------


async def test_war_room_runs_three_analysts_and_a_lead(desk: Desk) -> None:
    user = await desk.rig.user()
    run = await desk.ask(user, "Is this a good moment for TCS?", kind="war_room", symbol="TCS")

    assert run.status == "completed"
    out = run.output
    assert out is not None and set(out["reports"]) == {"technical", "news", "risk"}
    assert out["signal"] in {"bullish", "bearish", "neutral"} and out["confidence"] in {"low", "medium", "high"}
    assert out["reports"]["news"]["signal"] == "bullish"  # the seeded headline is an upgrade
    assert all(r["signal"] for r in out["reports"].values())

    steps = await desk.steps(run)
    assert {s["agent"] for s in steps} == {"system", "technical", "news", "risk", "lead"}
    assert [s["agent"] for s in steps if s["type"] == "report"].__len__() == 3
    # The lead speaks once, after all three reports, and calls no tools.
    lead = [s for s in steps if s["agent"] == "lead"]
    assert [s["type"] for s in lead] == ["final"]
    assert max(s["seq"] for s in steps if s["type"] == "report") < lead[0]["seq"]


class FlakyProvider(MockProvider):
    """Fails for chosen roles, to show one analyst's failure does not sink the desk."""

    def __init__(self, failing_roles: set[str]) -> None:
        super().__init__(SYMBOLS)
        self.failing_roles = failing_roles

    async def complete(
        self, *, system: str, messages: list[Message], tools: list[ToolSpec], max_tokens: int
    ) -> LLMResponse:
        role = system.split("\n", 1)[0].removeprefix("ROLE:").strip()
        if role in self.failing_roles:
            raise LLMError(f"model unavailable for {role}", retryable=False)
        return await super().complete(system=system, messages=messages, tools=tools, max_tokens=max_tokens)


async def test_war_room_survives_one_analyst_failing_and_fails_cleanly_if_all_do(
    db: Database, bus: RedisBus, settings: Settings
) -> None:
    rig = build_rig(db, bus, settings)
    user = await rig.user()

    desk = await build_desk(rig, providers=[FlakyProvider({"news"})])
    run = await desk.ask(user, "View on INFY?", kind="war_room", symbol="INFY")
    assert run.status == "completed"
    assert "report unavailable" in run.output["reports"]["news"]["text"]  # type: ignore[index]
    assert "Only 2 of 3 analyst reports" in run.output["text"]  # type: ignore[index]

    broken = await build_desk(rig, providers=[FlakyProvider({"technical", "news", "risk"})])
    failed = await broken.ask(user, "View on INFY?", kind="war_room", symbol="INFY")
    assert failed.status == "failed" and "every analyst failed" in (failed.error or "")
    assert (await stream_events(broken, failed))[-1]["payload"]["status"] == "failed"


# --- Sentinel ---------------------------------------------------------------


async def test_sentinel_writes_a_brief_for_an_anomaly_within_its_budget(db: Database, bus: RedisBus) -> None:
    rig = build_rig(db, bus, make_settings(redis_prefix=bus.prefix, sentinel_max_briefs_per_hour=2))
    desk = await build_desk(rig)
    anomaly_id = uuid.uuid4()
    event = {"id": str(anomaly_id), "symbol": "TCS", "kind": "price_spike", "move_pct": 2.1, "zscore": 7.5,
             "window_seconds": 30, "volume_ratio": 4.0, "price": 4190.0}  # fmt: skip
    async with rig.db.transaction() as session:
        session.add(
            Anomaly(
                id=anomaly_id,
                symbol="TCS",
                kind="price_spike",
                severity="high",
                details=event,
                detected_at=datetime.now(UTC),
            )
        )
    pulses: list[dict[str, Any]] = []

    async def listen() -> None:
        async for _, data in bus.subscribe([topics.PULSE]):
            pulses.append(data)

    listener = asyncio.create_task(listen())
    await asyncio.sleep(0.05)

    # Budget: one brief per symbol per cooldown, and a hard hourly cap across all symbols.
    assert await desk.worker._sentinel_allowed("TCS") is True
    assert await desk.worker._sentinel_allowed("TCS") is False
    assert await desk.worker._sentinel_allowed("INFY") is True
    assert await desk.worker._sentinel_allowed("SBIN") is False, "third brief this hour is over the cap"

    run = await desk.agents.create_run(kind="sentinel", user_id=None, symbol="TCS", input={"anomaly": event})
    await desk.worker.execute(run.id)
    await asyncio.sleep(0.05)
    listener.cancel()

    finished = await desk.agents.get_run(run.id)
    assert finished is not None and finished.status == "completed"
    brief = finished.output["text"]  # type: ignore[index]
    assert "TCS moved +2.10%" in brief and "brokerage upgrade" in brief
    async with rig.db.session() as session:
        stored = await session.get(Anomaly, anomaly_id)
    assert stored is not None and stored.brief == brief and stored.run_id == run.id
    assert [p["type"] for p in pulses] == ["brief"] and pulses[0]["anomaly_id"] == str(anomaly_id)
    # An autonomous run has no user, so it was never offered anything user-scoped.
    assert not {"get_portfolio", "propose_trade_plan"} & set(SENTINEL_TOOLS)
    assert {s["payload"]["name"] for s in await desk.steps(finished) if s["type"] == "tool_call"} <= set(SENTINEL_TOOLS)


async def test_a_redelivered_job_does_not_run_twice(desk: Desk) -> None:
    user = await desk.rig.user()
    run = await desk.agents.create_run(kind="copilot", user_id=user, input={"message": "How is the market?"})
    first, second = await asyncio.gather(desk.worker.execute(run.id), desk.worker.execute(run.id))
    assert [first is None, second is None].count(True) == 1, "exactly one worker claims the run"
    assert sum(1 for s in await desk.agents.steps(run.id) if s["type"] == "done") == 1


# --- Gateway ----------------------------------------------------------------


async def test_gateway_enforces_the_daily_budget_and_fails_over_between_real_providers(
    db: Database, bus: RedisBus, settings: Settings
) -> None:
    rig = build_rig(db, bus, settings)
    user = await rig.user()

    tight = await build_desk(rig, daily_token_budget=50)
    first = await tight.ask(user, "How is the market?")
    # The first model call is admitted (nothing spent yet); it alone exceeds 50 tokens, so the next call is refused.
    assert first.status == "failed" and "daily token budget" in (first.error or "")
    assert [s["type"] for s in await tight.steps(first)].count("tool_call") == 1
    blocked = await tight.ask(user, "And now?")
    assert blocked.status == "failed" and "daily token budget" in (blocked.error or "")
    assert not any(s["type"] == "tool_call" for s in await tight.steps(blocked))

    other = await rig.user()
    failover = await build_desk(rig, providers=[FlakyProvider({"copilot"}), MockProvider(SYMBOLS)])
    run = await failover.ask(other, "How is the market?")
    assert run.status == "completed", "the second provider answered when the first failed"


class RefusingProvider(MockProvider):
    async def complete(self, **kw: Any) -> LLMResponse:
        raise LLMRefusal("request declined (unspecified)")


async def test_a_refusal_is_final_and_is_not_shopped_to_the_next_provider(
    db: Database, bus: RedisBus, settings: Settings
) -> None:
    rig = build_rig(db, bus, settings)
    backup = MockProvider(SYMBOLS)
    desk = await build_desk(rig, providers=[RefusingProvider(SYMBOLS), backup])
    run = await desk.ask(await rig.user(), "How is the market?")
    assert run.status == "failed" and "declined" in (run.error or "") and backup.calls == 0


def test_signal_line_parsing() -> None:
    assert parse_signal("...\nSIGNAL: bullish  CONFIDENCE: high") == {"signal": "bullish", "confidence": "high"}
    assert parse_signal("signal: Bearish confidence: LOW") == {"signal": "bearish", "confidence": "low"}
    assert parse_signal("no verdict here") == {"signal": None, "confidence": None}
