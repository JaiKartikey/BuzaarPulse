"""The agent loop, and the recorder that makes every step durable and streamable.

The loop is the standard one: call the model with tools; if it asks for
tools, run them (in parallel), hand all the results back in one message, and
repeat until it answers in plain text or the step budget runs out.

It is written by hand rather than taken from a framework because the two
things this system cares about live *between* the steps: every step is written
to Postgres (an audit trail of what the agent saw and did) and published to a
replayable Redis stream (so a browser can watch the run live, and catch up
after a reconnect).
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.dialects.postgresql import insert

from bazaarpulse.agents.llm import LLMGateway, LLMResponse
from bazaarpulse.agents.llm.types import Message, tool_result
from bazaarpulse.agents.prompts import STEP_LIMIT_NOTE
from bazaarpulse.agents.tools import Toolbox, ToolContext
from bazaarpulse.bus import RedisBus, topics
from bazaarpulse.db import Database
from bazaarpulse.db.models import AgentStep

STREAM_MAXLEN = 2_000
STREAM_TTL_SECONDS = 3_600
_PREVIEW_CHARS = 1_500


@dataclass(slots=True)
class RunTotals:
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    llm_calls: int = 0
    tool_calls: int = 0
    provider: str | None = None
    model: str | None = None

    def add(self, response: LLMResponse) -> None:
        self.input_tokens += (
            response.usage.input_tokens + response.usage.cache_read_tokens + response.usage.cache_write_tokens
        )
        self.output_tokens += response.usage.output_tokens
        self.cost_usd += response.cost_usd
        self.llm_calls += 1
        self.provider, self.model = response.provider, response.model


@dataclass(slots=True)
class RunRecorder:
    """Persists and publishes the steps of one run. Shared by every agent taking part in it."""

    db: Database
    bus: RedisBus
    run_id: uuid.UUID
    totals: RunTotals = field(default_factory=RunTotals)
    _seq: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def emit(self, agent: str, type: str, payload: dict[str, Any]) -> None:
        # The lock keeps sequence numbers gap-free and ordered when several agents emit at once.
        async with self._lock:
            self._seq += 1
            seq = self._seq
            async with self.db.transaction() as session:
                await session.execute(
                    insert(AgentStep).values(run_id=self.run_id, seq=seq, agent=agent, type=type, payload=payload)
                )
            await self.bus.publish(
                topics.agent_run(self.run_id),
                {"seq": seq, "agent": agent, "type": type, "payload": payload, "ts": time.time()},
                maxlen=STREAM_MAXLEN,
            )

    async def close(self) -> None:
        """Let the live stream expire; after that, readers fall back to the copy in Postgres."""
        await self.bus.redis.expire(self.bus.key(topics.agent_run(self.run_id)), STREAM_TTL_SECONDS)


def _preview(text: str) -> Any:
    """Tool results are stored parsed (for the UI) and bounded (a step row is not a data lake)."""
    clipped = text[:_PREVIEW_CHARS]
    try:
        return json.loads(text) if len(text) <= _PREVIEW_CHARS else clipped + "..."
    except json.JSONDecodeError:
        return clipped


class AgentRuntime:
    def __init__(self, gateway: LLMGateway, toolbox: Toolbox, *, max_steps: int, max_output_tokens: int) -> None:
        self.gateway = gateway
        self.toolbox = toolbox
        self.max_steps = max_steps
        self.max_output_tokens = max_output_tokens

    async def run(
        self,
        recorder: RunRecorder,
        ctx: ToolContext,
        *,
        agent: str,
        system: str,
        prompt: str,
        tools: list[str],
        final_type: str = "final",
    ) -> str:
        """Run one agent to completion and return its final text."""
        specs = self.toolbox.specs(tools, ctx)
        user_key = str(ctx.user_id) if ctx.user_id else "system"
        messages: list[Message] = [{"role": "user", "content": prompt}]

        for step in range(self.max_steps + 1):
            out_of_steps = step == self.max_steps
            response = await self.gateway.complete(
                user_key=user_key,
                system=system,
                messages=messages,
                # On the last allowed step the model gets no tools: it has to answer with what it has.
                tools=[] if out_of_steps else specs,
                max_tokens=self.max_output_tokens,
            )
            recorder.totals.add(response)

            if not response.tool_calls or out_of_steps:
                text = response.text.strip() or "(the model returned no text)"
                if response.stop_reason == "max_tokens":
                    text += "\n\n(The answer was cut off at the output limit.)"
                await recorder.emit(agent, final_type, {"text": text})
                return text

            if response.text.strip():
                await recorder.emit(agent, "message", {"text": response.text.strip()})
            # The assistant turn goes back exactly as received (see llm/types.py).
            messages.append({"role": "assistant", "content": response.content})
            for call in response.tool_calls:
                await recorder.emit(agent, "tool_call", {"id": call.id, "name": call.name, "input": call.input})

            # Independent lookups run concurrently; their results return together in ONE user message.
            outcomes = await asyncio.gather(*(self.toolbox.call(c.name, c.input, ctx) for c in response.tool_calls))
            recorder.totals.tool_calls += len(outcomes)
            blocks: list[dict[str, Any]] = []
            for call, (text, is_error) in zip(response.tool_calls, outcomes, strict=True):
                blocks.append(tool_result(call.id, text, is_error=is_error))
                await recorder.emit(
                    agent,
                    "tool_result",
                    {"id": call.id, "name": call.name, "ok": not is_error, "result": _preview(text)},
                )
            if step == self.max_steps - 1:
                blocks.append({"type": "text", "text": STEP_LIMIT_NOTE})
            messages.append({"role": "user", "content": blocks})

        raise AssertionError("unreachable: the loop always returns on its last iteration")
