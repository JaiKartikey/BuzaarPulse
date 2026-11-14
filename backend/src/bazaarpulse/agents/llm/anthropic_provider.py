"""Claude through the official Anthropic SDK.

Request shape, and why each part is there:

* ``system`` carries a cache breakpoint. Tools and the system prompt are
  identical on every step of a run, so from the second step on they are read
  from the prompt cache at a fraction of the price. For that to work the
  system prompt must be byte-stable: nothing time-dependent goes in it.
* Tools are ``strict``: the API guarantees tool arguments match the schema.
* ``output_config.effort`` is the depth-versus-cost dial. Thinking is adaptive
  on current models and is left at its default; no sampling parameters are sent.
* The assistant turn is returned as the SDK's own content blocks and replayed
  untouched, which keeps reasoning blocks valid across tool-use steps.
* Refusal fallback (beta, on by default): if a safety classifier declines a
  request, the API retries it on another Claude model within the same call.
  A ``refusal`` that still comes back is surfaced as ``LLMRefusal``.
"""

from __future__ import annotations

import time
from typing import Any

import anthropic

from bazaarpulse.agents.llm.types import LLMError, LLMRefusal, LLMResponse, Message, ToolCall, ToolSpec, Usage

FALLBACK_BETA = "server-side-fallback-2026-07-01"


class AnthropicProvider:
    name = "anthropic"

    def __init__(
        self,
        *,
        api_key: str | None,
        model: str = "claude-opus-5-5",
        effort: str = "medium",
        refusal_fallback: bool = True,
        timeout: float = 120.0,
        client: anthropic.AsyncAnthropic | None = None,
    ) -> None:
        self.model = model
        self.effort = effort
        self.refusal_fallback = refusal_fallback
        # The SDK retries 429s, 5xx and connection errors itself (twice, with backoff).
        self._client = client or anthropic.AsyncAnthropic(api_key=api_key, timeout=timeout)

    async def complete(
        self, *, system: str, messages: list[Message], tools: list[ToolSpec], max_tokens: int
    ) -> LLMResponse:
        params: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens,
            "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            "messages": messages,
            "output_config": {"effort": self.effort},
        }
        if tools:
            params["tools"] = [
                {"name": t.name, "description": t.description, "input_schema": t.input_schema, "strict": True}
                for t in tools
            ]
        started = time.perf_counter()
        try:
            if self.refusal_fallback:
                response: Any = await self._client.beta.messages.create(
                    betas=[FALLBACK_BETA], fallbacks="default", **params
                )
            else:
                response = await self._client.messages.create(**params)
        except anthropic.RateLimitError as e:
            raise LLMError(f"rate limited: {e.message}", retryable=True, status=429) from e
        except anthropic.APIStatusError as e:
            raise LLMError(
                f"API error {e.status_code}: {e.message}", retryable=e.status_code >= 500, status=e.status_code
            ) from e
        except anthropic.APIConnectionError as e:
            raise LLMError(f"connection error: {e}", retryable=True) from e

        if response.stop_reason == "refusal":
            details = getattr(response, "stop_details", None)
            raise LLMRefusal(f"request declined ({getattr(details, 'category', None) or 'unspecified'})")

        text = "".join(b.text for b in response.content if b.type == "text")
        calls = [ToolCall(b.id, b.name, dict(b.input)) for b in response.content if b.type == "tool_use"]
        usage = response.usage
        return LLMResponse(
            text=text,
            tool_calls=calls,
            stop_reason=response.stop_reason or "end_turn",
            content=response.content,
            usage=Usage(
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_read_tokens=getattr(usage, "cache_read_input_tokens", 0) or 0,
                cache_write_tokens=getattr(usage, "cache_creation_input_tokens", 0) or 0,
            ),
            model=response.model,  # with fallback on, this can differ from the model requested
            provider=self.name,
            latency_ms=(time.perf_counter() - started) * 1000,
        )

    async def aclose(self) -> None:
        await self._client.close()
