"""The provider-neutral shape of a model call.

The conversation format is the Messages-API one: a message's content is either
a string or a list of typed blocks (``text``, ``tool_use``, ``tool_result``).
Each provider adapter translates to and from its own wire format, so the agent
runtime is written once.

One rule matters for correctness: the runtime treats an assistant turn's
``content`` as **opaque** and replays it unchanged on the next request. Some
models attach reasoning blocks that are only valid if returned exactly as
they were sent; rebuilding the turn from just its text would drop them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

Message = dict[str, Any]


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ToolCall:
    id: str
    name: str
    input: dict[str, Any]


@dataclass(slots=True)
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass(slots=True)
class LLMResponse:
    text: str
    tool_calls: list[ToolCall]
    stop_reason: str  # end_turn | tool_use | max_tokens
    content: Any  # the assistant turn exactly as the provider returned it; append to history untouched
    usage: Usage
    model: str
    provider: str
    latency_ms: float = 0.0
    cost_usd: float = field(default=0.0)


class LLMError(Exception):
    def __init__(self, message: str, *, retryable: bool, status: int | None = None) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.status = status


class LLMRefusal(LLMError):
    """The model declined the request. Retrying the same request will not help."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=False)


class LLMProvider(Protocol):
    name: str
    model: str

    async def complete(
        self, *, system: str, messages: list[Message], tools: list[ToolSpec], max_tokens: int
    ) -> LLMResponse: ...

    async def aclose(self) -> None: ...


def block_to_dict(block: Any) -> dict[str, Any]:
    """Content blocks are plain dicts from some adapters and SDK objects from others."""
    if isinstance(block, dict):
        return block
    dumped: dict[str, Any] = block.model_dump() if hasattr(block, "model_dump") else dict(vars(block))
    return dumped


def content_blocks(content: Any) -> list[dict[str, Any]]:
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    return [block_to_dict(b) for b in content or []]


def tool_result(tool_use_id: str, content: str, *, is_error: bool = False) -> dict[str, Any]:
    block: dict[str, Any] = {"type": "tool_result", "tool_use_id": tool_use_id, "content": content}
    if is_error:
        block["is_error"] = True
    return block
