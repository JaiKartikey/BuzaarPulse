"""Groq's OpenAI-compatible Chat Completions endpoint (Llama models), over plain httpx.

This adapter exists so the project can run a real model on a free tier. It
translates the runtime's block-style conversation into Chat Completions
messages and back:

    assistant tool_use block   <->  assistant.tool_calls[] (arguments as a JSON string)
    user tool_result block     <->  a separate {"role": "tool"} message per result
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx

from bazaarpulse.agents.llm.types import (
    LLMError,
    LLMResponse,
    Message,
    ToolCall,
    ToolSpec,
    Usage,
    content_blocks,
)

_MAX_COMPLETION_TOKENS = 8_000
_FINISH = {"tool_calls": "tool_use", "length": "max_tokens"}


def to_chat_messages(system: str, messages: list[Message]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = [{"role": "system", "content": system}]
    for message in messages:
        blocks = content_blocks(message["content"])
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
        if message["role"] == "assistant":
            calls = [
                {
                    "id": b["id"],
                    "type": "function",
                    "function": {"name": b["name"], "arguments": json.dumps(b["input"])},
                }
                for b in blocks
                if b.get("type") == "tool_use"
            ]
            entry: dict[str, Any] = {"role": "assistant", "content": text or None}
            if calls:
                entry["tool_calls"] = calls
            out.append(entry)
            continue
        # Tool results must come first and directly follow the assistant turn that asked for them.
        for b in blocks:
            if b.get("type") == "tool_result":
                content = b.get("content")
                out.append(
                    {
                        "role": "tool",
                        "tool_call_id": b["tool_use_id"],
                        "content": content if isinstance(content, str) else json.dumps(content),
                    }
                )
        if text:
            out.append({"role": "user", "content": text})
    return out


class GroqProvider:
    name = "groq"

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "llama-3.3-70b-versatile",
        base_url: str = "https://api.groq.com/openai/v1",
        timeout: float = 60.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.model = model
        self._client = client or httpx.AsyncClient(
            base_url=base_url, timeout=timeout, headers={"Authorization": f"Bearer {api_key}"}
        )

    async def complete(
        self, *, system: str, messages: list[Message], tools: list[ToolSpec], max_tokens: int
    ) -> LLMResponse:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": to_chat_messages(system, messages),
            "max_tokens": min(max_tokens, _MAX_COMPLETION_TOKENS),
            "temperature": 0.2,
        }
        if tools:
            body["tools"] = [
                {
                    "type": "function",
                    "function": {"name": t.name, "description": t.description, "parameters": t.input_schema},
                }
                for t in tools
            ]
        started = time.perf_counter()
        try:
            r = await self._client.post("/chat/completions", json=body)
        except httpx.TimeoutException as e:
            raise LLMError(f"timeout calling {self.model}", retryable=True) from e
        except httpx.TransportError as e:
            raise LLMError(f"network error calling {self.model}: {e}", retryable=True) from e
        if r.status_code == 429 or r.status_code >= 500:
            raise LLMError(f"{self.model} returned {r.status_code}", retryable=True, status=r.status_code)
        if r.status_code >= 400:
            raise LLMError(
                f"{self.model} returned {r.status_code}: {r.text[:300]}", retryable=False, status=r.status_code
            )

        data = r.json()
        choice = data["choices"][0]
        message = choice["message"]
        text = message.get("content") or ""
        calls: list[ToolCall] = []
        for call in message.get("tool_calls") or []:
            raw = call["function"].get("arguments") or "{}"
            try:
                arguments = json.loads(raw)
            except json.JSONDecodeError:
                # Hand the problem back to the model as a failed tool call rather than guessing what it meant.
                arguments = {"__invalid_json__": raw[:500]}
            calls.append(
                ToolCall(call["id"], call["function"]["name"], arguments if isinstance(arguments, dict) else {})
            )

        content: list[dict[str, Any]] = [{"type": "text", "text": text}] if text else []
        content += [{"type": "tool_use", "id": c.id, "name": c.name, "input": c.input} for c in calls]
        usage = data.get("usage") or {}
        return LLMResponse(
            text=text,
            tool_calls=calls,
            stop_reason="tool_use" if calls else _FINISH.get(choice.get("finish_reason", ""), "end_turn"),
            content=content,
            usage=Usage(int(usage.get("prompt_tokens", 0)), int(usage.get("completion_tokens", 0))),
            model=data.get("model", self.model),
            provider=self.name,
            latency_ms=(time.perf_counter() - started) * 1000,
        )

    async def aclose(self) -> None:
        await self._client.aclose()
