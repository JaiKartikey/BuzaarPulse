"""Provider adapters, checked at the wire: what is sent, and how responses and errors are mapped.

The Claude tests drive the real Anthropic SDK against a mock HTTP transport, so
they cover the SDK's own request building, not just our arguments to it.
"""

from __future__ import annotations

import json
from typing import Any

import anthropic
import httpx
import httpx2
import pytest

from bazaarpulse.agents.backtest import run_backtest
from bazaarpulse.agents.llm import LLMError, LLMRefusal, ToolSpec, build_gateway
from bazaarpulse.agents.llm.anthropic_provider import FALLBACK_BETA, AnthropicProvider
from bazaarpulse.agents.llm.groq_provider import GroqProvider, to_chat_messages
from bazaarpulse.agents.llm.mock_provider import MockProvider
from bazaarpulse.agents.llm.types import tool_result
from tests.conftest import make_settings

TOOL = ToolSpec(
    "get_quote",
    "Latest price.",
    {
        "type": "object",
        "properties": {"symbols": {"type": "array", "items": {"type": "string"}}},
        "required": ["symbols"],
        "additionalProperties": False,
    },
)


def claude(handler: Any, **kw: Any) -> AnthropicProvider:
    client = anthropic.AsyncAnthropic(
        api_key="test-key",
        max_retries=0,
        http_client=anthropic.DefaultAsyncHttpxClient(transport=httpx2.MockTransport(handler)),
    )
    return AnthropicProvider(api_key=None, client=client, **kw)


def message(content: list[dict[str, Any]], stop_reason: str = "end_turn", **extra: Any) -> dict[str, Any]:
    return {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5-5",
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {
            "input_tokens": 120,
            "output_tokens": 30,
            "cache_read_input_tokens": 900,
            "cache_creation_input_tokens": 0,
        },
        **extra,
    }


async def test_claude_request_shape_and_tool_use_round_trip() -> None:
    sent: list[httpx2.Request] = []
    replies = [
        message(
            [
                {"type": "text", "text": "Checking."},
                {"type": "tool_use", "id": "toolu_1", "name": "get_quote", "input": {"symbols": ["TCS"]}},
            ],
            stop_reason="tool_use",
        ),
        message([{"type": "text", "text": "TCS is at 4,100."}]),
    ]

    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(request)
        return httpx2.Response(200, json=replies[len(sent) - 1])

    provider = claude(handler, effort="high")
    history: list[dict[str, Any]] = [{"role": "user", "content": "How is TCS?"}]
    first = await provider.complete(system="ROLE: copilot", messages=history, tools=[TOOL], max_tokens=1000)

    assert first.stop_reason == "tool_use" and first.text == "Checking."
    assert [(c.id, c.name, c.input) for c in first.tool_calls] == [("toolu_1", "get_quote", {"symbols": ["TCS"]})]
    assert (first.usage.input_tokens, first.usage.output_tokens, first.usage.cache_read_tokens) == (120, 30, 900)

    body = json.loads(sent[0].content)
    assert sent[0].url.path == "/v1/messages" and FALLBACK_BETA in sent[0].headers["anthropic-beta"]
    assert body["model"] == "claude-opus-5-5" and body["fallbacks"] == "default"
    assert body["output_config"] == {"effort": "high"}
    assert body["system"] == [{"type": "text", "text": "ROLE: copilot", "cache_control": {"type": "ephemeral"}}]
    assert body["tools"][0]["strict"] is True and body["tools"][0]["name"] == "get_quote"
    # Current models reject sampling parameters and forced tool choice; neither is ever sent.
    assert not {"temperature", "top_p", "top_k", "tool_choice", "thinking"} & set(body)

    # The assistant turn is replayed exactly as received, then all tool results go back in one user message.
    history += [
        {"role": "assistant", "content": first.content},
        {"role": "user", "content": [tool_result("toolu_1", '{"last": 4100}')]},
    ]
    second = await provider.complete(system="ROLE: copilot", messages=history, tools=[TOOL], max_tokens=1000)
    assert second.text == "TCS is at 4,100." and second.tool_calls == [] and second.stop_reason == "end_turn"
    replayed = json.loads(sent[1].content)["messages"]
    assert [m["role"] for m in replayed] == ["user", "assistant", "user"]
    assert [b["type"] for b in replayed[1]["content"]] == ["text", "tool_use"]
    assert replayed[1]["content"][1]["id"] == "toolu_1"
    assert replayed[2]["content"] == [{"type": "tool_result", "tool_use_id": "toolu_1", "content": '{"last": 4100}'}]
    await provider.aclose()


async def test_claude_without_refusal_fallback_uses_the_plain_endpoint() -> None:
    sent: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(request)
        return httpx2.Response(200, json=message([{"type": "text", "text": "ok"}]))

    provider = claude(handler, refusal_fallback=False)
    await provider.complete(system="s", messages=[{"role": "user", "content": "hi"}], tools=[], max_tokens=100)
    body = json.loads(sent[0].content)
    assert "fallbacks" not in body and "tools" not in body
    assert FALLBACK_BETA not in sent[0].headers.get("anthropic-beta", "")


async def test_claude_refusal_and_errors_are_classified() -> None:
    refusal = message(
        [], stop_reason="refusal", stop_details={"type": "refusal", "category": "cyber", "explanation": None}
    )
    provider = claude(lambda r: httpx2.Response(200, json=refusal))
    with pytest.raises(LLMRefusal, match="cyber"):
        await provider.complete(system="s", messages=[{"role": "user", "content": "x"}], tools=[], max_tokens=10)

    def error(status: int, kind: str) -> AnthropicProvider:
        body = {"type": "error", "error": {"type": kind, "message": "nope"}}
        return claude(lambda r: httpx2.Response(status, json=body))

    for status, kind, retryable in [
        (429, "rate_limit_error", True),
        (529, "overloaded_error", True),
        (400, "invalid_request_error", False),
        (401, "authentication_error", False),
    ]:
        with pytest.raises(LLMError) as caught:
            await error(status, kind).complete(
                system="s", messages=[{"role": "user", "content": "x"}], tools=[], max_tokens=10
            )
        assert caught.value.retryable is retryable and caught.value.status == status


def groq(handler: Any) -> GroqProvider:
    client = httpx.AsyncClient(base_url="https://api.groq.test/openai/v1", transport=httpx.MockTransport(handler))
    return GroqProvider(api_key="k", client=client)


def test_conversation_is_translated_to_chat_completions_messages() -> None:
    history = [
        {"role": "user", "content": "How is TCS?"},
        {
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "", "signature": "sig"},  # another provider's block: dropped
                {"type": "text", "text": "Checking."},
                {"type": "tool_use", "id": "c1", "name": "get_quote", "input": {"symbols": ["TCS"]}},
            ],
        },
        {"role": "user", "content": [tool_result("c1", '{"last": 4100}'), {"type": "text", "text": "Answer now."}]},
    ]
    assert to_chat_messages("SYSTEM", history) == [
        {"role": "system", "content": "SYSTEM"},
        {"role": "user", "content": "How is TCS?"},
        {
            "role": "assistant",
            "content": "Checking.",
            "tool_calls": [
                {"id": "c1", "type": "function", "function": {"name": "get_quote", "arguments": '{"symbols": ["TCS"]}'}}
            ],
        },
        {"role": "tool", "tool_call_id": "c1", "content": '{"last": 4100}'},
        {"role": "user", "content": "Answer now."},
    ]


async def test_groq_tool_calls_are_parsed_and_bad_json_is_handed_back() -> None:
    reply = {
        "model": "llama-3.3-70b-versatile",
        "choices": [
            {
                "finish_reason": "tool_calls",
                "message": {
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "a",
                            "type": "function",
                            "function": {"name": "get_quote", "arguments": '{"symbols":["INFY"]}'},
                        },
                        {"id": "b", "type": "function", "function": {"name": "get_quote", "arguments": "{not json"}},
                    ],
                },
            }
        ],
        "usage": {"prompt_tokens": 200, "completion_tokens": 40},
    }
    sent: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return httpx.Response(200, json=reply)

    response = await groq(handler).complete(
        system="s", messages=[{"role": "user", "content": "q"}], tools=[TOOL], max_tokens=50_000
    )
    assert response.stop_reason == "tool_use" and response.usage.input_tokens == 200
    assert response.tool_calls[0].input == {"symbols": ["INFY"]}
    assert "__invalid_json__" in response.tool_calls[1].input
    assert [b["type"] for b in response.content] == ["tool_use", "tool_use"]
    assert sent[0]["tools"][0] == {
        "type": "function",
        "function": {"name": "get_quote", "description": "Latest price.", "parameters": TOOL.input_schema},
    }
    assert sent[0]["max_tokens"] == 8000, "capped to what the model accepts"


async def test_groq_errors_are_classified() -> None:
    for status, retryable in [(429, True), (503, True), (400, False), (401, False)]:
        with pytest.raises(LLMError) as caught:
            await groq(lambda r, s=status: httpx.Response(s, text="nope")).complete(
                system="s", messages=[{"role": "user", "content": "q"}], tools=[], max_tokens=10
            )
        assert caught.value.retryable is retryable

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    with pytest.raises(LLMError) as caught:
        await groq(timeout).complete(system="s", messages=[{"role": "user", "content": "q"}], tools=[], max_tokens=10)
    assert caught.value.retryable is True


def test_provider_selection_never_adds_the_mock_behind_a_real_model() -> None:
    redis: Any = None
    names = lambda **kw: [p.name for p in build_gateway(make_settings(**kw), redis).providers]  # noqa: E731
    assert names() == ["mock"]
    assert names(llm_provider="auto", groq_api_key="g") == ["groq"]
    assert names(llm_provider="auto", anthropic_api_key="a", groq_api_key="g") == ["anthropic", "groq"]
    assert names(llm_provider="groq", anthropic_api_key="a", groq_api_key="g") == ["groq", "anthropic"]
    assert names(llm_provider="mock", anthropic_api_key="a") == ["mock"]
    gateway = build_gateway(make_settings(llm_provider="auto", anthropic_api_key="a"), redis)
    assert isinstance(gateway.primary, AnthropicProvider) and gateway.primary.model == "claude-opus-5-5"


def test_gateway_cost_uses_the_price_table_and_cache_discounts() -> None:
    from bazaarpulse.agents.llm import LLMResponse, Usage

    redis: Any = None
    gateway = build_gateway(make_settings(), redis)
    usage = Usage(input_tokens=1_000_000, output_tokens=100_000, cache_read_tokens=2_000_000)
    paid = LLMResponse("", [], "end_turn", [], usage, "claude-opus-5-5", "anthropic")
    # 1M input at $4 + 2M cache reads at a tenth of that + 0.1M output at $20
    assert gateway.cost(paid) == pytest.approx(4.0 + 0.8 + 2.0)
    assert gateway.cost(LLMResponse("", [], "end_turn", [], usage, "offline-scripted", "mock")) == 0.0


def test_mock_recognises_symbols_by_ticker_and_by_name() -> None:
    mock = MockProvider({"TCS": "Tata Consultancy Services", "SBIN": "State Bank of India", "ITC": "ITC"})
    assert mock._mentioned("compare tcs with State Bank today") == ["TCS", "SBIN"]
    assert mock._mentioned("what about switching costs?") == []  # 'itc' inside a word is not ITC


# --- Backtester ---------------------------------------------------------------


def candles(closes: list[float]) -> list[dict[str, float]]:
    return [{"o": c, "h": c, "l": c, "c": c, "v": 100} for c in closes]


def test_backtest_trend_following_on_a_trend_and_on_noise() -> None:
    rising = candles([100 + i for i in range(120)])
    trend = run_backtest(rising, "sma_cross", fee_bps=0)
    hold = run_backtest(rising, "buy_hold", fee_bps=0)
    assert hold["total_return_pct"] == pytest.approx(118.0, abs=1.5) and hold["trades"] == 1
    # It can only join the trend once both averages exist, so it captures less than buy and hold.
    assert 0 < trend["total_return_pct"] < hold["total_return_pct"] and trend["max_drawdown_pct"] == 0

    falling = candles([200 - i for i in range(120)])
    assert run_backtest(falling, "sma_cross", fee_bps=0)["trades"] == 0, "never long in a steady downtrend"
    assert run_backtest(candles([100.0] * 10), "sma_cross")["error"].startswith("not enough history")
    with pytest.raises(ValueError, match="unknown strategy"):
        run_backtest(rising, "martingale")


def test_backtest_trades_at_the_next_open_not_on_the_bar_that_gave_the_signal() -> None:
    # Flat, then one huge bar. A strategy that could trade on the same bar's close would "catch" the jump.
    series = [{"o": 100.0, "h": 100.0, "l": 100.0, "c": 100.0, "v": 1} for _ in range(40)]
    series.append({"o": 100.0, "h": 200.0, "l": 100.0, "c": 200.0, "v": 1})
    series += [{"o": 200.0, "h": 200.0, "l": 200.0, "c": 200.0, "v": 1} for _ in range(5)]
    result = run_backtest(series, "sma_cross", fee_bps=0)
    assert result["trades"] == 1
    assert result["total_return_pct"] == 0.0, "it bought at 200, after the jump, and made nothing"


def test_fees_reduce_returns() -> None:
    zigzag = candles([100 + (8 if (i // 12) % 2 else -8) + i * 0.05 for i in range(300)])
    free = run_backtest(zigzag, "rsi_reversion", fee_bps=0)
    costly = run_backtest(zigzag, "rsi_reversion", fee_bps=50)
    assert free["trades"] == costly["trades"] > 0
    assert costly["total_return_pct"] < free["total_return_pct"]
