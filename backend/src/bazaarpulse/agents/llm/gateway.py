"""LLM gateway: the one code path through which any agent reaches any model.

Centralising the call gives one place to enforce who may spend what (a daily
token budget per user), to fail over between providers, and to measure tokens,
cost and latency. Agents never see API keys, quotas or pricing.

Failover only ever goes from one *real* provider to another. The offline mock
is never a silent fallback: if the real model is down, the run fails and says
so, instead of quietly returning templated analysis that looks genuine.
"""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime

from redis.asyncio import Redis

from bazaarpulse.agents.llm.types import LLMError, LLMProvider, LLMRefusal, LLMResponse, Message, ToolSpec
from bazaarpulse.config import Settings
from bazaarpulse.logging import get_logger
from bazaarpulse.metrics import LLM_COST, LLM_REQUESTS, LLM_TOKENS

log = get_logger(__name__)
_BUDGET_TTL_SECONDS = 2 * 86_400


class BudgetExceeded(LLMError):
    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=False)


class LLMGateway:
    def __init__(
        self,
        providers: list[LLMProvider],
        *,
        redis: Redis,
        prefix: str,
        prices: dict[str, tuple[float, float]],
        daily_token_budget: int,
        retries: int = 1,
    ) -> None:
        if not providers:
            raise ValueError("at least one LLM provider is required")
        self.providers = providers
        self.redis = redis
        self.prefix = prefix
        self.prices = prices
        self.daily_token_budget = daily_token_budget
        self.retries = retries

    @property
    def primary(self) -> LLMProvider:
        return self.providers[0]

    def _budget_key(self, user_key: str) -> str:
        return f"{self.prefix}llm.budget.{user_key}.{datetime.now(UTC):%Y%m%d}"

    async def tokens_used_today(self, user_key: str) -> int:
        return int(await self.redis.get(self._budget_key(user_key)) or 0)

    def cost(self, response: LLMResponse) -> float:
        if response.provider == "mock":
            return 0.0  # pricing the offline stand-in would produce made-up numbers
        price_in, price_out = self.prices.get(response.model, (0.0, 0.0))
        u = response.usage
        # Cache writes cost 1.25x input; cache reads cost 0.1x.
        billable_in = u.input_tokens + 1.25 * u.cache_write_tokens + 0.1 * u.cache_read_tokens
        return round((billable_in * price_in + u.output_tokens * price_out) / 1_000_000, 6)

    async def complete(
        self, *, user_key: str, system: str, messages: list[Message], tools: list[ToolSpec], max_tokens: int
    ) -> LLMResponse:
        if await self.tokens_used_today(user_key) >= self.daily_token_budget:
            raise BudgetExceeded(f"daily token budget of {self.daily_token_budget:,} reached")

        errors: list[str] = []
        for provider in self.providers:
            for attempt in range(self.retries + 1):
                try:
                    response = await provider.complete(
                        system=system, messages=messages, tools=tools, max_tokens=max_tokens
                    )
                except LLMRefusal:
                    LLM_REQUESTS.labels(provider.name, "refusal").inc()
                    raise  # another provider would be asked the very same thing; do not shop a refusal around
                except LLMError as e:
                    LLM_REQUESTS.labels(provider.name, "error").inc()
                    errors.append(f"{provider.name}: {e}")
                    log.warning("llm.call_failed", provider=provider.name, attempt=attempt, error=str(e))
                    if not e.retryable or attempt == self.retries:
                        break
                    # Full jitter: retries from many runs spread out instead of arriving together.
                    await asyncio.sleep(random.uniform(0, min(8.0, 0.5 * 2**attempt)))
                    continue
                await self._account(user_key, response)
                return response
        raise LLMError("all providers failed: " + "; ".join(errors), retryable=False)

    async def _account(self, user_key: str, response: LLMResponse) -> None:
        response.cost_usd = self.cost(response)
        used = response.usage.input_tokens + response.usage.output_tokens
        key = self._budget_key(user_key)
        pipe = self.redis.pipeline(transaction=False)
        pipe.incrby(key, used)
        pipe.expire(key, _BUDGET_TTL_SECONDS)
        await pipe.execute()
        LLM_REQUESTS.labels(response.provider, "ok").inc()
        LLM_TOKENS.labels(response.provider, "input").inc(response.usage.input_tokens)
        LLM_TOKENS.labels(response.provider, "output").inc(response.usage.output_tokens)
        LLM_COST.labels(response.provider).inc(response.cost_usd)

    async def aclose(self) -> None:
        for provider in self.providers:
            await provider.aclose()


def build_gateway(settings: Settings, redis: Redis, symbols: dict[str, str] | None = None) -> LLMGateway:
    """Wire providers from settings. ``symbols`` (symbol -> name) is only used by the offline mock."""
    providers: list[LLMProvider] = []
    choice = settings.resolved_llm_provider
    if choice == "mock":
        from bazaarpulse.agents.llm.mock_provider import MockProvider

        providers.append(MockProvider(symbols or {}))
    else:
        if settings.anthropic_api_key or choice == "anthropic":
            from bazaarpulse.agents.llm.anthropic_provider import AnthropicProvider

            providers.append(
                AnthropicProvider(
                    api_key=settings.anthropic_api_key,
                    model=settings.anthropic_model,
                    effort=settings.anthropic_effort,
                    refusal_fallback=settings.anthropic_refusal_fallback,
                    timeout=settings.llm_timeout_seconds,
                )
            )
        if settings.groq_api_key:
            from bazaarpulse.agents.llm.groq_provider import GroqProvider

            providers.append(
                GroqProvider(api_key=settings.groq_api_key, model=settings.groq_model, base_url=settings.groq_base_url)
            )
        # The configured provider goes first; any other real provider is its failover.
        providers.sort(key=lambda p: p.name != choice)
    return LLMGateway(
        providers,
        redis=redis,
        prefix=settings.redis_prefix,
        prices=settings.llm_prices,
        daily_token_budget=settings.agent_daily_token_budget,
    )
