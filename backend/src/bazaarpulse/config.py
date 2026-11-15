"""Typed settings, read from the environment (or a local ``.env``).

Every knob that changes behaviour, risk or cost lives here, so the whole
posture of a deployment is visible in one place and tests can build a
``Settings`` with exactly the values they need.
"""

from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEV_JWT_SECRET = "dev-only-secret-change-me-dev-only-secret"  # noqa: S105 - refused outside dev/test


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    env: Literal["dev", "test", "prod"] = "dev"
    log_level: str = "INFO"
    log_json: bool = False

    # --- Infrastructure ----------------------------------------------------
    database_url: str = "postgresql+asyncpg://bazaar:bazaar@127.0.0.1:5432/bazaarpulse"
    redis_url: str = "redis://127.0.0.1:6379/0"
    # Prefix for every Redis key and stream; lets tests and environments share one Redis.
    redis_prefix: str = "bp:"
    db_pool_size: int = 10

    # --- API ---------------------------------------------------------------
    jwt_secret: str = DEV_JWT_SECRET
    jwt_ttl_minutes: int = 12 * 60
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:5173"])
    seed_demo_user: bool = True
    # Which background workers the API process also runs. "all" is the
    # single-process dev mode; in Kubernetes each worker is its own Deployment.
    embedded_workers: Literal["all", "none"] = "all"

    # --- Market data -------------------------------------------------------
    market_provider: Literal["simulator", "yahoo", "finnhub"] = "simulator"
    finnhub_api_key: str | None = None
    # How often each symbol is refreshed. Yahoo's endpoint is unofficial; polling faster invites rate limits.
    yahoo_poll_seconds: float = 30.0
    # Ticks are spread over this many stream partitions by symbol hash. One
    # processor owns a partition at a time, which keeps per-symbol ordering.
    tick_partitions: int = 1
    tick_stream_maxlen: int = 200_000
    quote_conflate_ms: int = 250
    sim_tick_interval_ms: int = 250
    sim_seed: int | None = None
    sim_shock_per_hour: float = 3.0
    history_backfill_minutes: int = 6 * 60

    # --- Anomaly detection -------------------------------------------------
    anomaly_bar_seconds: int = 5
    anomaly_window_bars: int = 60
    anomaly_zscore: float = 4.0
    anomaly_min_move_pct: float = 0.6
    anomaly_cooldown_seconds: int = 90

    # --- Paper trading -----------------------------------------------------
    currency: str = "INR"
    starting_cash: Decimal = Decimal("1000000")
    fee_bps: Decimal = Decimal("3")  # 0.03% of notional per fill
    slippage_bps: Decimal = Decimal("2")  # applied against the trader on market/stop fills
    max_order_notional: Decimal = Decimal("500000")
    max_position_pct: Decimal = Decimal("40")  # of account equity, post-trade
    price_band_pct: Decimal = Decimal("10")  # limit price must be within this of last
    max_quote_age_seconds: float = 20.0
    max_orders_per_minute: int = 30
    proposal_ttl_minutes: int = 15

    # --- LLM ---------------------------------------------------------------
    # auto = Claude if ANTHROPIC_API_KEY is set, else Groq if GROQ_API_KEY is set, else the offline mock.
    llm_provider: Literal["auto", "anthropic", "groq", "mock"] = "auto"
    anthropic_api_key: str | None = None
    anthropic_model: str = "claude-opus-5-5"
    anthropic_effort: Literal["low", "medium", "high", "xhigh", "max"] = "medium"
    # Server-side refusal fallback (beta): if a safety classifier declines a
    # request, the API retries it on another Claude model inside the same call.
    anthropic_refusal_fallback: bool = True
    groq_api_key: str | None = None
    groq_base_url: str = "https://api.groq.com/openai/v1"
    groq_model: str = "llama-3.3-70b-versatile"
    llm_max_output_tokens: int = 16_000
    llm_timeout_seconds: float = 120.0
    # USD per 1M tokens as (input, output). Configuration, not truth: check the provider's pricing page.
    llm_prices: dict[str, tuple[float, float]] = Field(
        default_factory=lambda: {
            "claude-opus-5-5": (4.0, 20.0),
            "claude-sonnet-5-5": (2.0, 10.0),
            "claude-haiku-5-5": (0.10, 0.50),
            "llama-3.3-70b-versatile": (0.59, 0.79),
        }
    )

    # --- Agents ------------------------------------------------------------
    agent_max_steps: int = 8
    agent_concurrency: int = 4
    agent_daily_token_budget: int = 400_000  # per user
    sentinel_enabled: bool = True
    sentinel_cooldown_seconds: int = 180  # per symbol
    # The detector fires early in a move. Waiting a few seconds lets the move finish and the headline
    # arrive, so the brief describes what happened rather than its first half-second.
    sentinel_settle_seconds: float = 8.0
    sentinel_max_briefs_per_hour: int = 20  # hard cap on autonomous LLM spend

    @model_validator(mode="after")
    def _refuse_dev_secret_in_prod(self) -> Settings:
        if self.env == "prod" and self.jwt_secret == DEV_JWT_SECRET:
            raise ValueError("JWT_SECRET must be set to a real secret when ENV=prod")
        return self

    @property
    def resolved_llm_provider(self) -> Literal["anthropic", "groq", "mock"]:
        if self.llm_provider != "auto":
            return self.llm_provider
        if self.anthropic_api_key:
            return "anthropic"
        if self.groq_api_key:
            return "groq"
        return "mock"


@lru_cache
def get_settings() -> Settings:
    return Settings()
