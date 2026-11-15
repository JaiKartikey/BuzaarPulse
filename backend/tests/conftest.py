"""Shared fixtures.

Tests run against a real Postgres and a real Redis: the things most worth
testing here (constraint triggers, row locks, consumer groups) do not exist
in fakes. `make services-up` starts both locally; CI uses service containers.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from bazaarpulse.bus import RedisBus
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.seed import seed_instruments

BACKEND_DIR = Path(__file__).resolve().parent.parent
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://bazaar:bazaar@127.0.0.1:5432/bazaarpulse_test"
)
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:6379/15")

# Tables wiped between tests. Instruments are reference data, seeded once.
_MUTABLE_TABLES = (
    "trade_proposals, agent_steps, agent_runs, outbox, positions, fills, orders, "
    "ledger_entries, ledger_transactions, ledger_accounts, anomalies, news, candles, users"
)


def make_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "env": "test",
        "database_url": TEST_DATABASE_URL,
        "redis_url": TEST_REDIS_URL,
        "redis_prefix": f"test:{uuid.uuid4().hex[:10]}:",
        "llm_provider": "mock",
        "anthropic_api_key": None,
        "groq_api_key": None,
        "embedded_workers": "none",
        "sim_seed": 7,
        "seed_demo_user": False,
        "sentinel_settle_seconds": 0,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)  # type: ignore[arg-type]


@pytest.fixture
def settings() -> Settings:
    return make_settings()


@pytest_asyncio.fixture(scope="session")
async def _database() -> AsyncIterator[Database]:
    """Drop everything, run the real migrations, seed reference data. Once per test session."""
    engine = create_async_engine(TEST_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
    await engine.dispose()

    cfg = Config(str(BACKEND_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(BACKEND_DIR / "alembic"))
    cfg.attributes["database_url"] = TEST_DATABASE_URL
    cfg.attributes["configure_logging"] = False
    # Alembic's env.py calls asyncio.run(), which needs a thread without a running loop.
    await asyncio.to_thread(command.upgrade, cfg, "head")

    database = Database(TEST_DATABASE_URL, pool_size=20)
    async with database.transaction() as session:
        await seed_instruments(session, "simulator")
    yield database
    await database.dispose()


@pytest_asyncio.fixture
async def db(_database: Database) -> Database:
    async with _database.engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {_MUTABLE_TABLES} RESTART IDENTITY CASCADE"))
    return _database


@pytest_asyncio.fixture(scope="session")
async def _redis() -> AsyncIterator[Redis]:
    client = Redis.from_url(TEST_REDIS_URL)
    await client.ping()
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def bus(_redis: Redis, settings: Settings) -> AsyncIterator[RedisBus]:
    """A bus under a prefix unique to this test, cleaned up afterwards."""
    bus = RedisBus(_redis, settings.redis_prefix)
    yield bus
    keys = [k async for k in _redis.scan_iter(match=f"{settings.redis_prefix}*", count=500)]
    if keys:
        await _redis.delete(*keys)
