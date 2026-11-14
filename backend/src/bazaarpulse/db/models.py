"""ORM models.

The schema itself is owned by the Alembic migration (``alembic/versions``),
which also creates what an ORM cannot express: the time-partitioned candles
table, the ledger's constraint triggers and the news full-text index. A test
checks that these models and the migrated database agree.

Money is always ``Decimal`` / ``NUMERIC``; a float never touches a balance.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from bazaarpulse.db.base import Base

Money = Numeric(28, 8)
Price = Numeric(18, 4)


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


def _created_at() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now())


# --- Identity ---------------------------------------------------------------


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = _uuid_pk()
    email: Mapped[str] = mapped_column(String(254), unique=True)
    display_name: Mapped[str] = mapped_column(String(80))
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(16), default="user")
    created_at: Mapped[datetime] = _created_at()


# --- Market data ------------------------------------------------------------


class Instrument(Base):
    __tablename__ = "instruments"

    symbol: Mapped[str] = mapped_column(String(24), primary_key=True)
    name: Mapped[str] = mapped_column(String(120))
    exchange: Mapped[str] = mapped_column(String(16))
    sector: Mapped[str] = mapped_column(String(40))
    currency: Mapped[str] = mapped_column(String(8))
    tick_size: Mapped[Decimal] = mapped_column(Price)
    lot_size: Mapped[int] = mapped_column(Integer, default=1)
    # Reference price: the previous close for live feeds, the starting level for the simulator.
    ref_price: Mapped[Decimal] = mapped_column(Price)
    provider_symbol: Mapped[str | None] = mapped_column(String(32))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class Candle(Base):
    """One OHLCV bar. Only 1-minute bars are stored; wider bars are aggregated on read.

    The table is range-partitioned by month on ``ts`` (see the migration).
    """

    __tablename__ = "candles"

    symbol: Mapped[str] = mapped_column(String(24), primary_key=True)
    interval: Mapped[str] = mapped_column(String(4), primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    open: Mapped[Decimal] = mapped_column(Price)
    high: Mapped[Decimal] = mapped_column(Price)
    low: Mapped[Decimal] = mapped_column(Price)
    close: Mapped[Decimal] = mapped_column(Price)
    volume: Mapped[int] = mapped_column(BigInteger)
    vwap: Mapped[Decimal] = mapped_column(Price)
    trades: Mapped[int] = mapped_column(Integer)


class News(Base):
    __tablename__ = "news"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str | None] = mapped_column(String(24))
    headline: Mapped[str] = mapped_column(Text)
    summary: Mapped[str] = mapped_column(Text, default="")
    source: Mapped[str] = mapped_column(String(60))
    url: Mapped[str | None] = mapped_column(Text)
    sentiment: Mapped[Decimal | None] = mapped_column(Numeric(4, 3))
    is_simulated: Mapped[bool] = mapped_column(Boolean, default=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    dedupe_key: Mapped[str] = mapped_column(String(64), unique=True)
    # ``tsv`` (generated tsvector) exists in the database for search; it is not mapped.


class Anomaly(Base):
    __tablename__ = "anomalies"

    id: Mapped[uuid.UUID] = _uuid_pk()
    symbol: Mapped[str] = mapped_column(String(24))
    kind: Mapped[str] = mapped_column(String(24))  # price_spike | price_drop | volume_surge
    severity: Mapped[str] = mapped_column(String(8))  # low | medium | high
    details: Mapped[dict[str, Any]] = mapped_column(JSONB)
    brief: Mapped[str | None] = mapped_column(Text)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


# --- Ledger -----------------------------------------------------------------


class LedgerAccount(Base):
    """A bucket holding exactly one asset: rupees, or shares of one symbol.

    ``user_id`` is NULL for the system's own accounts (market counterparty,
    fees, funding). ``balance`` is a cached sum of the account's entries,
    updated in the same transaction that inserts them.
    """

    __tablename__ = "ledger_accounts"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    kind: Mapped[str] = mapped_column(String(16))  # cash | position | market | fees | funding
    asset: Mapped[str] = mapped_column(String(24))
    balance: Mapped[Decimal] = mapped_column(Money, default=Decimal(0))
    created_at: Mapped[datetime] = _created_at()


class LedgerTransaction(Base):
    __tablename__ = "ledger_transactions"

    id: Mapped[uuid.UUID] = _uuid_pk()
    kind: Mapped[str] = mapped_column(String(16))  # deposit | trade
    reference: Mapped[str | None] = mapped_column(String(80))
    idempotency_key: Mapped[str | None] = mapped_column(String(120), unique=True)
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = _created_at()


class LedgerEntry(Base):
    """One leg of a transaction. Append-only: a trigger rejects UPDATE and DELETE."""

    __tablename__ = "ledger_entries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("ledger_transactions.id"))
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("ledger_accounts.id"))
    asset: Mapped[str] = mapped_column(String(24))
    amount: Mapped[Decimal] = mapped_column(Money)  # signed: positive increases the account
    created_at: Mapped[datetime] = _created_at()


# --- Trading ----------------------------------------------------------------


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    symbol: Mapped[str] = mapped_column(ForeignKey("instruments.symbol"))
    side: Mapped[str] = mapped_column(String(4))  # buy | sell
    type: Mapped[str] = mapped_column(String(8))  # market | limit | stop
    quantity: Mapped[int] = mapped_column(BigInteger)
    limit_price: Mapped[Decimal | None] = mapped_column(Price)
    stop_price: Mapped[Decimal | None] = mapped_column(Price)
    status: Mapped[str] = mapped_column(String(12))  # open | filled | cancelled | rejected
    reserved_cash: Mapped[Decimal] = mapped_column(Money, default=Decimal(0))
    filled_quantity: Mapped[int] = mapped_column(BigInteger, default=0)
    avg_fill_price: Mapped[Decimal | None] = mapped_column(Price)
    reject_reason: Mapped[str | None] = mapped_column(Text)
    risk_report: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    idempotency_key: Mapped[str] = mapped_column(String(120))
    source: Mapped[str] = mapped_column(String(8), default="user")  # user | agent
    proposal_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    created_at: Mapped[datetime] = _created_at()
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Fill(Base):
    __tablename__ = "fills"

    id: Mapped[uuid.UUID] = _uuid_pk()
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id"), unique=True)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    symbol: Mapped[str] = mapped_column(String(24))
    side: Mapped[str] = mapped_column(String(4))
    quantity: Mapped[int] = mapped_column(BigInteger)
    price: Mapped[Decimal] = mapped_column(Price)
    fee: Mapped[Decimal] = mapped_column(Money)
    ledger_transaction_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("ledger_transactions.id"))
    created_at: Mapped[datetime] = _created_at()


class Position(Base):
    """Read model for holdings: quantity mirrors the ledger, plus cost basis and realised P&L."""

    __tablename__ = "positions"

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(24), primary_key=True)
    quantity: Mapped[int] = mapped_column(BigInteger, default=0)
    avg_cost: Mapped[Decimal] = mapped_column(Price, default=Decimal(0))
    realized_pnl: Mapped[Decimal] = mapped_column(Money, default=Decimal(0))
    fees_paid: Mapped[Decimal] = mapped_column(Money, default=Decimal(0))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class OutboxEvent(Base):
    """Transactional outbox: events are written with the state change, published after commit."""

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(80))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _created_at()
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# --- Agents -----------------------------------------------------------------


class AgentRun(Base):
    __tablename__ = "agent_runs"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    kind: Mapped[str] = mapped_column(String(12))  # copilot | war_room | sentinel
    status: Mapped[str] = mapped_column(String(12), default="queued")  # queued | running | completed | failed
    symbol: Mapped[str | None] = mapped_column(String(24))
    input: Mapped[dict[str, Any]] = mapped_column(JSONB)
    output: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    provider: Mapped[str | None] = mapped_column(String(16))
    model: Mapped[str | None] = mapped_column(String(64))
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cost_usd: Mapped[Decimal] = mapped_column(Numeric(12, 6), default=Decimal(0))
    error: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = _created_at()
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AgentStep(Base):
    """One persisted step of a run: every model turn, tool call and tool result is auditable."""

    __tablename__ = "agent_steps"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("agent_runs.id", ondelete="CASCADE"))
    seq: Mapped[int] = mapped_column(Integer)
    agent: Mapped[str] = mapped_column(String(24))
    type: Mapped[str] = mapped_column(String(16))  # status | message | tool_call | tool_result | final | error
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _created_at()


class TradeProposal(Base):
    """A trade plan drafted by an agent. Nothing executes until the user approves it."""

    __tablename__ = "trade_proposals"

    id: Mapped[uuid.UUID] = _uuid_pk()
    run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("agent_runs.id", ondelete="SET NULL"))
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    status: Mapped[str] = mapped_column(
        String(12), default="pending"
    )  # pending|approved|rejected|expired|executed|failed
    rationale: Mapped[str] = mapped_column(Text)
    orders: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    risk_report: Mapped[dict[str, Any]] = mapped_column(JSONB)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _created_at()
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
