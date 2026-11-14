"""The ledger's guarantees, including the ones the database enforces on its own."""

from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from bazaarpulse.bus import RedisBus
from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.trading.ledger import InsufficientFunds, LedgerError, Leg
from tests.trading_helpers import Rig, build_rig


@pytest.fixture
def rig(db: Database, bus: RedisBus, settings: Settings) -> Rig:
    return build_rig(db, bus, settings)


async def test_signup_deposit_is_balanced_and_idempotent(rig: Rig) -> None:
    user = await rig.user()
    assert await rig.cash(user) == Decimal("1000000")
    # Running onboarding again (a retried request) must not double the money.
    async with rig.db.transaction() as session:
        await rig.ledger.deposit(session, user, Decimal("1000000"), idempotency_key=f"deposit:signup:{user}")
    assert await rig.cash(user) == Decimal("1000000")
    async with rig.db.session() as session:
        audit = await rig.ledger.audit(session)
    assert audit["ok"] and audit["transactions"] == 1 and audit["entries"] == 2


async def test_unbalanced_legs_are_refused_before_touching_the_database(rig: Rig) -> None:
    user = await rig.user()
    async with rig.db.transaction() as session:
        cash = await rig.ledger.cash_account(session, user)
        funding = await rig.ledger.account(session, user_id=None, kind="funding", asset="INR")
        with pytest.raises(LedgerError, match="does not balance"):
            await rig.ledger.post(session, kind="deposit", legs=[Leg(cash, Decimal(10)), Leg(funding, Decimal(-9))])
        with pytest.raises(LedgerError):
            await rig.ledger.post(session, kind="deposit", legs=[Leg(cash, Decimal(0))])
    assert await rig.cash(user) == Decimal("1000000")


async def test_database_rejects_an_unbalanced_transaction_written_behind_the_ledgers_back(rig: Rig) -> None:
    user = await rig.user()
    with pytest.raises((IntegrityError, DBAPIError), match="unbalanced"):
        async with rig.db.transaction() as session:
            cash = await rig.ledger.cash_account(session, user)
            tx_id = uuid.uuid4()
            await session.execute(
                text("INSERT INTO ledger_transactions (id, kind) VALUES (:id, 'hack')"), {"id": tx_id}
            )
            # One-sided entry: money from nowhere. The deferred trigger fires at COMMIT.
            await session.execute(
                text(
                    "INSERT INTO ledger_entries (transaction_id, account_id, asset, amount)"
                    " VALUES (:tx, :acct, 'INR', 500)"
                ),
                {"tx": tx_id, "acct": cash.id},
            )
    async with rig.db.session() as session:
        assert (await rig.ledger.audit(session))["entries"] == 2  # nothing from the bad transaction survived


async def test_ledger_entries_cannot_be_edited_or_deleted(rig: Rig) -> None:
    await rig.user()
    for statement in ("UPDATE ledger_entries SET amount = amount * 2", "DELETE FROM ledger_entries"):
        with pytest.raises((IntegrityError, DBAPIError), match="append-only"):
            async with rig.db.transaction() as session:
                await session.execute(text(statement))


async def test_overdraft_is_refused_and_the_transaction_stays_usable(rig: Rig) -> None:
    rich, poor = await rig.user(), await rig.user(funded=False)
    async with rig.db.transaction() as session:
        a = await rig.ledger.cash_account(session, rich)
        b = await rig.ledger.cash_account(session, poor)
        with pytest.raises(InsufficientFunds):
            await rig.ledger.post(session, kind="transfer", legs=[Leg(b, Decimal(-1)), Leg(a, Decimal(1))])
        # The failed posting rolled back to its savepoint; this one, in the same transaction, still works.
        await rig.ledger.post(session, kind="transfer", legs=[Leg(a, Decimal(-250)), Leg(b, Decimal(250))])
    assert await rig.cash(rich) == Decimal("999750") and await rig.cash(poor) == Decimal("250")


async def test_concurrent_transfers_lose_nothing_and_never_deadlock(rig: Rig) -> None:
    a, b = await rig.user(), await rig.user()

    async def transfer(src: uuid.UUID, dst: uuid.UUID, amount: int) -> None:
        async with rig.db.transaction() as session:
            s = await rig.ledger.cash_account(session, src)
            d = await rig.ledger.cash_account(session, dst)
            await rig.ledger.post(session, kind="transfer", legs=[Leg(s, Decimal(-amount)), Leg(d, Decimal(amount))])

    # Opposite directions at once: the classic recipe for a deadlock, if locks were taken in leg order.
    await asyncio.gather(*(transfer(a, b, 7) if i % 2 else transfer(b, a, 3) for i in range(40)))

    assert await rig.cash(a) == Decimal("1000000") - 20 * 7 + 20 * 3
    assert await rig.cash(b) == Decimal("1000000") + 20 * 7 - 20 * 3
    async with rig.db.session() as session:
        assert (await rig.ledger.audit(session))["ok"]
