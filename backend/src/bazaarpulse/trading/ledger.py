"""Double-entry ledger.

Every movement of value is a *transaction* made of *legs*. Each leg adds a
signed amount of one asset to one account, and for every asset the legs of a
transaction must sum to zero: value moves between accounts, it is never
created or destroyed. Buying 10 TCS at Rs 4,100 with a Rs 12.30 fee is:

    asset   account          amount
    INR     you: cash        -41,012.30
    INR     market           +41,000.00
    INR     fees                 +12.30
    TCS     you: position           +10
    TCS     market                  -10

Three independent layers keep this honest:

1. ``post()`` refuses unbalanced legs before touching the database.
2. A deferred constraint trigger re-checks the balance of every transaction at COMMIT.
3. A CHECK constraint stops any customer account from going negative.

So even code that bypasses this module cannot corrupt the books.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import Table, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bazaarpulse.db.models import LedgerAccount, LedgerEntry, LedgerTransaction

_ACCOUNTS = cast(Table, LedgerAccount.__table__)


class LedgerError(Exception):
    pass


class InsufficientFunds(LedgerError):
    """A customer account would have gone below zero."""


@dataclass(frozen=True, slots=True)
class Account:
    """An immutable view of an account. ``balance`` is the value at the moment it was read."""

    id: uuid.UUID
    user_id: uuid.UUID | None
    kind: str
    asset: str
    balance: Decimal


@dataclass(frozen=True, slots=True)
class Leg:
    account: Account
    amount: Decimal  # positive adds to the account, negative takes from it


class Ledger:
    def __init__(self, currency: str) -> None:
        self.currency = currency

    # --- Accounts -----------------------------------------------------------

    async def account(
        self, session: AsyncSession, *, user_id: uuid.UUID | None, kind: str, asset: str, lock: bool = False
    ) -> Account:
        """Get or create. ``user_id=None`` is one of the system's own accounts."""
        query = select(
            _ACCOUNTS.c.id, _ACCOUNTS.c.user_id, _ACCOUNTS.c.kind, _ACCOUNTS.c.asset, _ACCOUNTS.c.balance
        ).where(_ACCOUNTS.c.user_id.is_not_distinct_from(user_id), _ACCOUNTS.c.kind == kind, _ACCOUNTS.c.asset == asset)
        if lock:
            query = query.with_for_update()
        row = (await session.execute(query)).one_or_none()
        if row is None:
            # ON CONFLICT DO NOTHING makes a concurrent first use safe: one insert wins, both then read it.
            await session.execute(
                insert(LedgerAccount)
                .values(id=uuid.uuid4(), user_id=user_id, kind=kind, asset=asset, balance=Decimal(0))
                .on_conflict_do_nothing(index_elements=["user_id", "kind", "asset"])
            )
            row = (await session.execute(query)).one()
        return Account(row.id, row.user_id, row.kind, row.asset, row.balance)

    async def cash_account(self, session: AsyncSession, user_id: uuid.UUID, *, lock: bool = False) -> Account:
        """The user's cash account.

        ``lock=True`` takes ``FOR UPDATE`` on that one row. It is the per-user
        mutex: it serialises everything that changes or reserves a user's
        money, without blocking any other user.
        """
        return await self.account(session, user_id=user_id, kind="cash", asset=self.currency, lock=lock)

    # --- Posting ------------------------------------------------------------

    async def post(
        self,
        session: AsyncSession,
        *,
        kind: str,
        legs: list[Leg],
        reference: str | None = None,
        idempotency_key: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> uuid.UUID:
        """Record one balanced transaction inside the caller's database transaction; returns its id.

        Raises ``InsufficientFunds`` (leaving the caller's transaction usable)
        if a customer account would go negative.
        """
        legs = [leg for leg in legs if leg.amount != 0]
        totals: dict[str, Decimal] = defaultdict(Decimal)
        for leg in legs:
            totals[leg.account.asset] += leg.amount
        unbalanced = {asset: total for asset, total in totals.items() if total != 0}
        if unbalanced or len(legs) < 2:
            raise LedgerError(f"transaction does not balance: {unbalanced or 'needs at least two legs'}")

        if idempotency_key is not None:
            existing = await session.scalar(
                select(LedgerTransaction.id).where(LedgerTransaction.idempotency_key == idempotency_key)
            )
            if existing is not None:
                return existing

        per_account: dict[uuid.UUID, Decimal] = defaultdict(Decimal)
        for leg in legs:
            # Only customer balances are cached and locked. The market and fee
            # accounts take part in every single trade; locking them would
            # make every trade in the system wait in one queue (the "hot
            # account" problem). Their balance is derived from entries on demand.
            if leg.account.user_id is not None:
                per_account[leg.account.id] += leg.amount

        tx_id = uuid.uuid4()
        try:
            # A savepoint: an overdraft undoes this posting only, not the caller's whole transaction.
            async with session.begin_nested():
                await session.execute(
                    insert(LedgerTransaction).values(
                        id=tx_id, kind=kind, reference=reference, idempotency_key=idempotency_key, meta=meta or {}
                    )
                )
                await session.execute(
                    insert(LedgerEntry),
                    [
                        {
                            "transaction_id": tx_id,
                            "account_id": leg.account.id,
                            "asset": leg.account.asset,
                            "amount": leg.amount,
                        }
                        for leg in legs
                    ],
                )
                # Always lock accounts in the same (sorted) order, so two postings
                # touching the same pair of accounts can never deadlock each other.
                for account_id in sorted(per_account):
                    await session.execute(
                        update(_ACCOUNTS)
                        .where(_ACCOUNTS.c.id == account_id)
                        .values(balance=_ACCOUNTS.c.balance + per_account[account_id])
                    )
        except IntegrityError as e:
            if "customer_balance_non_negative" in str(e.orig):
                raise InsufficientFunds("account balance would go negative") from e
            raise
        return tx_id

    async def deposit(
        self, session: AsyncSession, user_id: uuid.UUID, amount: Decimal, *, idempotency_key: str
    ) -> None:
        """Paper capital: moved from the system's funding account to the user's cash account."""
        cash = await self.cash_account(session, user_id)
        funding = await self.account(session, user_id=None, kind="funding", asset=self.currency)
        await self.post(
            session,
            kind="deposit",
            legs=[Leg(funding, -amount), Leg(cash, amount)],
            idempotency_key=idempotency_key,
            reference=f"user:{user_id}",
        )

    async def deposits(self, session: AsyncSession, cash_account_id: uuid.UUID) -> Decimal:
        total = await session.scalar(
            select(func.coalesce(func.sum(LedgerEntry.amount), 0))
            .join(LedgerTransaction, LedgerTransaction.id == LedgerEntry.transaction_id)
            .where(LedgerEntry.account_id == cash_account_id, LedgerTransaction.kind == "deposit")
        )
        return Decimal(total or 0)

    # --- Audit --------------------------------------------------------------

    async def audit(self, session: AsyncSession) -> dict[str, Any]:
        """Prove the books: every asset nets to zero, and cached balances match their entries."""
        imbalances = (
            await session.execute(
                text("SELECT asset, sum(amount) AS total FROM ledger_entries GROUP BY asset HAVING sum(amount) <> 0")
            )
        ).all()
        drift = (
            await session.execute(
                text(
                    """
                    SELECT a.id, a.balance, coalesce(sum(e.amount), 0) AS actual
                      FROM ledger_accounts a LEFT JOIN ledger_entries e ON e.account_id = a.id
                     WHERE a.user_id IS NOT NULL
                     GROUP BY a.id, a.balance
                    HAVING a.balance <> coalesce(sum(e.amount), 0)
                    """
                )
            )
        ).all()
        counts = (
            await session.execute(
                text("SELECT (SELECT count(*) FROM ledger_transactions), (SELECT count(*) FROM ledger_entries)")
            )
        ).one()
        return {
            "ok": not imbalances and not drift,
            "transactions": counts[0],
            "entries": counts[1],
            "unbalanced_assets": [{"asset": r.asset, "total": str(r.total)} for r in imbalances],
            "balance_drift": [{"account": str(r.id), "cached": str(r.balance), "actual": str(r.actual)} for r in drift],
        }
