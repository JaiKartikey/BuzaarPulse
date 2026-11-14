"""Trade proposals: the human-in-the-loop gate between an agent's idea and a real order.

An agent can only *write a proposal*. Turning it into orders takes a separate,
authenticated call from the user. This puts the approval outside the model's
reach entirely: no prompt, however cleverly injected through a news headline,
can make the model approve its own plan, because approval is not a tool.

Risk is checked twice, on purpose: when the plan is drafted (so the agent gets
feedback and the user sees a vetted plan) and again when each order is placed
on approval (because prices and balances have moved since).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import select, update

from bazaarpulse.config import Settings
from bazaarpulse.db import Database
from bazaarpulse.db.models import TradeProposal
from bazaarpulse.trading.orders import OrderService, serialize_order, user_event
from bazaarpulse.trading.risk import OrderRequest

MAX_ORDERS_PER_PLAN = 8


class ProposalError(Exception):
    pass


def serialize_proposal(p: TradeProposal) -> dict[str, Any]:
    return {
        "id": str(p.id),
        "run_id": str(p.run_id) if p.run_id else None,
        "status": p.status,
        "rationale": p.rationale,
        "orders": p.orders,
        "risk_report": p.risk_report,
        "result": p.result,
        "created_at": p.created_at.timestamp(),
        "expires_at": p.expires_at.timestamp(),
        "decided_at": p.decided_at.timestamp() if p.decided_at else None,
    }


def parse_order(raw: dict[str, Any]) -> OrderRequest:
    try:
        limit = raw.get("limit_price")
        order_type = str(raw.get("order_type") or raw.get("type") or "market").lower()
        return OrderRequest(
            symbol=str(raw["symbol"]).upper(),
            side=str(raw["side"]).lower(),
            type=order_type,
            quantity=int(raw["quantity"]),
            limit_price=Decimal(str(limit)) if order_type == "limit" and limit is not None else None,
        )
    except (KeyError, ValueError, TypeError, InvalidOperation) as e:
        raise ProposalError(f"malformed order {raw!r}: {e}") from e


class ProposalService:
    def __init__(self, db: Database, orders: OrderService, settings: Settings) -> None:
        self.db = db
        self.orders = orders
        self.settings = settings

    async def create(
        self, user_id: uuid.UUID, run_id: uuid.UUID | None, orders: list[dict[str, Any]], rationale: str
    ) -> dict[str, Any]:
        """Vet a plan and store it as pending. Returns the risk verdict either way, so the agent can react."""
        if not orders or len(orders) > MAX_ORDERS_PER_PLAN:
            raise ProposalError(f"a plan needs between 1 and {MAX_ORDERS_PER_PLAN} orders")
        requests = [parse_order(o) for o in orders]
        if any(r.type not in ("market", "limit") or r.side not in ("buy", "sell") for r in requests):
            raise ProposalError("plans may only contain market or limit orders to buy or sell")

        reports = [await self.orders.preview(user_id, r) for r in requests]
        normalised = [
            {
                "symbol": r.symbol,
                "side": r.side,
                "order_type": r.type,
                "quantity": r.quantity,
                "limit_price": None if r.limit_price is None else float(r.limit_price),
                "estimated_value": float(report.notional),
            }
            for r, report in zip(requests, reports, strict=True)
        ]
        risk = {
            "passed": all(r.passed for r in reports),
            "orders": [
                {"symbol": req.symbol, "side": req.side, **report.to_dict()}
                for req, report in zip(requests, reports, strict=True)
            ],
        }
        if not risk["passed"]:
            failures = [
                f"{req.side} {req.quantity} {req.symbol}: {'; '.join(rep.failures)}"
                for req, rep in zip(requests, reports, strict=True)
                if not rep.passed
            ]
            return {"created": False, "risk_passed": False, "failures": failures}

        now = datetime.now(UTC)
        proposal = TradeProposal(
            id=uuid.uuid4(),
            run_id=run_id,
            user_id=user_id,
            status="pending",
            rationale=rationale.strip()[:2000],
            orders=normalised,
            risk_report=risk,
            created_at=now,
            expires_at=now + timedelta(minutes=self.settings.proposal_ttl_minutes),
        )
        async with self.db.transaction() as session:
            session.add(proposal)
            await session.flush()
            session.add(user_event(user_id, {"type": "proposal", "proposal": serialize_proposal(proposal)}))
        return {
            "created": True,
            "risk_passed": True,
            "proposal_id": str(proposal.id),
            "status": "pending_user_approval",
            "orders": normalised,
            "expires_in_minutes": self.settings.proposal_ttl_minutes,
        }

    async def decide(self, user_id: uuid.UUID, proposal_id: uuid.UUID, *, approve: bool) -> TradeProposal:
        now = datetime.now(UTC)
        problem: str | None = None
        async with self.db.transaction() as session:
            # One atomic claim: of two concurrent clicks (or an approve racing a reject), exactly one wins.
            proposal = (
                await session.execute(
                    update(TradeProposal)
                    .where(
                        TradeProposal.id == proposal_id,
                        TradeProposal.user_id == user_id,
                        TradeProposal.status == "pending",
                        TradeProposal.expires_at > now,
                    )
                    .values(status="approved" if approve else "rejected", decided_at=now)
                    .returning(TradeProposal)
                )
            ).scalar_one_or_none()
            if proposal is None:
                existing = await session.get(TradeProposal, proposal_id)
                if existing is None or existing.user_id != user_id:
                    problem = "proposal not found"
                elif existing.status == "pending":
                    existing.status, existing.decided_at = "expired", now
                    await session.flush()
                    session.add(user_event(user_id, {"type": "proposal", "proposal": serialize_proposal(existing)}))
                    problem = "proposal has expired; prices have moved since it was drafted"
                else:
                    problem = f"proposal is already {existing.status}"
            elif not approve:
                session.add(user_event(user_id, {"type": "proposal", "proposal": serialize_proposal(proposal)}))
        if problem is not None or proposal is None:
            raise ProposalError(problem or "proposal not found")
        if not approve:
            return proposal

        # Approved: place each order through the normal intake path, risk checks included.
        placed = []
        for index, raw in enumerate(proposal.orders):
            order = await self.orders.place(
                user_id,
                parse_order(raw),
                idempotency_key=f"proposal:{proposal.id}:{index}",  # a retried approval cannot double-place
                source="agent",
                proposal_id=proposal.id,
            )
            placed.append(serialize_order(order))

        async with self.db.transaction() as session:
            fresh = await session.get(TradeProposal, proposal.id)
            assert fresh is not None
            fresh.status = "executed" if all(o["status"] != "rejected" for o in placed) else "failed"
            fresh.result = {"orders": placed}
            await session.flush()
            session.add(user_event(user_id, {"type": "proposal", "proposal": serialize_proposal(fresh)}))
            return fresh

    async def list(self, user_id: uuid.UUID, *, limit: int = 30) -> list[dict[str, Any]]:
        async with self.db.session() as session:
            rows = (
                await session.execute(
                    select(TradeProposal)
                    .where(TradeProposal.user_id == user_id)
                    .order_by(TradeProposal.created_at.desc())
                    .limit(limit)
                )
            ).scalars()
            return [serialize_proposal(p) for p in rows]
