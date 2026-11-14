"""Reference data. Idempotent: safe to run on every start."""

from __future__ import annotations

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from bazaarpulse.db.models import Instrument
from bazaarpulse.marketdata.instruments import universe_for


async def seed_instruments(session: AsyncSession, provider: str) -> int:
    rows = [
        {
            "symbol": spec.symbol,
            "name": spec.name,
            "exchange": spec.exchange,
            "sector": spec.sector,
            "currency": spec.currency,
            "tick_size": spec.tick_size,
            "lot_size": 1,
            "ref_price": spec.ref_price,
            "provider_symbol": spec.provider_symbol,
            "is_active": True,
        }
        for spec in universe_for(provider)
    ]
    stmt = insert(Instrument).values(rows)
    # Re-seeding refreshes descriptive fields but never the reference price a live feed may have updated.
    stmt = stmt.on_conflict_do_update(
        index_elements=[Instrument.symbol],
        set_={"name": stmt.excluded.name, "sector": stmt.excluded.sector, "tick_size": stmt.excluded.tick_size},
    )
    await session.execute(stmt)
    return len(rows)
