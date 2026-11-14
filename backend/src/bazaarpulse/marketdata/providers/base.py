from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol

from bazaarpulse.marketdata.types import Bar, Batch


class MarketDataProvider(Protocol):
    name: str

    def stream(self) -> AsyncIterator[Batch]:
        """Yield batches of ticks and news forever, reconnecting on its own."""
        ...

    async def backfill(self, minutes: int) -> list[Bar]:
        """One-minute bars for the recent past, oldest first; empty if the source has none."""
        ...

    async def control(self, command: dict[str, Any]) -> None:
        """Operator commands (the simulator's shock injection); live feeds ignore them."""
        ...

    async def aclose(self) -> None: ...
