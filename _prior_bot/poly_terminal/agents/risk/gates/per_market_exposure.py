"""Gate 6: per-market exposure cap."""

from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable

from poly_terminal.shared.typed_reject import Reject

ExposureReader = Callable[[str], Awaitable[Decimal]]  # market_id -> usd


class PerMarketExposureGate:
    def __init__(self, max_usd: Decimal, reader: ExposureReader) -> None:
        self._max = max_usd
        self._read = reader

    async def __call__(self, intent: object) -> Reject | None:
        market_id = str(getattr(intent, "market_id", ""))
        size = Decimal(str(getattr(intent, "size_usd", 0)))
        current = await self._read(market_id)
        projected = current + size
        if projected > self._max:
            return Reject(
                code="per_market_exposure_exceeded",
                detail=f"{projected} > {self._max}",
            )
        return None
