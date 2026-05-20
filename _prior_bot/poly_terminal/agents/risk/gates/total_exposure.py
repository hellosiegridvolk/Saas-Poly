"""Gate 7: global total exposure cap."""

from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable

from poly_terminal.shared.typed_reject import Reject

TotalExposureReader = Callable[[], Awaitable[Decimal]]


class TotalExposureGate:
    def __init__(self, max_usd: Decimal, reader: TotalExposureReader) -> None:
        self._max = max_usd
        self._read = reader

    async def __call__(self, intent: object) -> Reject | None:
        size = Decimal(str(getattr(intent, "size_usd", 0)))
        current = await self._read()
        if (current + size) > self._max:
            return Reject(
                code="total_exposure_exceeded",
                detail=f"{current + size} > {self._max}",
            )
        return None
