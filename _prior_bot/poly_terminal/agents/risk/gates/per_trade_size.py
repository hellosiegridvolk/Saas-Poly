"""Gate 2: per-trade size cap."""

from __future__ import annotations

from decimal import Decimal

from poly_terminal.shared.typed_reject import Reject


class PerTradeSizeGate:
    def __init__(self, max_position_usd: Decimal) -> None:
        self._cap = max_position_usd

    async def __call__(self, intent: object) -> Reject | None:
        size = Decimal(str(getattr(intent, "size_usd", 0)))
        if size <= 0:
            return Reject(code="non_positive_size", detail=str(size))
        if size > self._cap:
            return Reject(
                code="size_above_cap",
                detail=f"{size} > {self._cap}",
            )
        return None
