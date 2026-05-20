"""Gate 9: spread ceiling (cents)."""

from __future__ import annotations

from decimal import Decimal

from poly_terminal.shared.typed_reject import Reject


class SpreadCeilingGate:
    def __init__(self, max_cents: Decimal) -> None:
        self._max = max_cents

    async def __call__(self, intent: object) -> Reject | None:
        spread = getattr(intent, "spread_cents", None)
        if spread is None:
            return Reject(code="spread_unavailable")
        spread_d = Decimal(str(spread))
        if spread_d > self._max:
            return Reject(
                code="spread_above_ceiling",
                detail=f"{spread_d}c > {self._max}c",
            )
        return None
