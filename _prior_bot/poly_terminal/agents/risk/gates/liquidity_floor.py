"""Gate 10: dynamic liquidity floor.

Required book depth = `max(min_floor_usd, multiplier × position_usd)`.
Replaces v2's flat $5,000 floor (Bug #5).
"""

from __future__ import annotations

from decimal import Decimal

from poly_terminal.shared.typed_reject import Reject


class LiquidityFloorGate:
    def __init__(
        self,
        min_floor_usd: Decimal,
        depth_multiplier: int = 20,
    ) -> None:
        self._floor = min_floor_usd
        self._mult = depth_multiplier

    async def __call__(self, intent: object) -> Reject | None:
        depth = getattr(intent, "book_depth_usd", None)
        if depth is None:
            return Reject(code="depth_unavailable")
        size = Decimal(str(getattr(intent, "size_usd", 0)))
        depth_d = Decimal(str(depth))
        required = max(self._floor, size * Decimal(self._mult))
        if depth_d < required:
            return Reject(
                code="depth_below_dynamic_floor",
                detail=f"{depth_d} < {required}",
            )
        return None
