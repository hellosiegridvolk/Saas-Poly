"""Gate 14: limit price must align to current tick size."""

from __future__ import annotations

from decimal import Decimal

from poly_terminal.shared.typed_reject import Reject


class TickSizeAlignmentGate:
    """Rejects intents whose `limit_price` is not a multiple of `tick_size`.

    Polymarket changes tick size intraday (Nautilus #2980); the Strategy /
    Execution Agents must capture the current tick on the intent. This gate
    is the safety net.
    """

    async def __call__(self, intent: object) -> Reject | None:
        tick = getattr(intent, "tick_size", None)
        price = getattr(intent, "limit_price", None)
        if tick is None or price is None:
            return Reject(code="missing_tick_or_price")
        tick_d = Decimal(str(tick))
        price_d = Decimal(str(price))
        if tick_d <= 0:
            return Reject(code="invalid_tick", detail=str(tick_d))
        # Decimal % is exact; non-zero remainder means misalignment.
        remainder = price_d % tick_d
        if remainder != Decimal("0"):
            return Reject(
                code="price_not_tick_aligned",
                detail=f"{price_d} % {tick_d} = {remainder}",
            )
        return None
