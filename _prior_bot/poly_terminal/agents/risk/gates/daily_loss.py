"""Gate 4: daily-loss cap."""

from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable

from poly_terminal.shared.typed_reject import Reject

DailyPnlReader = Callable[[], Awaitable[Decimal]]


class DailyLossGate:
    """Reads today's realized PnL and rejects if cumulative loss exceeds cap."""

    def __init__(self, cap_usd: Decimal, pnl_reader: DailyPnlReader) -> None:
        self._cap = cap_usd
        self._read = pnl_reader

    async def __call__(self, _intent: object) -> Reject | None:
        pnl = await self._read()
        if pnl <= -self._cap:
            return Reject(
                code="daily_loss_cap_exceeded",
                detail=f"{pnl} <= -{self._cap}",
            )
        return None
