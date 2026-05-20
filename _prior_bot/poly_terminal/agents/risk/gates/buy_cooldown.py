"""Gate: cooldown that pauses BUYs only (SELL exits unaffected).

Used by CashGuardAgent to halt new BUY entries when cumulative
cash+portfolio value crosses a threshold, without blocking SELL
exits (those still need to fire to capture pending profits or
cut losses).

State is held externally — the gate just reads a `paused_until_ts`
from the supplied callable on every check. Setting paused_until_ts
to a future epoch second pauses; setting to <= now resumes.
"""

from __future__ import annotations

import time
from typing import Callable

from poly_terminal.shared.typed_reject import Reject


class BuyCooldownGate:
    def __init__(self, paused_until_getter: Callable[[], int]) -> None:
        self._getter = paused_until_getter

    async def __call__(self, intent: object) -> Reject | None:
        paused_until = int(self._getter() or 0)
        if paused_until <= 0:
            return None
        now = int(time.time())
        if now < paused_until:
            return Reject(
                code="buy_cooldown_active",
                detail=f"buys paused until {paused_until} ({paused_until - now}s left)",
            )
        return None
