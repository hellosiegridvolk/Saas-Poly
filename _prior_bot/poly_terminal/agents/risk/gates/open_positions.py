"""Gate 5: open-positions cap.

Closes Bug #2 (cap race, 2026-05-05): when two BUY intents arrive in the
same bus tick they both call the reader concurrently, both see the same
pre-fill count, and both pass. The optional `ledger` argument adds an
in-memory in-flight counter so the second intent observes the first one's
reservation. Read/check/reserve runs under an asyncio.Lock so the three
steps are atomic across concurrent gate evaluations.

When `ledger` is `None` the gate behaves exactly as before — useful for
unit tests that don't want to wire the bus-side release machinery.
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

from poly_terminal.agents.risk.reservation_ledger import (
    OpenPositionsReservationLedger,
)
from poly_terminal.shared.typed_reject import Reject

OpenCountReader = Callable[[], Awaitable[int]]


class OpenPositionsGate:
    def __init__(
        self,
        max_open: int,
        reader: OpenCountReader,
        ledger: OpenPositionsReservationLedger | None = None,
    ) -> None:
        self._max = max_open
        self._read = reader
        self._ledger = ledger
        self._lock = asyncio.Lock()

    async def __call__(self, intent: object) -> Reject | None:
        # Atomic read-check-reserve so concurrent intents serialize on
        # the gate's lock and the second one sees the first's reservation.
        async with self._lock:
            count = await self._read()
            in_flight = self._ledger.count() if self._ledger is not None else 0
            if count + in_flight >= self._max:
                return Reject(
                    code="open_positions_cap_exceeded",
                    detail=f"{count}+{in_flight} >= {self._max}",
                )
            if self._ledger is not None:
                intent_id = str(getattr(intent, "intent_id", "") or "")
                if intent_id:
                    self._ledger.reserve(intent_id)
        return None
