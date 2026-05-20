"""Risk gate pipeline — cheap-first, short-circuits on first reject.

Each gate is an async callable returning `None` (pass) or `Reject(code, detail)`.
The pipeline iterates them in declared order and returns at the first reject;
the metrics middleware wraps every gate so we get pass/{reject_code} counters
for free.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Protocol

from poly_terminal.shared.typed_reject import Reject

logger = logging.getLogger(__name__)


class IntentLike(Protocol):
    """Structural typing — gates accept any object exposing the fields they need."""

    intent_id: str


Gate = Callable[[object], Awaitable[Reject | None]]


class GatePipeline:
    """Ordered list of gates with optional middleware (metrics, logging)."""

    def __init__(
        self,
        gates: list[tuple[str, Gate]],
        middleware: Callable[[str, Gate], Gate] | None = None,
    ) -> None:
        if middleware is None:
            self._gates = list(gates)
        else:
            self._gates = [(name, middleware(name, fn)) for name, fn in gates]

    @property
    def names(self) -> list[str]:
        return [n for n, _ in self._gates]

    async def evaluate(self, intent: object) -> tuple[bool, Reject | None]:
        """Run gates in order; first reject wins. Returns (allowed, reject_or_none)."""
        for name, gate in self._gates:
            try:
                result = await gate(intent)
            except Exception:
                logger.exception("gate %s crashed — treating as reject", name)
                return False, Reject(code="gate_crashed", detail=name)
            if result is not None:
                return False, result
        return True, None
