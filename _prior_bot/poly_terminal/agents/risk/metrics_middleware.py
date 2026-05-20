"""Metrics middleware — wraps every gate with a pass/reject counter."""

from __future__ import annotations

from typing import Awaitable, Callable

from poly_terminal.persistence.repositories.gate_metrics import GateMetricsRepo
from poly_terminal.shared.typed_reject import Reject

Gate = Callable[[object], Awaitable[Reject | None]]


def metrics_middleware(repo: GateMetricsRepo) -> Callable[[str, Gate], Gate]:
    """Return a `(gate_name, gate) -> wrapped_gate` decorator factory.

    Each invocation increments either `pass` or the reject code in
    `gate_metrics`. DB failures are swallowed — the gate's verdict is more
    important than its telemetry.
    """

    def wrap(name: str, fn: Gate) -> Gate:
        async def inner(intent: object) -> Reject | None:
            result = await fn(intent)
            outcome = "pass" if result is None else result.code
            try:
                await repo.increment(name, outcome)
            except Exception:
                pass
            return result

        return inner

    return wrap
