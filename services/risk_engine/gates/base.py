"""Gate framework (spec §10.1).

A Gate is an async callable ``(Signal, RiskContext) -> GateDecision``.
Concrete gates live as small classes in this package; the engine runs
them as data so the list can be reordered or extended per strategy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from services.risk_engine.context import RiskContext
from shared.domain import GateDecision, Signal


@dataclass(frozen=True)
class GateConfig:
    """Marker base for per-gate configuration objects. Concrete gates
    declare their own dataclass subclasses with the fields they need."""


@runtime_checkable
class Gate(Protocol):
    name: str

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision: ...


def passed_decision(name: str) -> GateDecision:
    """Construct a passing GateDecision. Duration filled in by the engine."""
    return GateDecision(
        gate_name=name,
        passed=True,
        reason=None,
        measured_at=datetime.now(tz=UTC),
        duration_ms=0,
    )


def failed_decision(name: str, reason: str) -> GateDecision:
    return GateDecision(
        gate_name=name,
        passed=False,
        reason=reason,
        measured_at=datetime.now(tz=UTC),
        duration_ms=0,
    )
