"""Cross-cutting helper that emits the standard
``risk_gate_decisions_total{user, strategy, gate, outcome}`` counter
(spec §10.2, §19).
"""

from __future__ import annotations

from shared.domain import GateDecision
from shared.telemetry.metrics import Metrics


def record_gate_decision(
    metrics: Metrics, *, strategy_id: str, user_id: str, decision: GateDecision
) -> None:
    outcome = "passed" if decision.passed else "rejected"
    metrics.increment(
        "risk_gate_decisions_total",
        gate=decision.gate_name,
        outcome=outcome,
        strategy=strategy_id,
        user=user_id,
    )
    metrics.observe(
        "risk_gate_duration_ms",
        float(decision.duration_ms),
        gate=decision.gate_name,
        strategy=strategy_id,
    )
