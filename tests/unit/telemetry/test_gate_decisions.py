"""record_gate_decision emits the right metric shape (spec §10.2, §19)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from shared.domain import GateDecision
from shared.telemetry import record_gate_decision


@dataclass
class _Recorder:
    calls: list[tuple[str, float, dict[str, str]]] = field(default_factory=list)

    def increment(self, name: str, value: float = 1.0, **tags: str) -> None:
        self.calls.append((name, value, dict(tags)))

    def observe(self, name: str, value: float, **tags: str) -> None:
        self.calls.append((name, value, dict(tags)))

    def gauge(self, name: str, value: float, **tags: str) -> None:
        self.calls.append((name, value, dict(tags)))


def _make_decision(passed: bool) -> GateDecision:
    return GateDecision(
        gate_name="example",
        passed=passed,
        reason=None if passed else "blocked",
        measured_at=datetime.now(tz=UTC),
        duration_ms=3,
    )


def test_emits_counter_and_histogram() -> None:
    m = _Recorder()
    record_gate_decision(
        m, strategy_id="ninety_cent", user_id="user-1", decision=_make_decision(True)
    )
    names = [c[0] for c in m.calls]
    assert "risk_gate_decisions_total" in names
    assert "risk_gate_duration_ms" in names


def test_outcome_tag_reflects_pass_or_reject() -> None:
    m = _Recorder()
    record_gate_decision(
        m, strategy_id="x", user_id="u", decision=_make_decision(True)
    )
    record_gate_decision(
        m, strategy_id="x", user_id="u", decision=_make_decision(False)
    )
    outcomes = [
        c[2]["outcome"]
        for c in m.calls
        if c[0] == "risk_gate_decisions_total"
    ]
    assert outcomes == ["passed", "rejected"]


def test_duration_observed_as_float() -> None:
    m = _Recorder()
    record_gate_decision(
        m, strategy_id="x", user_id="u", decision=_make_decision(True)
    )
    duration_calls = [c for c in m.calls if c[0] == "risk_gate_duration_ms"]
    assert duration_calls
    assert isinstance(duration_calls[0][1], float)
