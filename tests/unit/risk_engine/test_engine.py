"""Engine orchestration: fail-fast, telemetry, duration measurement."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

import pytest

from services.risk_engine import RiskEngine
from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal
from tests.unit.risk_engine.conftest import FakeRiskContext, make_signal


@dataclass
class RecordingMetrics:
    calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    def increment(self, name: str, value: float = 1.0, **tags: str) -> None:
        self.calls.append((name, dict(tags)))

    def observe(self, name: str, value: float, **tags: str) -> None:
        self.calls.append((name, dict(tags)))

    def gauge(self, name: str, value: float, **tags: str) -> None:
        self.calls.append((name, dict(tags)))


class _AlwaysPass:
    name = "always_pass"
    invoked: bool = False

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        self.invoked = True
        return passed_decision(self.name)


class _AlwaysFail:
    name = "always_fail"
    invoked: bool = False

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        self.invoked = True
        return failed_decision(self.name, "nope")


async def _ctx_factory_for(ctx: FakeRiskContext) -> RiskContext:
    return ctx


def _make_factory(ctx: FakeRiskContext):
    async def factory(user_id: UUID) -> RiskContext:
        return ctx

    return factory


class TestRiskEngine:
    async def test_all_pass_yields_approved_intent(
        self, fake_ctx: FakeRiskContext
    ) -> None:
        engine = RiskEngine([_AlwaysPass(), _AlwaysPass()], _make_factory(fake_ctx))
        result = await engine.evaluate(make_signal())
        assert result.approved
        assert len(result.decisions) == 2
        assert all(d.passed for d in result.decisions)

    async def test_fail_fast_stops_on_first_rejection(
        self, fake_ctx: FakeRiskContext
    ) -> None:
        g1, g2, g3 = _AlwaysPass(), _AlwaysFail(), _AlwaysPass()
        engine = RiskEngine([g1, g2, g3], _make_factory(fake_ctx))
        result = await engine.evaluate(make_signal())
        assert not result.approved
        assert g1.invoked
        assert g2.invoked
        assert not g3.invoked
        assert len(result.decisions) == 2

    async def test_intent_links_back_to_signal(self, fake_ctx: FakeRiskContext) -> None:
        engine = RiskEngine([_AlwaysPass()], _make_factory(fake_ctx))
        signal = make_signal()
        result = await engine.evaluate(signal)
        assert result.intent.signal_id == signal.signal_id
        assert result.intent.user_id == signal.user_id
        assert result.intent.size == signal.size
        assert result.intent.limit_price == signal.limit_price

    async def test_telemetry_emitted_per_gate(
        self, fake_ctx: FakeRiskContext
    ) -> None:
        metrics = RecordingMetrics()
        engine = RiskEngine(
            [_AlwaysPass(), _AlwaysFail()],
            _make_factory(fake_ctx),
            metrics=metrics,
        )
        await engine.evaluate(make_signal())
        names = [c[0] for c in metrics.calls]
        assert names.count("risk_gate_decisions_total") == 2
        outcomes = [c[1].get("outcome") for c in metrics.calls if c[0] == "risk_gate_decisions_total"]
        assert outcomes == ["passed", "rejected"]

    async def test_duration_attached(self, fake_ctx: FakeRiskContext) -> None:
        engine = RiskEngine([_AlwaysPass()], _make_factory(fake_ctx))
        result = await engine.evaluate(make_signal())
        assert result.decisions[0].duration_ms >= 0


@pytest.mark.parametrize(
    "approved_at",
    [datetime(2026, 5, 21, tzinfo=UTC)],
)
async def test_intent_has_utc_timestamp(
    fake_ctx: FakeRiskContext, approved_at: datetime
) -> None:
    engine = RiskEngine([_AlwaysPass()], _make_factory(fake_ctx))
    result = await engine.evaluate(make_signal())
    assert result.intent.approved_at.tzinfo is not None
