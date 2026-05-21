"""Gates 1-4 (identity & state)."""

from __future__ import annotations

import pytest

from services.risk_engine.context import (
    RiskState,
    StrategyInstanceState,
    UserState,
)
from services.risk_engine.gates import (
    IdempotencyGate,
    KillSwitchGate,
    StrategyActiveGate,
    UserActiveGate,
)
from tests.unit.risk_engine.conftest import FakeRiskContext, make_signal


class TestUserActiveGate:
    async def test_active_passes(self, fake_ctx: FakeRiskContext) -> None:
        decision = await UserActiveGate().evaluate(make_signal(), fake_ctx)
        assert decision.passed

    async def test_inactive_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.user = UserState(user_id=fake_ctx.user_id, active=False, suspended=False)
        decision = await UserActiveGate().evaluate(make_signal(), fake_ctx)
        assert not decision.passed
        assert decision.reason == "user inactive"

    async def test_suspended_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.user = UserState(user_id=fake_ctx.user_id, active=True, suspended=True)
        decision = await UserActiveGate().evaluate(make_signal(), fake_ctx)
        assert not decision.passed
        assert decision.reason == "user suspended"


class TestStrategyActiveGate:
    async def test_active_passes(self, fake_ctx: FakeRiskContext) -> None:
        signal = make_signal()
        fake_ctx.strategy_instance = StrategyInstanceState(
            strategy_instance_id=signal.strategy_instance_id,
            user_id=signal.user_id,
            strategy_id="ninety_cent",
            mode="paper",
            status="active",
        )
        decision = await StrategyActiveGate().evaluate(signal, fake_ctx)
        assert decision.passed

    async def test_missing_instance_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.strategy_instance = None
        decision = await StrategyActiveGate().evaluate(make_signal(), fake_ctx)
        assert not decision.passed

    @pytest.mark.parametrize("status", ["paused", "archived"])
    async def test_non_active_status_fails(
        self, fake_ctx: FakeRiskContext, status: str
    ) -> None:
        signal = make_signal()
        fake_ctx.strategy_instance = StrategyInstanceState(
            strategy_instance_id=signal.strategy_instance_id,
            user_id=signal.user_id,
            strategy_id="ninety_cent",
            mode="paper",
            status=status,  # type: ignore[arg-type]
        )
        decision = await StrategyActiveGate().evaluate(signal, fake_ctx)
        assert not decision.passed


class TestKillSwitchGate:
    async def test_clean_passes(self, fake_ctx: FakeRiskContext) -> None:
        decision = await KillSwitchGate().evaluate(make_signal(), fake_ctx)
        assert decision.passed

    async def test_global_blocks(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.risk = RiskState(kill_switch_on=False, global_kill_switch_on=True)
        decision = await KillSwitchGate().evaluate(make_signal(), fake_ctx)
        assert not decision.passed
        assert "global" in (decision.reason or "")

    async def test_user_blocks(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.risk = RiskState(kill_switch_on=True, global_kill_switch_on=False)
        decision = await KillSwitchGate().evaluate(make_signal(), fake_ctx)
        assert not decision.passed
        assert "user" in (decision.reason or "")


class TestIdempotencyGate:
    async def test_new_signal_passes(self, fake_ctx: FakeRiskContext) -> None:
        decision = await IdempotencyGate().evaluate(make_signal(), fake_ctx)
        assert decision.passed

    async def test_duplicate_blocks(self, fake_ctx: FakeRiskContext) -> None:
        signal = make_signal()
        fake_ctx.processed_signal_ids.add(signal.signal_id)
        decision = await IdempotencyGate().evaluate(signal, fake_ctx)
        assert not decision.passed
