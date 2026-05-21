"""Gates 13-18 (sizing & exposure)."""

from __future__ import annotations

from decimal import Decimal

from services.risk_engine.gates import (
    MinSizeGate,
    MinSizeGateConfig,
    PerMarketPositionCapGate,
    PerMarketPositionCapGateConfig,
    PerOrderSizeCapGate,
    PerOrderSizeCapGateConfig,
    PerStrategyExposureCapGate,
    PerStrategyExposureCapGateConfig,
    PerUserAggregateExposureCapGate,
    PerUserAggregateExposureCapGateConfig,
    USDCSufficientGate,
)
from services.risk_engine.gates.usdc_sufficient import USDCSufficientGateConfig
from tests.unit.risk_engine.conftest import FakeRiskContext, make_signal


class TestMinSizeGate:
    async def test_at_minimum_passes(self, fake_ctx: FakeRiskContext) -> None:
        gate = MinSizeGate(MinSizeGateConfig(min_size=Decimal("5")))
        decision = await gate.evaluate(make_signal(size=Decimal("5")), fake_ctx)
        assert decision.passed

    async def test_below_minimum_fails(self, fake_ctx: FakeRiskContext) -> None:
        gate = MinSizeGate(MinSizeGateConfig(min_size=Decimal("5")))
        decision = await gate.evaluate(make_signal(size=Decimal("4")), fake_ctx)
        assert not decision.passed


class TestPerOrderSizeCapGate:
    async def test_under_cap_passes(self, fake_ctx: FakeRiskContext) -> None:
        gate = PerOrderSizeCapGate(PerOrderSizeCapGateConfig(max_size=Decimal("100")))
        decision = await gate.evaluate(make_signal(size=Decimal("50")), fake_ctx)
        assert decision.passed

    async def test_over_cap_fails(self, fake_ctx: FakeRiskContext) -> None:
        gate = PerOrderSizeCapGate(PerOrderSizeCapGateConfig(max_size=Decimal("100")))
        decision = await gate.evaluate(make_signal(size=Decimal("150")), fake_ctx)
        assert not decision.passed


class TestPerMarketPositionCapGate:
    async def test_buy_within_cap(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.position_size = Decimal("50")
        gate = PerMarketPositionCapGate(
            PerMarketPositionCapGateConfig(max_abs_position=Decimal("100"))
        )
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("30")), fake_ctx
        )
        assert decision.passed

    async def test_buy_exceeds_cap(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.position_size = Decimal("80")
        gate = PerMarketPositionCapGate(
            PerMarketPositionCapGateConfig(max_abs_position=Decimal("100"))
        )
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("30")), fake_ctx
        )
        assert not decision.passed

    async def test_sell_to_zero_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.position_size = Decimal("50")
        gate = PerMarketPositionCapGate(
            PerMarketPositionCapGateConfig(max_abs_position=Decimal("100"))
        )
        decision = await gate.evaluate(
            make_signal(side="sell", size=Decimal("50")), fake_ctx
        )
        assert decision.passed


class TestPerStrategyExposureCapGate:
    async def test_buy_within_cap(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.per_strategy_exposure = Decimal("100")
        gate = PerStrategyExposureCapGate(
            PerStrategyExposureCapGateConfig(max_exposure_usdc=Decimal("200"))
        )
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("100"), limit_price=Decimal("0.500")),
            fake_ctx,
        )
        assert decision.passed

    async def test_buy_exceeds_cap(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.per_strategy_exposure = Decimal("180")
        gate = PerStrategyExposureCapGate(
            PerStrategyExposureCapGateConfig(max_exposure_usdc=Decimal("200"))
        )
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("100"), limit_price=Decimal("0.500")),
            fake_ctx,
        )
        assert not decision.passed


class TestPerUserAggregateExposureCapGate:
    async def test_under_cap(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.aggregate_exposure = Decimal("500")
        gate = PerUserAggregateExposureCapGate(
            PerUserAggregateExposureCapGateConfig(max_exposure_usdc=Decimal("1000"))
        )
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("100"), limit_price=Decimal("0.500")),
            fake_ctx,
        )
        assert decision.passed

    async def test_over_cap(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.aggregate_exposure = Decimal("980")
        gate = PerUserAggregateExposureCapGate(
            PerUserAggregateExposureCapGateConfig(max_exposure_usdc=Decimal("1000"))
        )
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("100"), limit_price=Decimal("0.500")),
            fake_ctx,
        )
        assert not decision.passed


class TestUSDCSufficientGate:
    async def test_buy_with_enough_balance(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.usdc_balance = Decimal("100")
        gate = USDCSufficientGate(USDCSufficientGateConfig(fee_pct=Decimal("0.02")))
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("50"), limit_price=Decimal("0.500")),
            fake_ctx,
        )
        # cost = 50 * 0.5 * 1.02 = 25.5
        assert decision.passed

    async def test_buy_insufficient_balance(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.usdc_balance = Decimal("10")
        gate = USDCSufficientGate(USDCSufficientGateConfig(fee_pct=Decimal("0.02")))
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("50"), limit_price=Decimal("0.500")),
            fake_ctx,
        )
        assert not decision.passed

    async def test_sell_always_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.usdc_balance = Decimal("0")
        gate = USDCSufficientGate()
        decision = await gate.evaluate(make_signal(side="sell"), fake_ctx)
        assert decision.passed
