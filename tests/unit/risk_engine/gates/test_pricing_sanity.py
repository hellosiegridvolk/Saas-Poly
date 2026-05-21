"""Gates 9-12 (pricing sanity)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.risk_engine.gates import (
    PriceBoundsGate,
    PriceVsMidGate,
    PriceVsMidGateConfig,
    SlippageCapGate,
    SlippageCapGateConfig,
    SpreadCapGate,
    SpreadCapGateConfig,
)
from tests.unit.risk_engine.conftest import FakeRiskContext, make_market, make_signal


class TestPriceBoundsGate:
    @pytest.mark.parametrize(
        "price", [Decimal("0.01"), Decimal("0.50"), Decimal("0.99")]
    )
    async def test_in_range_passes(
        self, fake_ctx: FakeRiskContext, price: Decimal
    ) -> None:
        decision = await PriceBoundsGate().evaluate(
            make_signal(limit_price=price), fake_ctx
        )
        assert decision.passed

    @pytest.mark.parametrize("price", [Decimal("0.005"), Decimal("0.995")])
    async def test_out_of_range_fails(
        self, fake_ctx: FakeRiskContext, price: Decimal
    ) -> None:
        decision = await PriceBoundsGate().evaluate(
            make_signal(limit_price=price), fake_ctx
        )
        assert not decision.passed


class TestPriceVsMidGate:
    async def test_within_tick_window_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt,
            best_bid=Decimal("0.498"),
            best_ask=Decimal("0.502"),
        )
        gate = PriceVsMidGate(PriceVsMidGateConfig(max_ticks_from_mid=5))
        decision = await gate.evaluate(
            make_signal(limit_price=Decimal("0.502")), fake_ctx
        )
        assert decision.passed

    async def test_outside_tick_window_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt,
            best_bid=Decimal("0.498"),
            best_ask=Decimal("0.502"),
        )
        gate = PriceVsMidGate(PriceVsMidGateConfig(max_ticks_from_mid=5))
        decision = await gate.evaluate(
            make_signal(limit_price=Decimal("0.600")), fake_ctx
        )
        assert not decision.passed


class TestSpreadCapGate:
    async def test_tight_spread_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt,
            best_bid=Decimal("0.499"),
            best_ask=Decimal("0.501"),
        )
        gate = SpreadCapGate(SpreadCapGateConfig(max_spread=Decimal("0.01")))
        decision = await gate.evaluate(make_signal(), fake_ctx)
        assert decision.passed

    async def test_wide_spread_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt,
            best_bid=Decimal("0.450"),
            best_ask=Decimal("0.550"),
        )
        gate = SpreadCapGate(SpreadCapGateConfig(max_spread=Decimal("0.01")))
        decision = await gate.evaluate(make_signal(), fake_ctx)
        assert not decision.passed


class TestSlippageCapGate:
    async def test_within_cap_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt,
            best_bid=Decimal("0.498"),
            best_ask=Decimal("0.502"),
            ask_depth=Decimal("100"),
        )
        gate = SlippageCapGate(SlippageCapGateConfig(max_slippage=Decimal("0.01")))
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("10"), limit_price=Decimal("0.510")),
            fake_ctx,
        )
        assert decision.passed

    async def test_over_cap_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt,
            best_bid=Decimal("0.498"),
            best_ask=Decimal("0.502"),
            ask_depth=Decimal("100"),
        )
        gate = SlippageCapGate(SlippageCapGateConfig(max_slippage=Decimal("0.005")))
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("10"), limit_price=Decimal("0.600")),
            fake_ctx,
        )
        assert not decision.passed

    async def test_size_exceeds_best_depth_fails(
        self, fake_ctx: FakeRiskContext
    ) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt, ask_depth=Decimal("5")
        )
        gate = SlippageCapGate(SlippageCapGateConfig(max_slippage=Decimal("0.05")))
        decision = await gate.evaluate(
            make_signal(side="buy", size=Decimal("10")), fake_ctx
        )
        assert not decision.passed
