"""Gates 5-8 (market freshness)."""

from __future__ import annotations

from datetime import timedelta

from services.risk_engine.gates import (
    MarketResolutionGate,
    OrderbookFreshnessGate,
    OrderbookFreshnessGateConfig,
    TickFreshnessGate,
    TickFreshnessGateConfig,
    VolatilityCooldownGate,
)
from tests.unit.risk_engine.conftest import FakeRiskContext, make_market, make_signal


class TestMarketResolutionGate:
    async def test_passes_when_present_and_unresolved(
        self, fake_ctx: FakeRiskContext
    ) -> None:
        fake_ctx.market = make_market(now=fake_ctx.now_dt)
        decision = await MarketResolutionGate().evaluate(make_signal(), fake_ctx)
        assert decision.passed

    async def test_fails_when_missing(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = None
        decision = await MarketResolutionGate().evaluate(make_signal(), fake_ctx)
        assert not decision.passed

    async def test_fails_when_resolved(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(now=fake_ctx.now_dt, resolved=True)
        decision = await MarketResolutionGate().evaluate(make_signal(), fake_ctx)
        assert not decision.passed


class TestOrderbookFreshnessGate:
    async def test_fresh_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(now=fake_ctx.now_dt, book_age_s=1.0)
        gate = OrderbookFreshnessGate(OrderbookFreshnessGateConfig(max_age_seconds=5.0))
        decision = await gate.evaluate(make_signal(), fake_ctx)
        assert decision.passed

    async def test_stale_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(now=fake_ctx.now_dt, book_age_s=30.0)
        gate = OrderbookFreshnessGate(OrderbookFreshnessGateConfig(max_age_seconds=5.0))
        decision = await gate.evaluate(make_signal(), fake_ctx)
        assert not decision.passed


class TestTickFreshnessGate:
    async def test_fresh_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(now=fake_ctx.now_dt, tick_age_s=1.0)
        gate = TickFreshnessGate(TickFreshnessGateConfig(max_age_seconds=5.0))
        decision = await gate.evaluate(make_signal(), fake_ctx)
        assert decision.passed

    async def test_stale_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(now=fake_ctx.now_dt, tick_age_s=30.0)
        gate = TickFreshnessGate(TickFreshnessGateConfig(max_age_seconds=5.0))
        decision = await gate.evaluate(make_signal(), fake_ctx)
        assert not decision.passed


class TestVolatilityCooldownGate:
    async def test_no_cooldown_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(now=fake_ctx.now_dt, cooldown_until=None)
        decision = await VolatilityCooldownGate().evaluate(make_signal(), fake_ctx)
        assert decision.passed

    async def test_in_cooldown_fails(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt,
            cooldown_until=fake_ctx.now_dt + timedelta(seconds=30),
        )
        decision = await VolatilityCooldownGate().evaluate(make_signal(), fake_ctx)
        assert not decision.passed

    async def test_cooldown_expired_passes(self, fake_ctx: FakeRiskContext) -> None:
        fake_ctx.market = make_market(
            now=fake_ctx.now_dt,
            cooldown_until=fake_ctx.now_dt - timedelta(seconds=30),
        )
        decision = await VolatilityCooldownGate().evaluate(make_signal(), fake_ctx)
        assert decision.passed
