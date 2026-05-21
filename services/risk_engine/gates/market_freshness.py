"""Gates 6 & 7: orderbook and last-tick freshness (spec §10.1 B)."""

from __future__ import annotations

from dataclasses import dataclass

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import (
    GateConfig,
    failed_decision,
    passed_decision,
)
from shared.domain import GateDecision, Signal


@dataclass(frozen=True)
class OrderbookFreshnessGateConfig(GateConfig):
    max_age_seconds: float


class OrderbookFreshnessGate:
    name = "orderbook_freshness"

    def __init__(self, config: OrderbookFreshnessGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        snapshot = await ctx.get_market_snapshot(signal.market_id, signal.token_id)
        if snapshot is None:
            return failed_decision(self.name, "no market snapshot")
        now = await ctx.now()
        age = (now - snapshot.last_book_update_at).total_seconds()
        if age > self._config.max_age_seconds:
            return failed_decision(
                self.name, f"orderbook stale: {age:.1f}s > {self._config.max_age_seconds}s"
            )
        return passed_decision(self.name)


@dataclass(frozen=True)
class TickFreshnessGateConfig(GateConfig):
    max_age_seconds: float


class TickFreshnessGate:
    name = "tick_freshness"

    def __init__(self, config: TickFreshnessGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        snapshot = await ctx.get_market_snapshot(signal.market_id, signal.token_id)
        if snapshot is None:
            return failed_decision(self.name, "no market snapshot")
        now = await ctx.now()
        age = (now - snapshot.last_tick_at).total_seconds()
        if age > self._config.max_age_seconds:
            return failed_decision(
                self.name, f"last tick stale: {age:.1f}s > {self._config.max_age_seconds}s"
            )
        return passed_decision(self.name)
