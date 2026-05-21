"""Gate 11: spread under cap (spec §10.1 C)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import (
    GateConfig,
    failed_decision,
    passed_decision,
)
from shared.domain import GateDecision, Signal


@dataclass(frozen=True)
class SpreadCapGateConfig(GateConfig):
    max_spread: Decimal


class SpreadCapGate:
    name = "spread_cap"

    def __init__(self, config: SpreadCapGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        snapshot = await ctx.get_market_snapshot(signal.market_id, signal.token_id)
        if snapshot is None or snapshot.spread is None:
            return failed_decision(self.name, "no spread available")
        if snapshot.spread > self._config.max_spread:
            return failed_decision(
                self.name,
                f"spread {snapshot.spread} > cap {self._config.max_spread}",
            )
        return passed_decision(self.name)
