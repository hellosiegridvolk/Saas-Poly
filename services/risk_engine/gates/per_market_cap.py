"""Gate 15: per-market position cap, evaluated post-fill (spec §10.1 D)."""

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
class PerMarketPositionCapGateConfig(GateConfig):
    max_abs_position: Decimal


class PerMarketPositionCapGate:
    name = "per_market_position_cap"

    def __init__(self, config: PerMarketPositionCapGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        current = await ctx.get_position_size(signal.market_id, signal.token_id)
        delta = signal.size if signal.side == "buy" else -signal.size
        projected = abs(current + delta)
        if projected > self._config.max_abs_position:
            return failed_decision(
                self.name,
                f"projected position {projected} > cap {self._config.max_abs_position}",
            )
        return passed_decision(self.name)
