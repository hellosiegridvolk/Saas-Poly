"""Gate 17: per-user aggregate exposure cap, evaluated post-fill (spec §10.1 D)."""

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
class PerUserAggregateExposureCapGateConfig(GateConfig):
    max_exposure_usdc: Decimal


class PerUserAggregateExposureCapGate:
    name = "per_user_aggregate_exposure_cap"

    def __init__(self, config: PerUserAggregateExposureCapGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        current = await ctx.get_user_aggregate_exposure()
        delta = signal.size * signal.limit_price if signal.side == "buy" else Decimal(0)
        projected = current + delta
        if projected > self._config.max_exposure_usdc:
            return failed_decision(
                self.name,
                f"projected aggregate exposure {projected} > cap "
                f"{self._config.max_exposure_usdc}",
            )
        return passed_decision(self.name)
