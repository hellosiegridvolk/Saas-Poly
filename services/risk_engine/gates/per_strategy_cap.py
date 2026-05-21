"""Gate 16: per-strategy exposure cap, evaluated post-fill (spec §10.1 D).

Exposure is approximated as ``current_exposure + size * limit_price`` —
the USDC notional the order would commit if it fully fills."""

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
class PerStrategyExposureCapGateConfig(GateConfig):
    max_exposure_usdc: Decimal


class PerStrategyExposureCapGate:
    name = "per_strategy_exposure_cap"

    def __init__(self, config: PerStrategyExposureCapGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        current = await ctx.get_per_strategy_exposure(signal.strategy_instance_id)
        delta = signal.size * signal.limit_price if signal.side == "buy" else Decimal(0)
        projected = current + delta
        if projected > self._config.max_exposure_usdc:
            return failed_decision(
                self.name,
                f"projected exposure {projected} > cap "
                f"{self._config.max_exposure_usdc}",
            )
        return passed_decision(self.name)
