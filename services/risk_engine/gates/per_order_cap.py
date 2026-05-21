"""Gate 14: per-order size cap (spec §10.1 D)."""

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
class PerOrderSizeCapGateConfig(GateConfig):
    max_size: Decimal


class PerOrderSizeCapGate:
    name = "per_order_size_cap"

    def __init__(self, config: PerOrderSizeCapGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        if signal.size > self._config.max_size:
            return failed_decision(
                self.name,
                f"size {signal.size} > per-order cap {self._config.max_size}",
            )
        return passed_decision(self.name)
