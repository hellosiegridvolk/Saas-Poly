"""Gate 13: size >= minimum CLOB unit (spec §10.1 D)."""

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
class MinSizeGateConfig(GateConfig):
    min_size: Decimal


class MinSizeGate:
    name = "min_size"

    def __init__(self, config: MinSizeGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        if signal.size < self._config.min_size:
            return failed_decision(
                self.name,
                f"size {signal.size} < minimum {self._config.min_size}",
            )
        return passed_decision(self.name)
