"""Gate 10: limit price within N ticks of mid (spec §10.1 C, §11.2 tick)."""

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
from shared.polymarket import PRICE_TICK


@dataclass(frozen=True)
class PriceVsMidGateConfig(GateConfig):
    max_ticks_from_mid: int


class PriceVsMidGate:
    name = "price_vs_mid"

    def __init__(self, config: PriceVsMidGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        snapshot = await ctx.get_market_snapshot(signal.market_id, signal.token_id)
        if snapshot is None or snapshot.mid is None:
            return failed_decision(self.name, "no mid available")
        delta = abs(signal.limit_price - snapshot.mid)
        max_delta = PRICE_TICK * Decimal(self._config.max_ticks_from_mid)
        if delta > max_delta:
            return failed_decision(
                self.name,
                f"limit_price {signal.limit_price} is {delta} from mid "
                f"{snapshot.mid} (max {max_delta})",
            )
        return passed_decision(self.name)
