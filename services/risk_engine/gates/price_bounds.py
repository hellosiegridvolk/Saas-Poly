"""Gate 9: limit price within [0.01, 0.99] (spec §10.1 C)."""

from __future__ import annotations

from decimal import Decimal

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal

PRICE_MIN: Decimal = Decimal("0.01")
PRICE_MAX: Decimal = Decimal("0.99")


class PriceBoundsGate:
    name = "price_bounds"

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        if signal.limit_price < PRICE_MIN:
            return failed_decision(
                self.name, f"limit_price {signal.limit_price} below {PRICE_MIN}"
            )
        if signal.limit_price > PRICE_MAX:
            return failed_decision(
                self.name, f"limit_price {signal.limit_price} above {PRICE_MAX}"
            )
        return passed_decision(self.name)
