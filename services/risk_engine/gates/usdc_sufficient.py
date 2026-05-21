"""Gate 18: USDC balance sufficient including fees (spec §10.1 D).

Conservative cost model: ``size * limit_price * (1 + fee_pct)``. The fee
percentage is a constructor argument so it can track the live CLOB
schedule without a code change.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal


@dataclass(frozen=True)
class USDCSufficientGateConfig:
    fee_pct: Decimal = Decimal("0.02")


class USDCSufficientGate:
    name = "usdc_sufficient"

    def __init__(self, config: USDCSufficientGateConfig | None = None) -> None:
        self._config = config or USDCSufficientGateConfig()

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        if signal.side != "buy":
            return passed_decision(self.name)
        cost = signal.size * signal.limit_price * (Decimal(1) + self._config.fee_pct)
        balance = await ctx.get_usdc_balance()
        if cost > balance:
            return failed_decision(
                self.name, f"projected cost {cost} > balance {balance}"
            )
        return passed_decision(self.name)
