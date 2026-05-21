"""Gate 5: market exists and is not resolved (spec §10.1 B)."""

from __future__ import annotations

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal


class MarketResolutionGate:
    name = "market_resolution"

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        snapshot = await ctx.get_market_snapshot(signal.market_id, signal.token_id)
        if snapshot is None:
            return failed_decision(self.name, "market not found")
        if snapshot.resolved:
            return failed_decision(self.name, "market already resolved")
        return passed_decision(self.name)
