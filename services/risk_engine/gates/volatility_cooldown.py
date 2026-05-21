"""Gate 8: market not in cooldown after a volatility spike (spec §10.1 B)."""

from __future__ import annotations

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal


class VolatilityCooldownGate:
    name = "volatility_cooldown"

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        snapshot = await ctx.get_market_snapshot(signal.market_id, signal.token_id)
        if snapshot is None:
            return failed_decision(self.name, "no market snapshot")
        cooldown_until = snapshot.volatility_cooldown_until
        if cooldown_until is None:
            return passed_decision(self.name)
        now = await ctx.now()
        if now < cooldown_until:
            return failed_decision(
                self.name,
                f"market in volatility cooldown until {cooldown_until.isoformat()}",
            )
        return passed_decision(self.name)
