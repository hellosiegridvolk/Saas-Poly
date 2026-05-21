"""Gate 1: user is active and not suspended (spec §10.1 A)."""

from __future__ import annotations

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal


class UserActiveGate:
    name = "user_active"

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        state = await ctx.get_user_state()
        if not state.active:
            return failed_decision(self.name, "user inactive")
        if state.suspended:
            return failed_decision(self.name, "user suspended")
        return passed_decision(self.name)
