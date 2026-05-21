"""Gate 3: kill switch off, both global and per-user (spec §10.1 A, §11.1)."""

from __future__ import annotations

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal


class KillSwitchGate:
    name = "kill_switch_off"

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        state = await ctx.get_risk_state()
        if state.global_kill_switch_on:
            return failed_decision(self.name, "global kill switch tripped")
        if state.kill_switch_on:
            return failed_decision(self.name, "user kill switch tripped")
        return passed_decision(self.name)
