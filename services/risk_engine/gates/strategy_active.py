"""Gate 2: strategy instance is active and the signal's mode matches the
instance's mode (spec §10.1 A, §21)."""

from __future__ import annotations

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal


class StrategyActiveGate:
    name = "strategy_active"

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        instance = await ctx.get_strategy_instance(signal.strategy_instance_id)
        if instance is None:
            return failed_decision(self.name, "strategy instance not found")
        if instance.status != "active":
            return failed_decision(
                self.name, f"strategy instance status={instance.status}"
            )
        return passed_decision(self.name)
