"""Gate 4: signal_id not already processed (spec §3.4, §10.1 A).

The idempotency key is the signal's id (which the strategy must derive
deterministically from ``user_id + strategy_id + tick``)."""

from __future__ import annotations

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import failed_decision, passed_decision
from shared.domain import GateDecision, Signal


class IdempotencyGate:
    name = "idempotency"

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        if await ctx.is_signal_processed(signal.signal_id):
            return failed_decision(self.name, "duplicate signal_id")
        return passed_decision(self.name)
