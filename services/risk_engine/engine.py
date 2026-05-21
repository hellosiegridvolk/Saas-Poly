"""Risk engine orchestrator (spec §10).

Runs an ordered list of gates against a Signal, fails fast on the first
rejection, and emits an Intent (``approved`` or ``rejected``) carrying
the full list of GateDecisions plus their measured durations. Telemetry
is fired per gate via :mod:`shared.telemetry`.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import Gate
from shared.domain import GateDecision, Intent, Signal
from shared.telemetry import Metrics, NoOpMetrics, record_gate_decision

ContextFactory = Callable[[UUID], Awaitable[RiskContext]]


@dataclass(frozen=True)
class RiskEngineResult:
    intent: Intent
    decisions: list[GateDecision]

    @property
    def approved(self) -> bool:
        return bool(self.decisions) and all(d.passed for d in self.decisions)


class RiskEngine:
    def __init__(
        self,
        gates: Sequence[Gate],
        context_factory: ContextFactory,
        *,
        metrics: Metrics | None = None,
    ) -> None:
        self._gates = list(gates)
        self._context_factory = context_factory
        self._metrics = metrics or NoOpMetrics()

    async def evaluate(self, signal: Signal) -> RiskEngineResult:
        ctx = await self._context_factory(signal.user_id)
        decisions: list[GateDecision] = []
        for gate in self._gates:
            start = time.perf_counter()
            decision = await gate.evaluate(signal, ctx)
            duration_ms = int((time.perf_counter() - start) * 1000)
            decision = decision.model_copy(update={"duration_ms": duration_ms})
            decisions.append(decision)
            record_gate_decision(
                self._metrics,
                strategy_id=signal.strategy_id,
                user_id=str(signal.user_id),
                decision=decision,
            )
            if not decision.passed:
                break

        intent = Intent(
            intent_id=uuid4(),
            signal_id=signal.signal_id,
            user_id=signal.user_id,
            strategy_id=signal.strategy_id,
            strategy_instance_id=signal.strategy_instance_id,
            market_id=signal.market_id,
            token_id=signal.token_id,
            side=signal.side,
            size=signal.size,
            limit_price=signal.limit_price,
            time_in_force=signal.time_in_force,
            risk_decisions=decisions,
            approved_at=datetime.now(tz=UTC),
        )
        # ``approved`` is also derivable from decisions; we expose it on the result.
        return RiskEngineResult(intent=intent, decisions=decisions)
