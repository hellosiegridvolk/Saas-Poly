"""Gate 15: latency-sensitive strategies refuse new entries when an upstream
budget is breached.
"""

from __future__ import annotations

from poly_terminal.data.latency_budget import LatencyBudget
from poly_terminal.shared.typed_reject import Reject

# Strategies whose edge requires the upstream data path to be healthy.
LATENCY_SENSITIVE_STRATEGIES: frozenset[str] = frozenset(
    {"flash_crash", "scalp_15m", "scalp_1h", "dump_hedge"}
)


class LatencyBudgetGate:
    def __init__(self, budget: LatencyBudget) -> None:
        self._budget = budget

    async def __call__(self, intent: object) -> Reject | None:
        strategy = str(getattr(intent, "strategy", ""))
        if strategy not in LATENCY_SENSITIVE_STRATEGIES:
            return None
        if self._budget.is_open():
            return Reject(
                code="upstream_latency_budget_open",
                detail=f"{self._budget.name} p95={self._budget.p95_ms()}ms",
            )
        return None
