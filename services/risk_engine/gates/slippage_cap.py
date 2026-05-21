"""Gate 12: effective slippage under cap given size & book depth (spec §10.1 C).

PR B implements a conservative single-level slippage estimate: if the
signal's size exceeds the depth at the best price on the relevant side,
slippage is treated as "unknown / over cap." A walk-the-book estimator
lands once the book snapshot carries multiple levels (post-Phase 1).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from services.risk_engine.context import RiskContext
from services.risk_engine.gates.base import (
    GateConfig,
    failed_decision,
    passed_decision,
)
from shared.domain import GateDecision, Signal


@dataclass(frozen=True)
class SlippageCapGateConfig(GateConfig):
    max_slippage: Decimal


class SlippageCapGate:
    name = "slippage_cap"

    def __init__(self, config: SlippageCapGateConfig) -> None:
        self._config = config

    async def evaluate(self, signal: Signal, ctx: RiskContext) -> GateDecision:
        snapshot = await ctx.get_market_snapshot(signal.market_id, signal.token_id)
        if snapshot is None or snapshot.mid is None:
            return failed_decision(self.name, "no book snapshot")

        depth = (
            snapshot.ask_depth_at_best if signal.side == "buy" else snapshot.bid_depth_at_best
        )
        if signal.size > depth:
            return failed_decision(
                self.name,
                f"size {signal.size} > best-level depth {depth}; "
                "deep-book slippage estimator not yet implemented",
            )

        best = snapshot.best_ask if signal.side == "buy" else snapshot.best_bid
        if best is None:
            return failed_decision(self.name, "no best price on signal side")
        slippage = abs(signal.limit_price - best)
        if slippage > self._config.max_slippage:
            return failed_decision(
                self.name,
                f"slippage {slippage} > cap {self._config.max_slippage}",
            )
        return passed_decision(self.name)
