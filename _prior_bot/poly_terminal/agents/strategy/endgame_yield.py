"""Endgame Yield — buy near-certain outcomes converging toward 1.00.

Phase 32 P3 (2026-05-09) — first brand-new strategy on the rebuild
framework. Pinned to playbook §5–6 (`docs/strategy polymarket.md`).

Trigger expression (playbook §19.1 row 4):
    STRATEGY_ENDGAME_YIELD == true
    AND 0.88 <= entry_price <= 0.97
    AND 600 <= time_to_close_s <= 21600        # 10m–6h
    AND verified_true_p >= break_even_p + 0.03
    AND exit_liquidity_at_target >= 2 * size
    AND spread <= 0.03
    AND NOT contradictory_catalyst_pending

The strategy is event-driven on `EVT_CONTEXT_OK`; for each market the
context agent approves, an injected `evaluate_market` callable returns
either an `EndgameCandidate` (price + spread + depth + ttc + verified
confidence) or `None`. The strategy applies the gate stack, routes
through the optional RiskAllocator, and emits `EVT_BUY_INTENT`.

The injected callable does the real plumbing — orderbook + Gamma +
ConfidenceSource lookups. This keeps the strategy unit-testable in
isolation; the wiring layer (main.py) glues it to live data.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from poly_terminal.agents.risk.intent import BuyIntent
from poly_terminal.agents.strategy.allocator import (
    LedgerSnapshot,
    RiskAllocator,
)
from poly_terminal.agents.strategy.base import BaseStrategy
from poly_terminal.agents.strategy.confidence_source import ConfidenceResult
from poly_terminal.agents.strategy.ev_gate import (
    EVMarginConfig,
    evaluate_ev_gate,
)
from poly_terminal.agents.strategy.exit_config import for_strategy
from poly_terminal.agents.strategy.framework import StrategySignal
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_BUY_INTENT, EVT_CONTEXT_OK
from poly_terminal.shared.enums import BotMode, IntentSide, IntentSource


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EndgameCandidate:
    """Snapshot of a market+token at decision time.

    Built by the wiring layer from orderbook + Gamma + ConfidenceSource.
    Tests construct it directly to drive the gate stack.
    """
    market_id: str
    token_id: str
    side: str  # "YES" or "NO"
    entry_price: float
    target_exit: float
    spread: float
    time_to_close_s: int
    top_3_depth: float
    confidence: ConfidenceResult
    end_date_iso: str | None = None
    contradictory_catalyst_pending: bool = False


@dataclass(frozen=True)
class EndgameYieldConfig:
    """Strategy parameters — defaults pin the playbook §5 numbers."""
    # Position sizing
    position_size_usd: float = 1.00
    # Price band
    entry_price_min: float = 0.88
    entry_price_max: float = 0.97
    # Default target exit when candidate doesn't supply one
    target_exit_default: float = 0.99
    # Time-to-close band (10m–6h)
    time_to_close_min_s: int = 600
    time_to_close_max_s: int = 21600
    # Spread cap
    max_spread: float = 0.03
    # Depth gate: top-3 depth must be >= multiplier × position_size
    depth_multiplier: float = 2.0
    # EV gate margin (above break-even)
    ev_margin: float = 0.03


# Type alias for the wiring layer's lookup callable.
EvaluateMarketFn = Callable[[str], EndgameCandidate | None]


class EndgameYieldStrategy(BaseStrategy):
    """Subscribes to EVT_CONTEXT_OK; emits EVT_BUY_INTENT when the
    candidate clears every gate (confidence, price band, ttc, spread,
    depth, EV, catalyst, allocator)."""

    name = "endgame_yield"

    def __init__(
        self,
        bus: EventBus,
        cfg: EndgameYieldConfig | None = None,
        *,
        evaluate_market: EvaluateMarketFn,
        # 2026-05-09 Phase 32 P3 — RiskAllocator gate (optional).
        # Same shape as copy_trade. None = legacy / paper-only.
        allocator: RiskAllocator | None = None,
        mode_getter: Callable[[], BotMode] | None = None,
        ledger_snapshot_getter: Callable[[], LedgerSnapshot] | None = None,
    ) -> None:
        super().__init__(bus)
        self._cfg = cfg or EndgameYieldConfig()
        self._evaluate_market = evaluate_market
        self._allocator = allocator
        self._mode_getter: Callable[[], BotMode] = (
            mode_getter or (lambda: BotMode.PAPER)
        )
        self._ledger_snapshot_getter: Callable[[], LedgerSnapshot] = (
            ledger_snapshot_getter or (lambda: LedgerSnapshot())
        )
        # Counters — surfaced via the existing strategy-stats aggregator.
        self.intents_rejected_no_confidence: int = 0
        self.intents_rejected_ev_gate: int = 0
        self.intents_rejected_price_band: int = 0
        self.intents_rejected_time_to_close: int = 0
        self.intents_rejected_spread: int = 0
        self.intents_rejected_low_depth: int = 0
        self.intents_rejected_catalyst: int = 0
        self.intents_rejected_allocator: int = 0

    async def _subscribe(self) -> None:
        self._bus.subscribe(EVT_CONTEXT_OK, self._on_context_ok)

    async def _on_context_ok(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        market_id = payload.get("market_id")
        if not market_id:
            return
        try:
            candidate = self._evaluate_market(str(market_id))
        except Exception:
            logger.exception(
                "endgame_yield: evaluate_market raised for %s — skipping",
                market_id,
            )
            return
        if candidate is None:
            # No Endgame candidate for this market — silent skip
            # (most markets are not endgame candidates).
            return

        # Gate stack — short-circuits on first failure.
        if not candidate.confidence.found:
            self.intents_rejected_no_confidence += 1
            logger.debug(
                "endgame_yield: skip %s — no confidence entry",
                candidate.market_id,
            )
            return

        if not (
            self._cfg.entry_price_min <= candidate.entry_price <=
            self._cfg.entry_price_max
        ):
            self.intents_rejected_price_band += 1
            logger.debug(
                "endgame_yield: skip %s — price %.4f outside band "
                "[%.2f, %.2f]",
                candidate.market_id, candidate.entry_price,
                self._cfg.entry_price_min, self._cfg.entry_price_max,
            )
            return

        if not (
            self._cfg.time_to_close_min_s <= candidate.time_to_close_s <=
            self._cfg.time_to_close_max_s
        ):
            self.intents_rejected_time_to_close += 1
            logger.debug(
                "endgame_yield: skip %s — ttc %ds outside [%d, %d]",
                candidate.market_id, candidate.time_to_close_s,
                self._cfg.time_to_close_min_s, self._cfg.time_to_close_max_s,
            )
            return

        if candidate.spread > self._cfg.max_spread:
            self.intents_rejected_spread += 1
            logger.debug(
                "endgame_yield: skip %s — spread %.4f > %.4f",
                candidate.market_id, candidate.spread, self._cfg.max_spread,
            )
            return

        depth_required = (
            self._cfg.position_size_usd * self._cfg.depth_multiplier
        )
        if candidate.top_3_depth < depth_required:
            self.intents_rejected_low_depth += 1
            logger.debug(
                "endgame_yield: skip %s — depth %.2f < required %.2f",
                candidate.market_id, candidate.top_3_depth, depth_required,
            )
            return

        if candidate.contradictory_catalyst_pending:
            self.intents_rejected_catalyst += 1
            logger.info(
                "endgame_yield: skip %s — contradictory catalyst pending",
                candidate.market_id,
            )
            return

        # EV gate (true_p >= break_even + margin).
        ev = evaluate_ev_gate(
            entry=candidate.entry_price,
            target=candidate.target_exit,
            true_p=candidate.confidence.true_p,
            cfg=EVMarginConfig(margin=self._cfg.ev_margin),
        )
        if not ev.passed:
            self.intents_rejected_ev_gate += 1
            logger.info(
                "endgame_yield: skip %s — %s",
                candidate.market_id, ev.reason,
            )
            return

        # Build BuyIntent.
        intent = BuyIntent(
            intent_id=str(uuid.uuid4()),
            strategy=self.name,
            market_id=candidate.market_id,
            token_id=candidate.token_id,
            side=IntentSide.BUY,
            size_usd=Decimal(str(self._cfg.position_size_usd)),
            limit_price=Decimal(str(candidate.entry_price)),
            source=IntentSource.ENDGAME_YIELD,
            created_at=0.0,
            end_date_iso=candidate.end_date_iso,
            exit_config=for_strategy(self.name),
        )

        # 2026-05-09 Phase 32 P3 — RiskAllocator gate.
        if self._allocator is not None:
            try:
                signal = StrategySignal(
                    strategy_name=self.name,
                    market_id=candidate.market_id,
                    token_id=candidate.token_id,
                    side=candidate.side,
                    confidence=float(candidate.confidence.true_p),
                    edge_bps=int(round((ev.true_p - ev.threshold) * 10000)),
                    max_loss_usd=float(self._cfg.position_size_usd),
                    target_exit=float(candidate.target_exit),
                    stop_exit=max(
                        0.01,
                        float(candidate.entry_price)
                        - 3 * float(candidate.spread),
                    ),
                    max_hold_s=int(candidate.time_to_close_s),
                    extra={
                        "true_p": float(candidate.confidence.true_p),
                        "sources": int(candidate.confidence.sources_count),
                        "break_even": float(ev.break_even),
                    },
                )
                decision = self._allocator.approve(
                    signal,
                    mode=self._mode_getter(),
                    ledger=self._ledger_snapshot_getter(),
                )
            except Exception:
                logger.exception(
                    "endgame_yield: allocator raised for %s — rejecting "
                    "for safety", candidate.market_id,
                )
                self.intents_rejected_allocator += 1
                return
            if not decision.approved:
                logger.info(
                    "endgame_yield: allocator REJECTED %s — "
                    "reason=%s detail=%s",
                    candidate.market_id,
                    decision.reason.value if decision.reason else "unknown",
                    decision.detail,
                )
                self.intents_rejected_allocator += 1
                return

        await self._bus.publish(EVT_BUY_INTENT, intent)
        self.intents_emitted += 1
        logger.info(
            "endgame_yield: emitted intent for %s tok=%s @ %.4f → %.4f "
            "(true_p=%.3f, break_even=%.3f, edge=%dbps)",
            candidate.market_id, candidate.token_id,
            candidate.entry_price, candidate.target_exit,
            candidate.confidence.true_p, ev.break_even,
            int(round((ev.true_p - ev.threshold) * 10000)),
        )


__all__ = [
    "EndgameCandidate",
    "EndgameYieldConfig",
    "EndgameYieldStrategy",
    "EvaluateMarketFn",
]
