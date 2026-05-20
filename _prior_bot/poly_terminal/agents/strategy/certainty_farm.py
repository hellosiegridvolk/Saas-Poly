"""Certainty Farm — "99-cent farming" / high-certainty premium capture.

Phase 39 (2026-05-12). Operationalizes the deep-research-report
(33/34) recommendation set for endgame-style premium capture, as a
SEPARATE strategy from endgame_yield with stricter defaults baked in.

**Why a new strategy, not an endgame_yield config tweak?**

The reports' hard gates differ from endgame_yield's wider thresholds
in five material ways:

  1. Entry band capped at **0.97** (not 0.99) — 0.95+ economics
     collapse with even a 3¢ exit slippage per the shock simulation.
  2. TTC band extended to **48 hours** so the operator can curate
     overrides well before the strict 6h endgame regime starts.
  3. **Two independent confidence sources required** (`source_count
     >= 2`), not a soft recommendation.
  4. **Tight spread cap** (≤ 0.02), not 0.03.
  5. **Smaller default size** ($1, not the bot-wide MAX_POSITION_USD)
     because the loss asymmetry at near-1.00 entries means a single
     bad call wipes out many small wins.

The same `ManualConfidenceSource` pattern endgame_yield uses applies
here. With zero overrides configured, the strategy emits zero
intents — same fail-closed contract.

**Operator workflow:**

1. Identify a high-certainty market (e.g. a mathematically-locked
   sports finish, a near-resolution political event with 2+ sources
   confirming the outcome).
2. Add to `CERTAINTY_FARM_OVERRIDES` env: `condition_id:token_id:
   true_p:source_count` (comma-separated rows). source_count must
   be ≥ 2.
3. Enable `STRATEGY_CERTAINTY_FARM=true` in .env.
4. Restart bot.

When the operator's curated market enters the configured price and
TTC bands, the strategy fires a $1 BUY intent. The position holds
until TP (0.98 default), SL (0.85 default), or bar resolution.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from poly_terminal.agents.strategy.base import BaseStrategy
from poly_terminal.bus.event_bus import EventBus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CertaintyFarmConfig:
    """Tunable gates. Defaults follow the deep-research-report 33/34
    explicit recommendation set verbatim."""
    # Entry band. 0.97 cap is the hard line from the shock simulation
    # — 0.98+ entries are explicitly rejected because a 3¢ exit
    # deterioration wipes out edge.
    price_lo: float = 0.90
    price_hi: float = 0.97
    # TTC band (15min to 48h). Wider than endgame_yield's [10min, 6h]
    # so operators can pre-position curated overrides early.
    ttc_min_s: int = 15 * 60
    ttc_max_s: int = 48 * 3600
    # Spread cap. Tighter than endgame_yield's 0.03 because at 0.95+
    # the premium is so thin that even 2-3¢ extra round-trip cost
    # erases it.
    max_spread: float = 0.02
    # Confidence requirement. Reports explicitly require two
    # independent sources — single-source is rejected by default.
    min_source_count: int = 2
    # EV-gate margin (added to break-even). Same formula as
    # endgame_yield: `true_p >= entry/0.99 + ev_margin`.
    ev_margin: float = 0.03
    # Sizing — small by default because loss asymmetry is brutal.
    size_usd: float = 1.0
    # Exit targets passed through to the position via exit_config.
    tp_price: float = 0.98
    sl_price: float = 0.85


@dataclass(frozen=True)
class FarmCandidate:
    """Snapshot of a market+token at decision time, built by the
    wiring layer from orderbook + Gamma + ManualConfidenceSource."""
    market_id: str
    token_id: str
    side: str               # "YES" or "NO" (which token we're buying)
    entry_price: float      # marketable price right now
    spread: float
    time_to_close_s: int
    true_p: float           # operator-supplied confidence
    source_count: int       # number of independent confirmations


@dataclass(frozen=True)
class FarmDecision:
    """Result of evaluating a FarmCandidate against the gate stack."""
    approved: bool
    reason: str
    required_true_p: float | None = None


def evaluate_farm_candidate(
    candidate: FarmCandidate, *, cfg: CertaintyFarmConfig,
) -> FarmDecision:
    """Pure gate-stack evaluator. Tests pin every gate; wiring layer
    just calls this and acts on `.approved`."""
    # 1. Price band — explicit upper bound at 0.97 by default.
    if not (cfg.price_lo <= candidate.entry_price <= cfg.price_hi):
        return FarmDecision(
            approved=False,
            reason=(
                f"price {candidate.entry_price:.4f} outside band "
                f"[{cfg.price_lo}, {cfg.price_hi}]"
            ),
        )
    # 2. Spread gate.
    if candidate.spread > cfg.max_spread + 1e-9:
        return FarmDecision(
            approved=False,
            reason=(
                f"spread {candidate.spread:.4f} > "
                f"max {cfg.max_spread:.4f}"
            ),
        )
    # 3. TTC band.
    if not (cfg.ttc_min_s <= candidate.time_to_close_s <= cfg.ttc_max_s):
        return FarmDecision(
            approved=False,
            reason=(
                f"ttc {candidate.time_to_close_s}s outside band "
                f"[{cfg.ttc_min_s}, {cfg.ttc_max_s}]"
            ),
        )
    # 4. Source-count gate. Hard line — reports require ≥ 2.
    if candidate.source_count < cfg.min_source_count:
        return FarmDecision(
            approved=False,
            reason=(
                f"source_count {candidate.source_count} < min "
                f"{cfg.min_source_count} (reports require ≥2 "
                f"independent confirmations)"
            ),
        )
    # 5. EV gate — same break-even formula as endgame_yield.
    break_even = candidate.entry_price / 0.99
    required = break_even + cfg.ev_margin
    if candidate.true_p < required:
        return FarmDecision(
            approved=False,
            reason=(
                f"EV gate FAIL: true_p {candidate.true_p:.4f} < "
                f"required {required:.4f} "
                f"(break-even {break_even:.4f} + margin {cfg.ev_margin})"
            ),
            required_true_p=round(required, 4),
        )
    return FarmDecision(
        approved=True,
        reason=(
            f"OK: price={candidate.entry_price:.4f}, "
            f"true_p={candidate.true_p:.4f} (req≥{required:.4f}), "
            f"sources={candidate.source_count}, "
            f"ttc={candidate.time_to_close_s}s"
        ),
        required_true_p=round(required, 4),
    )


class CertaintyFarmStrategy(BaseStrategy):
    """Strategy agent — subscribes to EVT_CONTEXT_OK and emits
    EVT_BUY_INTENT for approved candidates.

    Wiring layer injects `evaluate_candidate_at(market_id, token_id)`
    which returns a `FarmCandidate` or `None`. The factory typically
    composes:
      * orderbook snapshot (entry_price + spread)
      * Gamma metadata (time_to_close_s)
      * ManualConfidenceSource lookup (true_p + source_count)
    """
    name = "certainty_farm"

    def __init__(
        self,
        bus: EventBus,
        cfg: CertaintyFarmConfig,
        evaluate_candidate_at: Callable[[str, str], FarmCandidate | None],
        *,
        find_market_for_token: Callable[[str], str | None] | None = None,
        allocator: Any | None = None,
        mode_getter: Callable[[], Any] | None = None,
        ledger_snapshot_getter: Callable[[], Any] | None = None,
    ) -> None:
        super().__init__(
            bus=bus,
            allocator=allocator,
            mode_getter=mode_getter,
            ledger_snapshot_getter=ledger_snapshot_getter,
        )
        self._cfg = cfg
        self._evaluate_at = evaluate_candidate_at
        # 2026-05-12 — tick-driven flow. The strategy subscribes to
        # EVT_MARKET_TICK and uses this callback to reverse-lookup the
        # market_id from the tick's token_id. The wiring layer binds
        # this to `ManualConfidenceSource.find_market_id_for_token` so
        # the strategy only evaluates ticks for tokens it has overrides
        # for — drastically cheaper than evaluating every market tick.
        self._find_market_for_token = (
            find_market_for_token or (lambda _t: None)
        )
        # Telemetry — useful for the dashboard's per-strategy view.
        self.candidates_evaluated: int = 0
        self.candidates_rejected_gates: int = 0

    async def _subscribe(self) -> None:
        # 2026-05-12 — was EVT_CONTEXT_OK. Switched to EVT_MARKET_TICK
        # because EVT_CONTEXT_OK isn't firing under current bot wiring
        # (verified 6+ hours of soak: 0 EVT_CONTEXT_OK publications).
        # The tick stream is the bot's primary event signal and is
        # already producing 1000s of events per hour.
        from poly_terminal.bus.events import EVT_MARKET_TICK
        self._bus.subscribe(EVT_MARKET_TICK, self._on_tick)

    async def _on_tick(self, _event: str, payload: dict) -> None:
        if not isinstance(payload, dict):
            return
        token_id = str(payload.get("token_id") or "")
        if not token_id:
            return
        try:
            price = float(payload["price"])
        except (KeyError, TypeError, ValueError):
            return
        # Cheap pre-filter: skip ticks outside the configured price
        # band without doing any further work. ~90%+ of ticks short-
        # circuit here, keeping the strategy cost negligible even at
        # high tick rates.
        if not (self._cfg.price_lo <= price <= self._cfg.price_hi):
            return
        # Reverse-lookup: is this a token we have an override for?
        # If no override → silent (strategy doesn't evaluate random
        # ticks; only operator-curated markets).
        market_id = self._find_market_for_token(token_id)
        if market_id is None:
            return
        candidate = self._evaluate_at(market_id, token_id)
        if candidate is None:
            return
        self.candidates_evaluated += 1
        decision = evaluate_farm_candidate(candidate, cfg=self._cfg)
        if not decision.approved:
            self.candidates_rejected_gates += 1
            logger.info(
                "certainty_farm: gate reject — %s (market=%s token=%s)",
                decision.reason, market_id, candidate.token_id,
            )
            return
        # Allocator gate (RiskAllocator)
        if not self._allocator_approves_intent(
            market_id=candidate.market_id,
            token_id=candidate.token_id,
            size_usd=self._cfg.size_usd,
            marketable_price=candidate.entry_price,
        ):
            return
        # Emit BuyIntent
        from poly_terminal.agents.risk.intent import BuyIntent
        from poly_terminal.agents.strategy.exit_config import for_strategy
        from poly_terminal.bus.events import EVT_BUY_INTENT
        from poly_terminal.shared.enums import IntentSide, IntentSource

        intent = BuyIntent(
            intent_id=str(uuid.uuid4()),
            strategy=self.name,
            market_id=candidate.market_id,
            token_id=candidate.token_id,
            side=IntentSide.BUY,
            size_usd=Decimal(str(self._cfg.size_usd)),
            limit_price=Decimal(str(min(0.99, candidate.entry_price + 0.01))),
            source=IntentSource.MANUAL,  # No dedicated enum value yet
            created_at=0.0,
            end_date_iso=None,
            exit_config=for_strategy(self.name),
        )
        await self._bus.publish(EVT_BUY_INTENT, intent)
        self.intents_emitted += 1
        logger.info(
            "certainty_farm: emitted intent — market=%s token=%s "
            "price=%.4f true_p=%.4f (req≥%.4f) sources=%d",
            candidate.market_id, candidate.token_id,
            candidate.entry_price, candidate.true_p,
            decision.required_true_p or 0.0, candidate.source_count,
        )
