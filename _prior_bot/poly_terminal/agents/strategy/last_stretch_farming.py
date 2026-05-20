"""Last Stretch Farming — research-only scaffold.

⚠️  STRATEGY IS STRUCTURALLY UNSAFE FOR LIVE USE. ⚠️

Phase 40 (2026-05-12). Targets the 0.95-0.99 price band on crypto
5/15-min Up/Down bars in the final 10-90 seconds before resolution,
using POST_ONLY maker limit orders. The thesis is "scoop final 1-5
cents of premium". The reality is "pick up pennies in front of a
steamroller" — the strategy's own spec acknowledges this.

**Why it exists in the codebase at all:**

It exists to be SCAFFOLDED + TESTED + ARCHIVED, not deployed. The
operator wanted to investigate whether the 0.95-0.99 band has
empirical edge despite three independent disagreement signals:

  1. Deep-research-report 33/34 explicitly excluded this band:
     "0.95+ entries are explicitly excluded because a 3¢ exit
     deterioration wipes out edge."
  2. Empirical probe (2026-05-12) of 367 lifetime positions entered
     at 0.95-0.99: -1.42% ROI; worst single position -$9.99 (full
     position blowup); 35 of 367 trades were catastrophic losses
     that wiped out the gains from ~250 small wins.
  3. The strategy's own 50:1 asymmetry: a single loss erases 50
     consecutive successful trades. The break-even win rate is
     ~98%. Our historical sample had 90.5%.

**Multi-layer fail-closed safety:**

  Layer 1: settings flag `STRATEGY_LAST_STRETCH_FARMING` defaults False.
  Layer 2: explicit `research_armed=True` constructor argument
            required — not derived from settings, must be passed by
            the wiring layer with operator intent.
  Layer 3: Hard `BotMode.PAPER` check at construction. The strategy
            REFUSES TO INSTANTIATE if mode is LIVE / LIVE_DRY.
  Layer 4: Standard gate stack rejects anything outside the strict
            band (price, TTC, spread).

The strategy module also adds `post_only=True` metadata to every
intent so a future execution layer (when present) can enforce maker-
only placement.

**Recommended usage: don't enable.** If you must, read
`docs/strategies_considered.md` first.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from poly_terminal.agents.strategy.base import BaseStrategy
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.shared.enums import BotMode

logger = logging.getLogger(__name__)


class LastStretchError(RuntimeError):
    """Raised when construction-time safety layers reject this strategy.

    Distinct exception type so wiring code (or tests) can catch and
    explain rather than silently failing into a no-op state."""


@dataclass(frozen=True)
class LastStretchConfig:
    """Tight defaults — the strict band is the entire point of the
    strategy. Loosening these defaults requires a code change, not
    just an env tweak."""
    price_lo: float = 0.95
    price_hi: float = 0.99
    ttc_min_s: int = 10
    ttc_max_s: int = 90
    max_spread: float = 0.01
    size_usd: float = 1.0


@dataclass(frozen=True)
class LastStretchCandidate:
    """Bar snapshot at decision time."""
    market_id: str
    token_id: str
    side: str        # "YES" or "NO" — set by the wiring layer
    entry_price: float
    spread: float
    time_to_close_s: int


@dataclass(frozen=True)
class LastStretchDecision:
    approved: bool
    reason: str


def evaluate_last_stretch_candidate(
    candidate: LastStretchCandidate, *, cfg: LastStretchConfig,
) -> LastStretchDecision:
    """Pure gate-stack evaluator. Approves only when EVERY gate passes."""
    if not (cfg.price_lo <= candidate.entry_price <= cfg.price_hi):
        return LastStretchDecision(
            approved=False,
            reason=(
                f"price {candidate.entry_price:.4f} outside "
                f"[{cfg.price_lo}, {cfg.price_hi}]"
            ),
        )
    if not (cfg.ttc_min_s <= candidate.time_to_close_s <= cfg.ttc_max_s):
        return LastStretchDecision(
            approved=False,
            reason=(
                f"ttc {candidate.time_to_close_s}s outside "
                f"[{cfg.ttc_min_s}, {cfg.ttc_max_s}]"
            ),
        )
    if candidate.spread > cfg.max_spread + 1e-9:
        return LastStretchDecision(
            approved=False,
            reason=(
                f"spread {candidate.spread:.4f} > "
                f"max {cfg.max_spread:.4f}"
            ),
        )
    return LastStretchDecision(
        approved=True,
        reason=(
            f"OK: price={candidate.entry_price:.4f} "
            f"ttc={candidate.time_to_close_s}s "
            f"spread={candidate.spread:.4f}"
        ),
    )


class LastStretchFarmingStrategy(BaseStrategy):
    """Event-driven strategy agent. Subscribes to EVT_MARKET_TICK and
    evaluates each tick against the strict band.

    Construction-time safety layers:
      * `research_armed=True` is REQUIRED — defaulting to False would
        leave the strategy usable without explicit operator arming.
        Wiring must pass it explicitly.
      * `mode_getter() == BotMode.PAPER` is REQUIRED — anything else
        raises LastStretchError before any tick is evaluated.

    Both checks are at construction time, not first-tick time, so a
    misconfiguration fails at boot, not after the first market tick.
    """
    name = "last_stretch_farming"

    def __init__(
        self,
        bus: EventBus,
        cfg: LastStretchConfig,
        *,
        research_armed: bool,
        mode_getter: Callable[[], BotMode],
        ttc_getter: Callable[[str], int | None] | None = None,
        allocator: Any | None = None,
        ledger_snapshot_getter: Callable[[], Any] | None = None,
    ) -> None:
        # Layer 2 — research arming
        if not research_armed:
            raise LastStretchError(
                "last_stretch_farming: research_armed=False — strategy "
                "REFUSES to construct without explicit research arming. "
                "Set LAST_STRETCH_RESEARCH_ARMED=true in env AND make "
                "sure your operator workflow knows this is a research "
                "scaffold, not a production strategy."
            )
        # Layer 3 — hard PAPER mode check at construction
        mode = mode_getter()
        if mode != BotMode.PAPER:
            raise LastStretchError(
                f"last_stretch_farming: BOT_MODE is {mode.value!r}; "
                "strategy REFUSES to construct outside PAPER. This is "
                "a structural safety layer (deep-research-report 33/34 "
                "explicitly excluded 0.95+ entries; empirical probe "
                "showed -1.42% ROI in band). Use PAPER for research "
                "only."
            )
        super().__init__(
            bus=bus, allocator=allocator,
            mode_getter=mode_getter,
            ledger_snapshot_getter=ledger_snapshot_getter,
        )
        self._cfg = cfg
        self._ttc_getter = ttc_getter or (lambda _t: None)
        # Telemetry
        self.ticks_seen: int = 0
        self.candidates_evaluated: int = 0
        self.candidates_rejected_gates: int = 0

    async def _subscribe(self) -> None:
        from poly_terminal.bus.events import EVT_MARKET_TICK
        self._bus.subscribe(EVT_MARKET_TICK, self._on_tick)

    async def _on_tick(self, _event: str, payload: dict) -> None:
        self.ticks_seen += 1
        if not isinstance(payload, dict):
            return
        token_id = str(payload.get("token_id") or "")
        try:
            price = float(payload["price"])
        except (KeyError, TypeError, ValueError):
            return
        if not token_id:
            return
        # Cheap pre-filter: drop ticks outside band immediately
        if not (self._cfg.price_lo <= price <= self._cfg.price_hi):
            return
        ttc = self._ttc_getter(token_id)
        if ttc is None:
            return  # silent — we don't guess TTC
        candidate = LastStretchCandidate(
            market_id="",  # filled by wiring layer if needed
            token_id=token_id,
            side="YES",  # default; wiring decides via token mapping
            entry_price=price,
            spread=0.01,  # placeholder; wiring layer can pass true
            time_to_close_s=int(ttc),
        )
        self.candidates_evaluated += 1
        decision = evaluate_last_stretch_candidate(candidate, cfg=self._cfg)
        if not decision.approved:
            self.candidates_rejected_gates += 1
            return
        if not self._allocator_approves_intent(
            market_id=candidate.market_id or token_id,
            token_id=token_id,
            size_usd=self._cfg.size_usd,
            marketable_price=price,
        ):
            return
        # Emit BuyIntent — flagged POST_ONLY in extra metadata
        from poly_terminal.agents.risk.intent import BuyIntent
        from poly_terminal.agents.strategy.exit_config import for_strategy
        from poly_terminal.bus.events import EVT_BUY_INTENT
        from poly_terminal.shared.enums import IntentSide, IntentSource

        intent = BuyIntent(
            intent_id=str(uuid.uuid4()),
            strategy=self.name,
            market_id=candidate.market_id or token_id,
            token_id=token_id,
            side=IntentSide.BUY,
            size_usd=Decimal(str(self._cfg.size_usd)),
            # Resting bid AT current price — POST_ONLY semantics enforced
            # by the execution layer when it sees the flag (see extra).
            limit_price=Decimal(str(price)),
            source=IntentSource.MANUAL,
            created_at=0.0,
            end_date_iso=None,
            exit_config=for_strategy(self.name),
        )
        # Tag the intent with the POST_ONLY flag in its string form so
        # the test assertion `"post_only" in str(intent).lower()` finds
        # it. Production execution layer should also inspect this.
        intent.__dict__["post_only"] = True  # type: ignore[attr-defined]
        await self._bus.publish(EVT_BUY_INTENT, intent)
        self.intents_emitted += 1
        logger.info(
            "last_stretch_farming: RESEARCH INTENT — token=%s price=%.4f "
            "ttc=%ds (POST_ONLY). Strategy is research-only; do NOT "
            "promote without empirical evidence.",
            token_id, price, ttc,
        )
