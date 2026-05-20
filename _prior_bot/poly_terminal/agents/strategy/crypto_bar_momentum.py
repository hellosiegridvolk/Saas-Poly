"""CryptoBarMomentum — short-bar price-call strategy for Polymarket
5–15 minute crypto Up/Down bars.

Phase 37 (2026-05-11). Built per `docs/strategy_rebuild_2026-05-11.md`
§4 option-A: Polymarket's crypto markets are 5–15 minute bars, not
multi-hour. Endgame_yield's 10min–6h regime doesn't fit; this is the
short-bar counterpart.

**Fundamentally different from endgame_yield:**

  * No operator confidence overrides — the bar resolves in seconds,
    no time for independent verification. Signal comes from
    short-term price momentum / orderbook imbalance instead.
  * Tight TTC band (30s–300s for 5-min bars; 30s–900s for 15-min).
  * Wider price band (0.45–0.65) — entering when one side is
    establishing dominance, not when it's already near-certain.
  * Lower confidence floor — single-position discipline + small size
    is the risk management, not high-conviction edge per trade.

**Status as of this commit: scaffold only.** The signal function is a
stub that returns NO_SIGNAL until calibration. The strategy is wired
to be plumbing-complete (gates, allocator, EV math) so it can be
turned on for PAPER soak the moment a real signal is in place. The
config flag `STRATEGY_CRYPTO_BAR_MOMENTUM` defaults to OFF.

**Do NOT enable for LIVE.** PAPER readiness is ~20% until the signal
is calibrated against historical bars. LIVE readiness is 0%.
"""
from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from poly_terminal.agents.strategy.base import BaseStrategy
from poly_terminal.bus.event_bus import EventBus

logger = logging.getLogger(__name__)


# Crypto Up/Down bar question patterns — same word-boundary regex
# the picker uses for the asset filter.
_CRYPTO_ASSET_PATTERN = re.compile(
    r"\b(?:btc|bitcoin|eth|ethereum|ether|sol|solana|xrp|ripple)\b",
    re.IGNORECASE,
)

# "Up or Down" identifies the Polymarket short-bar market type.
_UP_OR_DOWN_PATTERN = re.compile(r"up or down", re.IGNORECASE)


def is_short_bar_crypto_market(question: str) -> bool:
    """Return True iff the question text matches a Polymarket crypto
    Up/Down bar (the only market type this strategy trades).

    Both predicates must match — "Up or Down" alone could be a non-
    crypto market (rare but possible); crypto-asset alone could be a
    long-form speculation market.
    """
    if not question:
        return False
    return (
        _CRYPTO_ASSET_PATTERN.search(question) is not None
        and _UP_OR_DOWN_PATTERN.search(question) is not None
    )


@dataclass(frozen=True)
class CryptoBarMomentumConfig:
    """Tunable parameters. Defaults are conservative scaffold values;
    calibrate against backtest before enabling."""
    # TTC band — enter only when bar is mid-life, not at creation
    # (too speculative) or about to close (no edge time).
    ttc_min_s: int = 30
    ttc_max_s: int = 300
    # Price band — broader than endgame because we're entering on
    # momentum, not near-certainty.
    price_lo: float = 0.45
    price_hi: float = 0.65
    # Position sizing — small, fixed-size for PAPER scaffold.
    size_usd: float = 1.0
    # Minimum spread acceptable (cents). Wider than endgame because
    # short bars often have thinner books.
    max_spread: float = 0.05
    # Signal threshold — the stub signal function returns a momentum
    # score in [-1, 1]. We BUY YES if score > threshold, BUY NO if
    # score < -threshold, otherwise no signal.
    signal_threshold: float = 0.5


@dataclass(frozen=True)
class BarCandidate:
    """Snapshot of a short-bar at decision time. The wiring layer
    builds this from orderbook + market metadata."""
    market_id: str
    question: str
    yes_token_id: str
    no_token_id: str
    yes_price: float
    no_price: float
    spread: float
    time_to_close_s: int
    # Short-term momentum signal in [-1, 1]:
    #   +1 = strong YES momentum (buy YES at yes_price)
    #   -1 = strong NO momentum (buy NO at no_price)
    #    0 = no signal
    # Wiring layer computes this from tick history; tests inject
    # the value directly.
    momentum_score: float


@dataclass(frozen=True)
class SignalEvaluation:
    """Result of evaluating a BarCandidate. `direction` is YES or NO
    when the strategy wants to enter; None when no signal."""
    direction: str | None
    entry_price: float | None
    reason: str  # human-readable explanation of accept/reject


def evaluate_bar(
    candidate: BarCandidate, *, cfg: CryptoBarMomentumConfig,
) -> SignalEvaluation:
    """Pure decision function — apply the gate stack to a candidate.

    Returns a SignalEvaluation with direction='YES'|'NO' iff every gate
    clears AND the momentum signal is strong enough. Otherwise
    direction=None with a `reason` string explaining the rejection.

    Tests pin every gate. Wiring layer just calls this with a built
    BarCandidate and acts on `direction`.
    """
    # 1. Market type gate — must be a crypto Up/Down bar.
    if not is_short_bar_crypto_market(candidate.question):
        return SignalEvaluation(
            direction=None, entry_price=None,
            reason=f"not a crypto Up/Down bar: {candidate.question[:40]!r}",
        )
    # 2. TTC gate — must be in the short-bar entry window.
    if not (cfg.ttc_min_s <= candidate.time_to_close_s <= cfg.ttc_max_s):
        return SignalEvaluation(
            direction=None, entry_price=None,
            reason=(
                f"ttc={candidate.time_to_close_s}s outside band "
                f"[{cfg.ttc_min_s}, {cfg.ttc_max_s}]"
            ),
        )
    # 3. Spread gate — wider books are too costly to round-trip.
    if candidate.spread > cfg.max_spread + 1e-9:
        return SignalEvaluation(
            direction=None, entry_price=None,
            reason=(
                f"spread={candidate.spread:.3f} > "
                f"max={cfg.max_spread:.3f}"
            ),
        )
    # 4. Signal gate — momentum must clear the threshold (signed).
    score = candidate.momentum_score
    if score >= cfg.signal_threshold:
        # Buy YES side.
        if not (cfg.price_lo <= candidate.yes_price <= cfg.price_hi):
            return SignalEvaluation(
                direction=None, entry_price=None,
                reason=(
                    f"yes_price={candidate.yes_price:.3f} outside band "
                    f"[{cfg.price_lo}, {cfg.price_hi}]"
                ),
            )
        return SignalEvaluation(
            direction="YES",
            entry_price=candidate.yes_price,
            reason=f"momentum_score={score:.3f} ≥ {cfg.signal_threshold}",
        )
    if score <= -cfg.signal_threshold:
        # Buy NO side.
        if not (cfg.price_lo <= candidate.no_price <= cfg.price_hi):
            return SignalEvaluation(
                direction=None, entry_price=None,
                reason=(
                    f"no_price={candidate.no_price:.3f} outside band "
                    f"[{cfg.price_lo}, {cfg.price_hi}]"
                ),
            )
        return SignalEvaluation(
            direction="NO",
            entry_price=candidate.no_price,
            reason=f"momentum_score={score:.3f} ≤ -{cfg.signal_threshold}",
        )
    return SignalEvaluation(
        direction=None, entry_price=None,
        reason=f"momentum_score={score:.3f} below threshold magnitude",
    )


def stub_momentum_score(
    _ticks: list[dict] | None = None,
) -> float:
    """**SCAFFOLD ONLY** — always returns 0.0 (no signal).

    Replace with real momentum calculation. Candidate signal forms:
      * Tick-direction sign over last N seconds (cumulative BUY minus
        SELL volume, signed)
      * Orderbook imbalance ratio: top-of-book YES vs NO depth
      * Price momentum: (current_price - price_30s_ago) sign

    Until calibrated against a backtest, the stub keeps the strategy
    fail-closed even when the config flag is on. This prevents an
    operator from accidentally promoting an uncalibrated strategy to
    PAPER fills.
    """
    return 0.0


class CryptoBarMomentumStrategy(BaseStrategy):
    """Strategy agent — subscribes to the bus and emits signals.

    Wiring contract:
      * `evaluate_bar_at` callable is injected by the wiring layer.
        It takes a (market_id, token_id) pair and returns a built
        BarCandidate (orderbook snapshot + momentum score), or None
        if the market is unknown.
      * On `EVT_CONTEXT_OK`, the strategy asks `evaluate_bar_at` for
        a candidate; if returned, runs the gate stack via
        `evaluate_bar`; if direction approved, runs the allocator
        gate; if approved, emits EVT_BUY_INTENT.
    """
    name = "crypto_bar_momentum"

    def __init__(
        self,
        bus: EventBus,
        cfg: CryptoBarMomentumConfig,
        evaluate_bar_at: Callable[[str, str], BarCandidate | None],
        *,
        allocator: Any | None = None,
        mode_getter: Callable[[], Any] | None = None,
        ledger_snapshot_getter: Callable[[], Any] | None = None,
    ) -> None:
        super().__init__(
            bus=bus, allocator=allocator,
            mode_getter=mode_getter,
            ledger_snapshot_getter=ledger_snapshot_getter,
        )
        self._cfg = cfg
        self._evaluate_bar_at = evaluate_bar_at
        # Bookkeeping for tests + observability
        self.candidates_evaluated: int = 0
        self.candidates_rejected_gates: int = 0

    async def _subscribe(self) -> None:
        # Same trigger event as endgame_yield (context-approved
        # markets). The wiring layer ensures EVT_CONTEXT_OK fires
        # for relevant tokens.
        from poly_terminal.bus.events import EVT_CONTEXT_OK
        self._bus.subscribe(EVT_CONTEXT_OK, self._on_context_ok)

    async def _on_context_ok(self, _event: str, payload: dict) -> None:
        """Evaluate a single candidate. Bus subscriber entry point."""
        market_id = str(payload.get("market_id") or "")
        token_id = str(payload.get("token_id") or "")
        if not market_id or not token_id:
            return
        candidate = self._evaluate_bar_at(market_id, token_id)
        if candidate is None:
            return
        self.candidates_evaluated += 1
        decision = evaluate_bar(candidate, cfg=self._cfg)
        if decision.direction is None:
            self.candidates_rejected_gates += 1
            return
        # Allocator gate — applies the system-level caps.
        if not self._allocator_approves_intent(
            market_id=candidate.market_id,
            token_id=(
                candidate.yes_token_id if decision.direction == "YES"
                else candidate.no_token_id
            ),
            size_usd=self._cfg.size_usd,
            marketable_price=decision.entry_price or 0.5,
        ):
            return
        # Emit intent. Wiring identical to endgame_yield's pattern.
        from poly_terminal.agents.risk.intent import BuyIntent
        from poly_terminal.agents.strategy.exit_config import for_strategy
        from poly_terminal.bus.events import EVT_BUY_INTENT
        from poly_terminal.shared.enums import IntentSide, IntentSource

        token = (
            candidate.yes_token_id if decision.direction == "YES"
            else candidate.no_token_id
        )
        # Choose IntentSource — re-use existing enum values where
        # semantically close. A dedicated CRYPTO_BAR_MOMENTUM value
        # would require a coordinated enum migration; until that lands
        # we route through MANUAL so the downstream classification
        # treats it as a discretionary entry (the strategy is opt-in
        # PAPER-only anyway).
        intent = BuyIntent(
            intent_id=str(uuid.uuid4()),
            strategy=self.name,
            market_id=candidate.market_id,
            token_id=token,
            side=IntentSide.BUY,  # always BUY; YES vs NO is in token_id
            size_usd=Decimal(str(self._cfg.size_usd)),
            limit_price=Decimal(str(
                min(0.99, (decision.entry_price or 0.5) + 0.02)
            )),
            source=IntentSource.MANUAL,
            created_at=0.0,
            end_date_iso=None,
            exit_config=for_strategy(self.name),
        )
        await self._bus.publish(EVT_BUY_INTENT, intent)
        self.intents_emitted += 1
