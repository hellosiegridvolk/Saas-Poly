"""Strategy framework — common types for the regime allocator rebuild.

Phase 32 P3 (2026-05-09) — companion to the playbook in
`docs/strategy polymarket.md` §1, §13, §14 and the rebuild plan in
`docs/phase2_unblockers_and_readiness.md` §8.

Every strategy emits a normalized `StrategySignal`; only the shared
`RiskAllocator` may approve capital. This module ONLY defines types —
no I/O, no agents, no wiring. Strategies and the allocator depend on
it; the rest of the bot does not.

Design choices:
  * Frozen dataclasses so signals can be safely passed across agents
    without accidental mutation. The `extra` dict is intentionally
    mutable inside the frozen wrapper for forward-compat extension
    metadata; treat it as read-only at strategy boundaries.
  * Reason codes are an enum (not strings) so callers can grep the
    code path that emits them and the test suite can pin behavior.
  * Regime tags are an enum used for cross-strategy regime-overlap
    decisions in the classifier.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RegimeTag(str, Enum):
    """Market-regime classification used by the Strategy Classifier
    to decide which strategy module owns a candidate market.

    Multiple tags may apply to one market (e.g. `HEALTHY_DURATION` +
    `HIGH_LIQUIDITY`); the classifier picks the highest-priority
    strategy whose preconditions match.
    """
    NEAR_RESOLUTION = "near_resolution"
    HEALTHY_DURATION = "healthy_duration"
    PENNY_TOKEN = "penny_token"
    WIDE_SPREAD = "wide_spread"
    LOW_LIQUIDITY = "low_liquidity"
    ORACLE_VERIFIED = "oracle_verified"
    UNKNOWN = "unknown"


class RejectReason(str, Enum):
    """Standard rejection reason codes — every rejected signal must
    carry one. See `strategy polymarket.md` §14."""
    NEAR_RESOLUTION = "REJECT_NEAR_RESOLUTION"
    LOW_LIQUIDITY = "REJECT_LOW_LIQUIDITY"
    WIDE_SPREAD = "REJECT_WIDE_SPREAD"
    WALLET_NEGATIVE_ROLLING = "REJECT_WALLET_NEGATIVE_ROLLING"
    NO_EXIT_BID = "REJECT_NO_EXIT_BID"
    NO_CONFIDENCE_SOURCE = "REJECT_NO_CONFIDENCE_SOURCE"
    EDGE_BELOW_THRESHOLD = "REJECT_EDGE_BELOW_THRESHOLD"
    WRONG_REGIME = "REJECT_WRONG_REGIME"
    PROBATION_FLOOR = "REJECT_PROBATION_FLOOR"
    QUARANTINED_TOKEN = "REJECT_QUARANTINED_TOKEN"
    DAILY_LOSS_CAP = "REJECT_DAILY_LOSS_CAP"
    DUPLICATE_STRATEGY_OPEN = "REJECT_DUPLICATE_STRATEGY_OPEN"
    TOKEN_ALREADY_OPEN = "REJECT_TOKEN_ALREADY_OPEN"
    POSITION_LIMIT = "REJECT_POSITION_LIMIT"
    EXPOSURE_LIMIT = "REJECT_EXPOSURE_LIMIT"
    CAPITAL_CAP = "REJECT_CAPITAL_CAP"
    STRATEGY_DISABLED = "REJECT_STRATEGY_DISABLED"
    OUTRANKED_BY_HIGHER = "REJECT_OUTRANKED_BY_HIGHER"


@dataclass(frozen=True)
class StrategySignal:
    """Normalized output of every strategy module.

    Required fields are positional/keyword; `extra` carries
    strategy-specific debug metadata (wallet id, edge bps breakdown,
    confidence-source URL, …) that the audit layer can persist.
    """
    strategy_name: str
    market_id: str
    token_id: str
    side: str  # "YES" or "NO"
    confidence: float  # 0..1
    edge_bps: int
    max_loss_usd: float
    target_exit: float
    stop_exit: float
    max_hold_s: int
    regime_tags: tuple[RegimeTag, ...] = ()
    reason_codes: tuple[str, ...] = ()
    extra: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.side not in ("YES", "NO"):
            raise ValueError(
                f"StrategySignal.side must be 'YES' or 'NO', got {self.side!r}"
            )
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(
                f"StrategySignal.confidence must be in [0,1], "
                f"got {self.confidence!r}"
            )
        if self.max_loss_usd < 0:
            raise ValueError(
                f"StrategySignal.max_loss_usd must be >= 0, "
                f"got {self.max_loss_usd!r}"
            )


@dataclass(frozen=True)
class StrategyDecision:
    """Result of `RiskAllocator.approve(signal)`.

    `approved=True` is the only path to capital. `approved=False`
    carries a `RejectReason` for forensic audit; `detail` is a
    human-readable note for the dashboard.
    """
    approved: bool
    signal: StrategySignal | None
    reason: RejectReason | None
    detail: str = ""


__all__ = [
    "RegimeTag",
    "RejectReason",
    "StrategySignal",
    "StrategyDecision",
]
