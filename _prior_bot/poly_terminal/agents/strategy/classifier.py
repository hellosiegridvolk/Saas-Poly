"""StrategyClassifier — decision-tree dispatch.

Phase 32 P3 (2026-05-09) — implements §12 of the playbook
(`docs/strategy polymarket.md`) and the priority order in §19.6.

Inputs: a `MarketContext` snapshot.
Outputs: the strategy_name to dispatch to (or `None` for SKIP), and a
human-readable reason for the dashboard / audit trail.

This module is *purely a router*. It does not emit signals, build
intents, or move capital. It only chooses which strategy module owns
a candidate market under the current regime.

**Scope: framework strategies only.** Event-driven legacy strategies
(`flash_crash`, `scalp_window`, `dump_hedge`) intentionally bypass
the classifier — they subscribe to specific tick patterns rather
than per-market context lookups. The RiskAllocator gate is the
single point of LIVE-mode enforcement for those strategies (see
`docs/strategy_wiring_debt.md` for the rationale).

Priority order (from playbook §19.6):
    1. resolution_arb
    2. endgame_yield (incl. ninety_cent sub-band)
    3. certainty_drift  — not implemented in this scaffold (returns None)
    4. arbitrage_*      — not implemented in this scaffold
    5. copy_trade
    6. indicator_signal
    7. copy_scalp
    8. pure_scalping    — not implemented in this scaffold

Audit hook: `regime_tags(ctx)` reports ALL applicable regime tags
for a market (orthogonal to `classify()` which picks one strategy).
Used by the strategy_rejections audit so operators can see WHY a
market was skipped.
"""
from __future__ import annotations

from dataclasses import dataclass

from poly_terminal.agents.strategy.framework import RegimeTag


# Constants — pinned to the playbook trigger expressions in §19.1.
_NEAR_RESOLUTION_MIN_SOURCES = 2
_RESOLUTION_ARB_MAX_PRICE = 0.97
_RESOLUTION_ARB_MAX_SPREAD = 0.03

_ENDGAME_PRICE_LO = 0.88
_ENDGAME_PRICE_HI = 0.97
_ENDGAME_TTC_MIN_S = 600        # 10m
_ENDGAME_TTC_MAX_S = 21600      # 6h
_ENDGAME_EV_MARGIN = 0.03       # confidence must exceed break-even by 3%
_ENDGAME_MAX_SPREAD = 0.03

_NINETY_CENT_PRICE_LO = 0.88
_NINETY_CENT_PRICE_HI = 0.93
_NINETY_CENT_TTC_MAX_S = 7200   # 2h

_COPY_TTC_MIN_S = 900
_COPY_MARKET_DURATION_MIN_S = 1800
_COPY_MAX_SPREAD = 0.08
_COPY_SCALP_MEDIAN_HOLD_THRESHOLD_S = 60

_INDICATOR_MIN_EDGE = 0.05
_INDICATOR_TTC_MIN_S = 300
_INDICATOR_MAX_SPREAD = 0.04
_INDICATOR_MIN_DEPTH_RATIO = 5.0

# Regime-tag thresholds (used by `regime_tags()` for audit only).
_NEAR_RESOLUTION_TTC_S = 600          # < 10m → near-resolution
_HEALTHY_DURATION_TTC_MIN_S = 1800    # >= 30m
_PENNY_TOKEN_PRICE = 0.05             # <= 5¢
_WIDE_SPREAD_THRESHOLD = 0.08         # > 8¢
_LIQUIDITY_DEPTH_RATIO = 5.0          # depth < 5x size → low liquidity


@dataclass(frozen=True)
class MarketContext:
    """Snapshot of a candidate market + wallet attribution.

    Default values (in tests via `_ctx()`) are "nothing applies" so
    each test sets only the flags relevant to its branch. Production
    wiring populates this from the orderbook + Gamma + wallet_intel.
    """
    market_id: str
    token_id: str
    entry_price: float
    spread: float
    time_to_close_s: int
    market_duration_s: int
    top_3_depth: float
    size: float
    recent_trade_count: int

    # Resolution arbitrage
    outcome_externally_verified: bool = False
    outcome_sources_count: int = 0
    is_market_resolved: bool = False

    # Endgame yield
    verified_true_p: float = 0.0
    contradictory_catalyst_pending: bool = False

    # Wallet copy
    wallet_signal: str | None = None         # wallet address or None
    wallet_median_hold_s: int = 0

    # External / indicator signal
    external_signal_edge: float = 0.0
    external_signal_expires_at: int = 0
    now_ts: int = 0


def _break_even_p(entry_price: float, target_exit: float = 0.99) -> float:
    """For an entry at `p` targeting `t`, the break-even probability
    is `loss / (loss + gain) = p / (p + (t - p))` = p / t."""
    if target_exit <= entry_price:
        return 1.0  # no upside → unreachable
    return entry_price / target_exit


def _matches_resolution_arb(ctx: MarketContext) -> tuple[bool, str]:
    if not ctx.outcome_externally_verified:
        return False, "outcome not externally verified"
    if ctx.outcome_sources_count < _NEAR_RESOLUTION_MIN_SOURCES:
        return False, (
            f"only {ctx.outcome_sources_count} source(s); need "
            f">= {_NEAR_RESOLUTION_MIN_SOURCES}"
        )
    if ctx.is_market_resolved:
        return False, "market already resolved"
    if ctx.entry_price > _RESOLUTION_ARB_MAX_PRICE:
        return False, f"price {ctx.entry_price:.2f} > resolution_arb max"
    if ctx.spread > _RESOLUTION_ARB_MAX_SPREAD:
        return False, f"spread {ctx.spread:.3f} > resolution_arb max"
    return True, "outcome verified, market not yet resolved"


def _matches_endgame_yield(ctx: MarketContext) -> tuple[bool, str]:
    if not (_ENDGAME_PRICE_LO <= ctx.entry_price <= _ENDGAME_PRICE_HI):
        return False, (
            f"price {ctx.entry_price:.2f} outside endgame band "
            f"[{_ENDGAME_PRICE_LO:.2f}, {_ENDGAME_PRICE_HI:.2f}]"
        )
    if not (_ENDGAME_TTC_MIN_S <= ctx.time_to_close_s <= _ENDGAME_TTC_MAX_S):
        return False, (
            f"time_to_close {ctx.time_to_close_s}s outside "
            f"[{_ENDGAME_TTC_MIN_S}, {_ENDGAME_TTC_MAX_S}]"
        )
    if ctx.spread > _ENDGAME_MAX_SPREAD:
        return False, f"spread {ctx.spread:.3f} > endgame max"
    if ctx.contradictory_catalyst_pending:
        return False, "contradictory catalyst pending"
    be = _break_even_p(ctx.entry_price)
    if ctx.verified_true_p < be + _ENDGAME_EV_MARGIN:
        return False, (
            f"verified_true_p {ctx.verified_true_p:.3f} < break-even "
            f"{be:.3f} + margin {_ENDGAME_EV_MARGIN:.2f}"
        )
    return True, (
        f"endgame in band; true_p={ctx.verified_true_p:.3f} >= "
        f"break-even+{_ENDGAME_EV_MARGIN:.2f}"
    )


def _matches_ninety_cent(ctx: MarketContext) -> bool:
    return (
        _NINETY_CENT_PRICE_LO <= ctx.entry_price <= _NINETY_CENT_PRICE_HI
        and ctx.time_to_close_s <= _NINETY_CENT_TTC_MAX_S
    )


def _matches_copy(ctx: MarketContext) -> tuple[bool, str]:
    if ctx.wallet_signal is None:
        return False, "no wallet signal"
    if ctx.time_to_close_s < _COPY_TTC_MIN_S:
        return False, (
            f"time_to_close {ctx.time_to_close_s}s < copy min "
            f"{_COPY_TTC_MIN_S}"
        )
    if ctx.market_duration_s < _COPY_MARKET_DURATION_MIN_S:
        return False, (
            f"market_duration {ctx.market_duration_s}s < copy min "
            f"{_COPY_MARKET_DURATION_MIN_S}"
        )
    if ctx.spread > _COPY_MAX_SPREAD:
        return False, f"spread {ctx.spread:.3f} > copy max"
    return True, "wallet signal in healthy regime"


def _matches_indicator(ctx: MarketContext) -> tuple[bool, str]:
    if ctx.external_signal_edge < _INDICATOR_MIN_EDGE:
        return False, (
            f"edge {ctx.external_signal_edge:.3f} < "
            f"{_INDICATOR_MIN_EDGE:.2f}"
        )
    if (
        ctx.external_signal_expires_at
        and ctx.now_ts >= ctx.external_signal_expires_at
    ):
        return False, "external signal expired"
    if ctx.time_to_close_s < _INDICATOR_TTC_MIN_S:
        return False, (
            f"time_to_close {ctx.time_to_close_s}s < indicator min"
        )
    if ctx.spread > _INDICATOR_MAX_SPREAD:
        return False, f"spread {ctx.spread:.3f} > indicator max"
    if ctx.size > 0 and ctx.top_3_depth < ctx.size * _INDICATOR_MIN_DEPTH_RATIO:
        return False, "depth < 5x size"
    return True, "external edge above threshold"


class StrategyClassifier:
    """Decision-tree dispatch — pure function, no I/O.

    Two surfaces:
      * `classify(ctx) -> str | None` — strategy_name or None (skip).
      * `classify_with_detail(ctx)`  — returns `(name, reason)`.
    """

    def classify(self, ctx: MarketContext) -> str | None:
        name, _ = self.classify_with_detail(ctx)
        return name

    def regime_tags(self, ctx: MarketContext) -> tuple[RegimeTag, ...]:
        """Return ALL applicable regime tags for `ctx`.

        Independent of `classify()` — used by the strategy_rejections
        audit so operators can see why a market was skipped (e.g.,
        "tagged NEAR_RESOLUTION + WIDE_SPREAD"). Multiple tags can
        apply to one market.
        """
        tags: list[RegimeTag] = []

        # Time-to-close band
        if ctx.time_to_close_s < _NEAR_RESOLUTION_TTC_S:
            tags.append(RegimeTag.NEAR_RESOLUTION)
        if ctx.time_to_close_s >= _HEALTHY_DURATION_TTC_MIN_S:
            tags.append(RegimeTag.HEALTHY_DURATION)

        # Price band — penny tokens are a recurring trap class
        if 0 < ctx.entry_price <= _PENNY_TOKEN_PRICE:
            tags.append(RegimeTag.PENNY_TOKEN)

        # Spread
        if ctx.spread > _WIDE_SPREAD_THRESHOLD:
            tags.append(RegimeTag.WIDE_SPREAD)

        # Liquidity (depth relative to size)
        if (
            ctx.size > 0
            and ctx.top_3_depth < ctx.size * _LIQUIDITY_DEPTH_RATIO
        ):
            tags.append(RegimeTag.LOW_LIQUIDITY)

        # Oracle / external verification (resolution_arb regime)
        if (
            ctx.outcome_externally_verified
            and ctx.outcome_sources_count >= _NEAR_RESOLUTION_MIN_SOURCES
        ):
            tags.append(RegimeTag.ORACLE_VERIFIED)

        if not tags:
            tags.append(RegimeTag.UNKNOWN)
        return tuple(tags)

    def classify_with_detail(
        self, ctx: MarketContext
    ) -> tuple[str | None, str]:
        # Resolution arb is special-cased FIRST: a verified-but-resolved
        # market must SKIP entirely (the bot can't trade something
        # already settled), regardless of price band.
        if ctx.outcome_externally_verified and ctx.is_market_resolved:
            return None, "market already resolved on chain"

        ok, why = _matches_resolution_arb(ctx)
        if ok:
            return "resolution_arb", why

        ok, why = _matches_endgame_yield(ctx)
        if ok:
            if _matches_ninety_cent(ctx):
                return "ninety_cent", why + " (ninety_cent sub-band)"
            return "endgame_yield", why

        ok, why = _matches_copy(ctx)
        if ok:
            assert ctx.wallet_signal is not None
            if ctx.wallet_median_hold_s <= _COPY_SCALP_MEDIAN_HOLD_THRESHOLD_S:
                return "copy_scalp", why + " (median_hold <= 60s)"
            return "copy_trade", why + " (median_hold > 60s)"

        ok, why = _matches_indicator(ctx)
        if ok:
            return "indicator_signal", why

        return None, "no strategy preconditions matched"


__all__ = [
    "MarketContext",
    "StrategyClassifier",
]
