"""EndgameMarketEvaluator — wiring layer that combines orderbook,
Gamma metadata, and ConfidenceSource into an `EndgameCandidate`.

Phase 32 P3 (2026-05-09) — companion to `endgame_yield.py`. The
strategy's `evaluate_market` callable is bound to `evaluator.evaluate`
in production; tests stub it directly.

Pure Python — every external dependency arrives as an injected
callable. Same pattern as the rest of the strategy framework: the
evaluator can be unit-tested without an event loop, network, or DB.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable

from poly_terminal.agents.strategy.confidence_source import (
    ConfidenceQuery,
    ConfidenceResult,
    ConfidenceSource,
)
from poly_terminal.agents.strategy.endgame_yield import EndgameCandidate


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GammaMarketMeta:
    """Subset of Gamma's market response the evaluator needs."""
    market_id: str
    end_date_iso: str | None
    close_time_unix_s: int     # 0 if unknown
    yes_token_id: str | None
    no_token_id: str | None


# Type aliases for clarity at the wiring layer.
GammaMetadataFetcherFn = Callable[[str], GammaMarketMeta | None]
PriceGetterFn = Callable[[str], float | None]
DepthGetterFn = Callable[[str], float]


# Sentinel used when the bid is unknown — guarantees the strategy's
# spread gate (default 0.03) rejects, so we fail closed rather than
# trade on degraded data.
_PESSIMISTIC_SPREAD = 1.0


class EndgameMarketEvaluator:
    """Combines orderbook + Gamma + ConfidenceSource → EndgameCandidate.

    Side selection rule:
      * If exactly one of (yes_token, no_token) has a confidence
        entry, that side is chosen.
      * If both have entries (operator misconfig), the higher
        `true_p` wins. The downstream EV gate filters the rest.
      * If neither, the evaluator returns None (silent skip).
    """

    def __init__(
        self,
        *,
        confidence_source: ConfidenceSource,
        gamma_metadata_fetcher: GammaMetadataFetcherFn,
        best_ask_getter: PriceGetterFn,
        best_bid_getter: PriceGetterFn,
        depth_getter: DepthGetterFn | None = None,
        now_ts_fn: Callable[[], int] = lambda: int(time.time()),
        target_exit_default: float = 0.99,
    ) -> None:
        self._confidence = confidence_source
        self._meta_fn = gamma_metadata_fetcher
        self._ask = best_ask_getter
        self._bid = best_bid_getter
        self._depth = depth_getter
        self._now = now_ts_fn
        self._target_exit_default = target_exit_default

    # ── Public surface ────────────────────────────────────────────

    def evaluate(self, market_id: str) -> EndgameCandidate | None:
        try:
            return self._evaluate_inner(market_id)
        except Exception:
            logger.exception(
                "endgame_evaluator: unexpected error evaluating %s "
                "— returning None for safety", market_id,
            )
            return None

    # ── Internal ──────────────────────────────────────────────────

    def _evaluate_inner(self, market_id: str) -> EndgameCandidate | None:
        try:
            meta = self._meta_fn(market_id)
        except Exception:
            logger.exception(
                "endgame_evaluator: gamma metadata fetch failed for %s",
                market_id,
            )
            return None
        if meta is None:
            return None

        # Choose the side with confidence (or higher true_p when both set).
        choice = self._pick_token(market_id, meta)
        if choice is None:
            return None
        token_id, side, conf = choice

        # Fetch price + bid (allow either to fail — evaluator stays calm).
        try:
            ask = self._ask(token_id)
        except Exception:
            logger.exception("endgame_evaluator: ask getter raised")
            return None
        if ask is None:
            return None
        try:
            bid = self._bid(token_id)
        except Exception:
            logger.exception("endgame_evaluator: bid getter raised")
            bid = None

        spread = (
            float(ask) - float(bid) if (bid is not None) else _PESSIMISTIC_SPREAD
        )

        # Time-to-close — clamp at 0 (already-closed markets get caught
        # by the strategy's ttc gate downstream; we don't suppress them
        # here so the audit trail records the rejection reason).
        now_ts = int(self._now())
        ttc_s = max(0, int(meta.close_time_unix_s) - now_ts)

        # Depth — 0 when no getter wired (strategy's depth gate then rejects).
        if self._depth is not None:
            try:
                depth = float(self._depth(token_id))
            except Exception:
                logger.exception("endgame_evaluator: depth getter raised")
                depth = 0.0
        else:
            depth = 0.0

        return EndgameCandidate(
            market_id=market_id,
            token_id=token_id,
            side=side,
            entry_price=float(ask),
            target_exit=self._target_exit_default,
            spread=float(spread),
            time_to_close_s=ttc_s,
            top_3_depth=depth,
            confidence=conf,
            end_date_iso=meta.end_date_iso,
            contradictory_catalyst_pending=False,
        )

    def _pick_token(
        self, market_id: str, meta: GammaMarketMeta,
    ) -> tuple[str, str, ConfidenceResult] | None:
        candidates: list[tuple[str, str, ConfidenceResult]] = []
        if meta.yes_token_id:
            res = self._confidence.lookup(
                ConfidenceQuery(market_id=market_id, token_id=meta.yes_token_id)
            )
            if res.found:
                candidates.append((meta.yes_token_id, "YES", res))
        if meta.no_token_id:
            res = self._confidence.lookup(
                ConfidenceQuery(market_id=market_id, token_id=meta.no_token_id)
            )
            if res.found:
                candidates.append((meta.no_token_id, "NO", res))
        if not candidates:
            return None
        # Higher true_p wins.
        candidates.sort(key=lambda x: x[2].true_p, reverse=True)
        return candidates[0]


__all__ = [
    "EndgameMarketEvaluator",
    "GammaMarketMeta",
    "GammaMetadataFetcherFn",
    "PriceGetterFn",
    "DepthGetterFn",
]
