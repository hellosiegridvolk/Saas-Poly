"""Retroactive PnL reconciler for flat closes that hit the shadow-price
fallback path.

2026-05-05 — when `ExitAgent._resolve_exit_price` falls back to
`pos.entry_price` (because `get_best_bid` raised — typically a 404 on
Polymarket's `/price` endpoint after the orderbook has been purged
post-resolution), the close is recorded with `realized_pnl=0` and
`exit_price = entry_price`. That's wrong in either direction: the
market may have settled YES (the position is worth shares × $1) or NO
(worth $0). Recording it as $0 PnL silently inflates win-rate metrics
and hides real losses.

This module sweeps those rows, queries Gamma `/markets?condition_ids=
…&closed=true` for each unique market, and rewrites realized_pnl from
the resolved `outcomePrices`. Idempotent — reconciled rows are tagged
`outcome='TIME_RECONCILED'` so subsequent passes skip them.

The Gamma resolver is the same one used by `RedeemerAgent`
(`agents.redeemer.agent.GammaMarketResolver`); `outcomePrices` parsing
mirrors that agent's logic so the win/loss/refund branches stay
consistent across the codebase.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

logger = logging.getLogger(__name__)


# ── Protocols (test seams) ───────────────────────────────────────────


class _PositionsRepoProto(Protocol):
    async def fetch_unreconciled_flat_closes(
        self, *, limit: int | None = None
    ) -> list[dict[str, Any]]: ...

    async def update_reconciled_pnl(
        self,
        *,
        position_id: int,
        exit_price: float,
        realized_pnl: float,
        outcome: str = "TIME_RECONCILED",
    ) -> bool: ...


class _MarketResolverProto(Protocol):
    async def fetch_resolution(
        self, condition_id: str
    ) -> dict[str, Any] | None: ...


# ── Stats / config ───────────────────────────────────────────────────


@dataclass
class ReconcileStats:
    candidates: int = 0          # rows pulled from fetch_unreconciled_flat_closes
    reconciled_win: int = 0      # outcomePrice = 1 → set realized_pnl > 0
    reconciled_loss: int = 0     # outcomePrice = 0 → set realized_pnl < 0
    market_pending: int = 0      # market not yet resolved on Gamma
    market_missing: int = 0      # Gamma returned no row for condition_id
    market_malformed: int = 0    # clobTokenIds / outcomePrices mismatch
    token_not_in_market: int = 0 # position.token_id not in clobTokenIds
    refund_or_invalid: int = 0   # outcomePrice not in {0, 1} (rare refund)
    update_no_op: int = 0        # update_reconciled_pnl returned False
    errors: int = 0              # Gamma fetch raised
    pnl_corrected_usd: float = 0.0  # signed sum of |new_pnl - old_pnl|
    markets_queried: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class ReconcileConfig:
    max_concurrent_lookups: int = 4   # match RedeemerConfig default
    candidates_per_pass: int | None = None  # None = no LIMIT


# ── Helpers ──────────────────────────────────────────────────────────


def _coerce_list(raw: Any) -> list[str]:
    """Gamma encodes `clobTokenIds` and `outcomePrices` as JSON strings,
    not native arrays. Mirror redeemer's parser to stay consistent."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (ValueError, TypeError):
            return []
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    return []


def _compute_realized_pnl_for_buy(
    *,
    shares: float,
    cost_basis_usd: float,
    outcome_price: float,
) -> float:
    """One BUY position resolves to `shares * outcome_price` USD on
    redemption (each share pays $1 if winning, $0 if losing). Realized
    PnL = redemption value − cost_basis.

    All bot positions are BUYs in v3, but this helper takes shares +
    cost_basis explicitly so it can be unit-tested without a position
    object.
    """
    redemption_value = float(shares) * float(outcome_price)
    return redemption_value - float(cost_basis_usd)


def _classify_outcome_price(price_str: str) -> str:
    """Same buckets RedeemerAgent uses: '1'/'1.0' → 'win',
    '0'/'0.0' → 'loss', anything else → 'refund_or_invalid'."""
    s = (price_str or "").strip()
    if s in ("1", "1.0", "1.00"):
        return "win"
    if s in ("0", "0.0", "0.00"):
        return "loss"
    return "refund_or_invalid"


# ── Reconciler ───────────────────────────────────────────────────────


class ResolutionReconciler:
    """One-shot or loop driver for resolution reconciliation.

    Public surface is `reconcile_once()` which returns ReconcileStats.
    Wrap in a CLI loop or a periodic task as needed; reconcile passes
    are independent and idempotent so calling more often than markets
    resolve is harmless.
    """

    def __init__(
        self,
        positions_repo: _PositionsRepoProto,
        market_resolver: _MarketResolverProto,
        cfg: ReconcileConfig | None = None,
    ) -> None:
        self._positions = positions_repo
        self._resolver = market_resolver
        self._cfg = cfg or ReconcileConfig()

    async def reconcile_once(self) -> ReconcileStats:
        stats = ReconcileStats()

        candidates = await self._positions.fetch_unreconciled_flat_closes(
            limit=self._cfg.candidates_per_pass,
        )
        stats.candidates = len(candidates)
        if not candidates:
            return stats

        # Group candidates by market_id so we Gamma-fetch each market
        # at most once even if we hold multiple positions in it.
        by_market: dict[str, list[dict[str, Any]]] = {}
        for c in candidates:
            by_market.setdefault(str(c["market_id"]), []).append(c)
        stats.markets_queried = set(by_market.keys())

        sem = asyncio.Semaphore(self._cfg.max_concurrent_lookups)

        async def _fetch(cid: str) -> tuple[str, dict[str, Any] | None]:
            async with sem:
                try:
                    return cid, await self._resolver.fetch_resolution(cid)
                except Exception:
                    logger.exception(
                        "resolution_reconciler: fetch failed for %s", cid
                    )
                    stats.errors += 1
                    return cid, None

        results = await asyncio.gather(*(_fetch(cid) for cid in by_market))
        market_state: dict[str, dict[str, Any] | None] = dict(results)

        for cid, positions in by_market.items():
            market = market_state.get(cid)
            if market is None:
                # Resolver returned None — could be still-open or
                # genuinely missing. Distinguish by re-checking the
                # `closed` flag would require a second fetch; since
                # fetch_resolution already filters by closed=true,
                # None == "not closed yet OR Gamma missing".
                stats.market_pending += len(positions)
                continue
            if not market.get("closed"):
                stats.market_pending += len(positions)
                continue

            clob_token_ids = _coerce_list(market.get("clobTokenIds"))
            outcome_prices = _coerce_list(market.get("outcomePrices"))
            if (
                not clob_token_ids
                or len(clob_token_ids) != len(outcome_prices)
            ):
                logger.warning(
                    "resolution_reconciler: market %s closed but malformed "
                    "(clobTokenIds=%d, outcomePrices=%d) — leaving rows alone",
                    cid, len(clob_token_ids), len(outcome_prices),
                )
                stats.market_malformed += len(positions)
                continue

            for pos in positions:
                token_id = str(pos["token_id"])
                try:
                    idx = clob_token_ids.index(token_id)
                except ValueError:
                    logger.warning(
                        "resolution_reconciler: pid=%s token_id=%s not in "
                        "market %s outcomes=%s — skipping",
                        pos["position_id"], token_id, cid, clob_token_ids,
                    )
                    stats.token_not_in_market += 1
                    continue

                price_str = outcome_prices[idx]
                bucket = _classify_outcome_price(price_str)
                if bucket == "refund_or_invalid":
                    logger.info(
                        "resolution_reconciler: pid=%s market %s "
                        "non-binary resolution price %r — leaving as flat",
                        pos["position_id"], cid, price_str,
                    )
                    stats.refund_or_invalid += 1
                    continue

                outcome_value = 1.0 if bucket == "win" else 0.0
                new_pnl = _compute_realized_pnl_for_buy(
                    shares=float(pos["shares"]),
                    cost_basis_usd=float(pos["cost_basis_usd"]),
                    outcome_price=outcome_value,
                )
                ok = await self._positions.update_reconciled_pnl(
                    position_id=int(pos["position_id"]),
                    exit_price=outcome_value,
                    realized_pnl=new_pnl,
                    outcome="TIME_RECONCILED",
                )
                if not ok:
                    stats.update_no_op += 1
                    continue

                if bucket == "win":
                    stats.reconciled_win += 1
                else:
                    stats.reconciled_loss += 1
                # Old realized_pnl was 0 by definition (we filtered on
                # it); the correction magnitude is just |new_pnl|.
                stats.pnl_corrected_usd += new_pnl

        return stats


# ── Recurring loop (optional) ────────────────────────────────────────


async def run_loop(
    reconciler: ResolutionReconciler,
    interval_s: float,
    shutdown: asyncio.Event,
    on_stats: Callable[[ReconcileStats], Awaitable[None] | None] | None = None,
) -> None:
    """Driver loop — call `reconcile_once()` every `interval_s` until
    `shutdown` is set. Useful for embedding in the bot's main run-tasks
    list so reconciliation tracks resolution events live.

    `on_stats` is an optional callback invoked after each pass; useful
    for monitoring / metrics emission. May be sync or async.
    """
    while not shutdown.is_set():
        try:
            stats = await reconciler.reconcile_once()
        except Exception:
            logger.exception("resolution_reconciler: pass crashed")
        else:
            if on_stats is not None:
                result = on_stats(stats)
                if asyncio.iscoroutine(result):
                    await result
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_s)
            return
        except asyncio.TimeoutError:
            continue
