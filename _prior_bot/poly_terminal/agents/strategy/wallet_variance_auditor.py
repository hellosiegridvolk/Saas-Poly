"""Phase 23(c) — wallet variance auditor.

Computes a wallet's rolling avg PnL per dollar over the last N closed
buy→sell pairs, sourced from Polymarket's /activity API. Pushes the
result to any strategy's `set_wallet_avg_pnl(wallet, avg_pct)` setter.

Used at boot (and optionally periodically) to populate the variance
gate. Wallets whose recent rolling avg is below the strategy's
`wallet_avg_pnl_floor_pct` config are demoted (no copies fire).

Failure mode: any HTTP / parse / arithmetic error logs a warning and
SKIPS that wallet — the strategy's variance gate is fail-open by
design (missing data must never block the wallet signal).
"""

from __future__ import annotations

import asyncio
import logging
import statistics
from decimal import Decimal
from typing import Iterable, Protocol

import requests

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com/activity"
DEFAULT_LIMIT = 200       # records to pull per wallet
DEFAULT_LAST_N = 20       # closed pairs in the rolling window


class _AvgPnlSetter(Protocol):
    """Strategies that accept variance updates (CopyTrade + CopyScalp)."""

    def set_wallet_avg_pnl(self, wallet: str, avg_pct: Decimal) -> None: ...


def _fetch_activity(wallet: str) -> list[dict]:
    try:
        r = requests.get(
            DATA_API,
            params={"user": wallet.lower(), "limit": DEFAULT_LIMIT},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        return r.json()
    except Exception:
        logger.exception("variance_audit: /activity fetch failed for %s", wallet)
        return []


def _compute_avg_pnl_pct(activity: list[dict]) -> Decimal | None:
    """Pair BUYs with subsequent SELLs FIFO per asset, return mean
    return per dollar over last `DEFAULT_LAST_N` closed pairs.

    Returns None when fewer than 1 closed pair exists in the window.
    """
    buys: dict[str, list[dict]] = {}
    closed: list[float] = []
    # /activity returns newest first; iterate oldest-first for chronology.
    for a in reversed(activity):
        if a.get("type") != "TRADE":
            continue
        side = str(a.get("side", "")).upper()
        asset = str(a.get("asset", ""))
        if side == "BUY":
            buys.setdefault(asset, []).append(a)
        elif side == "SELL":
            stack = buys.get(asset, [])
            if stack:
                b = stack.pop(0)
                try:
                    bp = float(b.get("price", 0))
                    sp = float(a.get("price", 0))
                except (TypeError, ValueError):
                    continue
                if bp > 0:
                    closed.append((sp - bp) / bp)
    last_n = closed[-DEFAULT_LAST_N:]
    if not last_n:
        return None
    return Decimal(str(statistics.mean(last_n)))


async def audit_wallets_into(
    wallets: Iterable[str],
    targets: Iterable[_AvgPnlSetter],
) -> dict[str, Decimal]:
    """Audit every wallet in `wallets`, push the avg into every target
    strategy that has `set_wallet_avg_pnl`. Returns the {wallet: avg}
    dict for logging/observability.
    """
    out: dict[str, Decimal] = {}
    targets_list = list(targets)
    for w in wallets:
        if not w:
            continue
        wallet = w.strip().lower()
        try:
            acts = await asyncio.to_thread(_fetch_activity, wallet)
            avg = _compute_avg_pnl_pct(acts) if acts else None
        except Exception:
            logger.exception("variance_audit: %s failed", wallet)
            continue
        if avg is None:
            logger.info(
                "variance_audit: %s — no closed pairs in window, skipping",
                wallet,
            )
            continue
        out[wallet] = avg
        for t in targets_list:
            t.set_wallet_avg_pnl(wallet, avg)
        logger.info(
            "variance_audit: %s avg_pnl(last %d pairs) = %.2f%%",
            wallet, DEFAULT_LAST_N, float(avg) * 100,
        )
    return out
