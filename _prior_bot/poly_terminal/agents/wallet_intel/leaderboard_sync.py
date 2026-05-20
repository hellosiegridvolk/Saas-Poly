"""Hourly Data API leaderboard refresh → seed wallet_scores.

The Data API leaderboard exposes pnl + volume + trades aggregated over a
period. We use it as a seed signal — the actual scorer (which reads our
own wallet_history) is the source of truth once positions accumulate.
For wallets we haven't ingested yet, we map the Data API fields directly:

  win_rate         = unknown → 0.55 placeholder (just above WR floor so
                     wallets aren't filtered out before our ingestor sees
                     them; but below the rank gate's 0.60 so the ranker
                     won't follow a wallet on placeholder evidence alone)
  avg_roi_pct      = pnl / volume (proxy)
  trades_30d       = entry.trades
  conviction_score = sigmoid-like compression of (pnl, trades)
"""

from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING, Protocol

from poly_terminal.data.data_api.leaderboard import LeaderboardEntry
from poly_terminal.persistence.repositories.wallets import (
    WalletScore,
    WalletsRepo,
)

logger = logging.getLogger(__name__)


class _LeaderboardSource(Protocol):
    async def fetch_leaderboard(
        self, period: str = ..., order_by: str = ..., limit: int = ...
    ) -> list[LeaderboardEntry]: ...


def _seed_conviction(entry: LeaderboardEntry) -> float:
    """Compress (pnl, trades) into [0, 1.5] for initial seeding.

    log1p(pnl) gives diminishing returns on raw PnL; log1p(trades) rewards
    sample size. The formula intentionally outputs lower scores than the
    real scorer would — these are placeholders.
    """
    pnl_term = math.log1p(max(0.0, entry.pnl))      # ~10 for $20K winners
    trades_term = math.log1p(max(0, entry.trades))  # ~3 for 20 trades
    raw = (pnl_term * 0.05) + (trades_term * 0.10)
    return max(0.0, raw)


class LeaderboardSync:
    def __init__(
        self,
        api: _LeaderboardSource,
        repo: WalletsRepo,
        period: str = "30d",
        order_by: str = "pnl",
        limit: int = 100,
        now_ts: int | None = None,
    ) -> None:
        self._api = api
        self._repo = repo
        self._period = period
        self._order_by = order_by
        self._limit = limit
        self._fixed_now = now_ts

    def _now(self) -> int:
        return self._fixed_now if self._fixed_now is not None else int(time.time())

    async def sync_once(self) -> int:
        try:
            entries = await self._api.fetch_leaderboard(
                period=self._period,
                order_by=self._order_by,
                limit=self._limit,
            )
        except Exception:  # pragma: no cover — defensive
            logger.exception("leaderboard fetch failed")
            return 0
        now = self._now()
        written = 0
        for entry in entries:
            avg_roi = (entry.pnl / entry.volume) if entry.volume > 0 else 0.0
            score = WalletScore(
                wallet=entry.address.lower(),
                win_rate=0.55,                 # placeholder until ingestor fills it
                avg_roi_pct=float(avg_roi),
                trades_30d=int(entry.trades),
                median_position_usd=0.0,
                conviction_score=_seed_conviction(entry),
                last_updated=now,
            )
            await self._repo.upsert_score(score)
            written += 1
        return written
