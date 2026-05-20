"""Wallet Intelligence Agent — orchestrates ingestor + scorer + ranker.

Owns:
  - WalletFillIngestor (subscribes to EVT_WALLET_FILL on the bus)
  - Scorer (pure function, applied per wallet)
  - WalletRanker (emits EVT_WALLET_RANK_CHANGED when followed set changes)

Cadence:
  - `refresh_scores_and_rank()` is called periodically by the scheduler
    (default 5min) and after every leaderboard sync.

The agent is the only place that knows about all three pieces; consumers
(Strategy Agent, Exit Agent for whale-out) only see the bus events.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from poly_terminal.agents.wallet_intel.ingestor import WalletFillIngestor
from poly_terminal.agents.wallet_intel.ranker import RankerConfig, WalletRanker
from poly_terminal.agents.wallet_intel.scorer import (
    ScoreInputs,
    Scorer,
)
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.persistence.repositories.wallets import (
    WalletScore,
    WalletsRepo,
)

if TYPE_CHECKING:
    from poly_terminal.agents.wallet_intel.leaderboard_sync import (
        LeaderboardSync,
    )

logger = logging.getLogger(__name__)


class WalletIntelAgent:
    def __init__(
        self,
        bus: EventBus,
        repo: WalletsRepo,
        tracked_wallets: set[str],
        scorer: Scorer | None = None,
        ranker_cfg: RankerConfig | None = None,
        target_position_usd: float = 100.0,
        now_ts: int | None = None,
        leaderboard_sync: "LeaderboardSync | None" = None,
        stale_buy_max_age_seconds: int = 24 * 60 * 60,
    ) -> None:
        self._bus = bus
        self._repo = repo
        self._tracked = {w.lower() for w in tracked_wallets}
        self._scorer = scorer or Scorer()
        self._ranker = WalletRanker(cfg=ranker_cfg, bus=bus)
        self._target = target_position_usd
        self._fixed_now = now_ts
        self._leaderboard = leaderboard_sync
        self._ingestor = WalletFillIngestor(
            bus=bus, repo=repo, tracked=self._tracked
        )
        self._stale_buy_max_age_seconds = int(stale_buy_max_age_seconds)
        self._started = False

    @property
    def followed_wallets(self) -> set[str]:
        return self._ranker.followed

    def set_tracked(self, wallets: set[str]) -> None:
        self._tracked = {w.lower() for w in wallets}
        self._ingestor.set_tracked(self._tracked)

    async def start(self) -> None:
        if self._started:
            return
        await self._ingestor.start()
        self._started = True

    def _now(self) -> int:
        return (
            self._fixed_now
            if self._fixed_now is not None
            else int(datetime.now(timezone.utc).timestamp())
        )

    async def refresh_scores_and_rank(self) -> None:
        """Recompute every tracked wallet's score from history; rank; emit.

        Preserves the existing `category` so the periodic refresh doesn't
        wipe the seeder's classification (was a regression — refreshes
        defaulted to 'unknown', breaking the preferred-category filter).

        Also sweeps stale-open BUY rows to closed (presumed-loss) before
        scoring. Polymarket /activity emits no event for losing positions,
        so without this sweep, losers leak forever and the win_rate metric
        skews to ~100% — pushing wallets above the floor that shouldn't be.
        """
        now = self._now()
        try:
            await self._ingestor.sweep_stale_buys(
                max_age_seconds=self._stale_buy_max_age_seconds, now_ts=now
            )
        except Exception:
            logger.exception("wallet_intel: stale-buy sweep failed")
        cutoff = now - 60 * 86_400  # read 60d back; scorer windows it
        new_scores: list[WalletScore] = []
        for wallet in self._tracked:
            history = await self._repo.history_since(wallet, since_ts=cutoff)
            result = self._scorer.score(
                ScoreInputs(
                    history=history,
                    target_position_usd=self._target,
                    now_ts=now,
                )
            )
            existing = await self._repo.fetch_score(wallet)
            preserved_category = existing.category if existing else "unknown"
            score = WalletScore(
                wallet=wallet,
                win_rate=result.win_rate,
                avg_roi_pct=result.avg_roi_pct,
                trades_30d=result.trades_30d,
                median_position_usd=result.median_position_usd,
                conviction_score=result.conviction_score,
                category=preserved_category,
                last_updated=now,
                verified=True,
            )
            await self._repo.upsert_score(score)
            new_scores.append(score)
        await self._ranker.refresh(new_scores)

    async def sync_leaderboard(self) -> int:
        """Hourly leaderboard pull. No-op if no leaderboard source configured."""
        if self._leaderboard is None:
            return 0
        return await self._leaderboard.sync_once()
