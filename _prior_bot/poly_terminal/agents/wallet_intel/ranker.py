"""Top-decile selection + EVT_WALLET_RANK_CHANGED emission.

Pure-logic `select(scores) -> set[wallet]` plus a thin `refresh()` async
wrapper that compares against the previous followed set and only emits a
bus event when membership changes.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_WALLET_RANK_CHANGED
from poly_terminal.persistence.repositories.wallets import WalletScore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RankerConfig:
    top_pct: float = 0.10        # top decile by default
    wr_floor: float = 0.60       # min win_rate to qualify
    trades_floor: int = 10       # min trades_30d to qualify
    # Explicit wallet allowlist. When non-empty, `select()` ignores
    # the wr_floor / trades_floor / top_pct rules and follows ONLY
    # the wallets listed here (intersected with what's actually in
    # the scores list, so we never follow a wallet we have no data
    # on). Wallets are matched case-insensitively (lowercased).
    # Use for canary / single-wallet test runs.
    wallet_followed_override: frozenset[str] | None = None


class WalletRanker:
    def __init__(
        self,
        cfg: RankerConfig | None = None,
        bus: EventBus | None = None,
    ) -> None:
        self._cfg = cfg or RankerConfig()
        self._bus = bus
        self._followed: set[str] = set()

    @property
    def followed(self) -> set[str]:
        return set(self._followed)

    def select(self, scores: list[WalletScore]) -> set[str]:
        """Apply WR + trades floor, then take top `top_pct` by conviction.

        If `wallet_followed_override` is set, that allowlist replaces
        the entire ranking pipeline — useful for canary / single-wallet
        runs where you want surgical control of who gets followed.
        """
        if self._cfg.wallet_followed_override:
            override = self._cfg.wallet_followed_override
            present = {s.wallet.lower() for s in scores}
            return {w for w in override if w in present}
        eligible = [
            s for s in scores
            if s.win_rate >= self._cfg.wr_floor
            and s.trades_30d >= self._cfg.trades_floor
            and s.verified
        ]
        if not eligible:
            return set()
        eligible.sort(key=lambda s: s.conviction_score, reverse=True)
        n = max(1, math.ceil(len(eligible) * self._cfg.top_pct))
        return {s.wallet.lower() for s in eligible[:n]}

    async def refresh(self, scores: list[WalletScore]) -> set[str]:
        """Recompute followed set; emit on change. Returns the new set."""
        new = self.select(scores)
        if new == self._followed:
            return new
        added = new - self._followed
        removed = self._followed - new
        self._followed = new
        if self._bus is not None:
            await self._bus.publish(
                EVT_WALLET_RANK_CHANGED,
                {
                    "followed": set(new),
                    "added": added,
                    "removed": removed,
                    "size": len(new),
                },
            )
        else:
            logger.info(
                "rank changed: %d followed (+%d -%d)",
                len(new),
                len(added),
                len(removed),
            )
        return new
