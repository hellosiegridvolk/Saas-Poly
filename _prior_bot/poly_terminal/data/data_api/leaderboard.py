"""Leaderboard cache + typed entry."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from poly_terminal.data.data_api.client import DataApiClient


@dataclass(frozen=True)
class LeaderboardEntry:
    address: str            # lowercase
    pnl: float              # USD (signed)
    volume: float           # USD
    trades: int             # count over the period
    raw: dict[str, Any] = field(default_factory=dict)


class LeaderboardCache:
    """Caches leaderboard responses for `ttl_s` seconds.

    Keyed by (period, order_by, limit). The Wallet Intelligence Agent's
    leaderboard_sync calls this on its hourly schedule.
    """

    def __init__(self, client: "DataApiClient", ttl_s: float = 3600.0) -> None:
        self._client = client
        self._ttl_s = ttl_s
        self._cache: dict[
            tuple[str, str, int],
            tuple[list[LeaderboardEntry], float],
        ] = {}

    async def get(
        self,
        period: str = "30d",
        order_by: str = "pnl",
        limit: int = 100,
        force: bool = False,
    ) -> list[LeaderboardEntry]:
        key = (period, order_by, limit)
        now = time.monotonic()
        if not force:
            cached = self._cache.get(key)
            if cached is not None and (now - cached[1]) < self._ttl_s:
                return list(cached[0])
        out = await self._client.fetch_leaderboard(  # type: ignore[arg-type]
            period=period, order_by=order_by, limit=limit
        )
        self._cache[key] = (list(out), now)
        return list(out)
