"""Composer that builds a backtest dataset by joining markets and trades.

Pulls closed markets via FalconMarketLoader, then iterates fetching trades
per-market via FalconTradesLoader. Returns a summary including a generated
`run_id` (uuid4 hex) so downstream replay/analytics can scope their reads.
"""

from __future__ import annotations

import uuid
from typing import Any


class BacktestDatasetBuilder:
    """Build a {markets, trades} dataset for a backtest run."""

    def __init__(self, db, market_loader, trades_loader) -> None:
        self._db = db
        self._market_loader = market_loader
        self._trades_loader = trades_loader

    async def build(
        self, min_volume: float = 100, max_markets: int = 50
    ) -> dict[str, Any]:
        """Pull closed markets, fetch trades for each, persist to research tables.

        Returns:
            {
                "markets_loaded": int,
                "trades_loaded": int,
                "run_id": str,   # uuid4 hex, for downstream scoping
            }
        """
        run_id = uuid.uuid4().hex
        markets = await self._market_loader.fetch_closed_markets(
            min_volume=min_volume, limit=max_markets
        )
        markets_loaded = await self._market_loader.upsert_to_db(self._db, markets)

        trades_loaded = 0
        for m in markets:
            slug = m.get("market_slug") or m.get("slug")
            if not slug:
                continue
            condition_id = m.get("condition_id")
            try:
                trades = await self._trades_loader.fetch_trades_for_market(slug)
            except Exception:
                # Skip per-market errors — partial dataset is better than none.
                continue
            if not trades:
                continue
            trades_loaded += await self._trades_loader.upsert_to_db(
                self._db, trades, slug, condition_id
            )

        return {
            "markets_loaded": markets_loaded,
            "trades_loaded": trades_loaded,
            "run_id": run_id,
        }
