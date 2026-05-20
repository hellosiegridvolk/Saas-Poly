# src/poly_terminal/persistence/migrations/0016_positions_lane_attribution.py
"""Add per-lane attribution to positions + backfill from paper_fills.

positions had no `strategy` column; per-strategy P&L was only
recoverable via a fragile entry_intent_id -> paper_fills.strategy join.
The bake-off harness governs virtual capital per lane, so attribution
must be durable. New positions write both columns at open; this
migration backfills history via the verified join and parks unmatched
legacy rows in a '(legacy)' bucket excluded from lane scoring
(lane_id NULL for those).
"""
from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    "ALTER TABLE positions ADD COLUMN strategy TEXT",
    "ALTER TABLE positions ADD COLUMN lane_id TEXT",
]

_BACKFILL: list[str] = [
    """
    UPDATE positions
       SET strategy = (
         SELECT pf.strategy FROM paper_fills pf
          WHERE pf.intent_id = positions.entry_intent_id
          ORDER BY pf.fill_id LIMIT 1)
     WHERE strategy IS NULL
    """,
    "UPDATE positions SET lane_id = strategy"
    " WHERE lane_id IS NULL AND strategy IS NOT NULL",
    "UPDATE positions SET strategy = '(legacy)' WHERE strategy IS NULL",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
    for stmt in _BACKFILL:
        await conn.execute(stmt)
