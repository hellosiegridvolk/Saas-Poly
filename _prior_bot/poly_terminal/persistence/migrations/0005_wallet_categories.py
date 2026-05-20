"""Wallet category column.

Each wallet is tagged with a category (e.g. 'crypto', 'sports', 'politics',
'unknown') so the Strategy Agent can filter on operator-preferred categories.
The default 'crypto' aligns with v3's primary focus on Up/Down binaries.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    "ALTER TABLE wallet_scores ADD COLUMN category TEXT NOT NULL DEFAULT 'unknown'",
    "CREATE INDEX IF NOT EXISTS idx_wallet_scores_category ON wallet_scores(category)",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
