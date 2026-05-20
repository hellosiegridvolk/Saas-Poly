# src/poly_terminal/persistence/migrations/0018_wallet_history_ms_timestamp_repair.py
"""One-time repair: a few wallet_history rows hold millisecond epochs
in opened_at/closed_at (ingestor stored raw payload ts before the
ms->s guard, ~12/1.18M rows). Divide clearly-ms values (>= 1e12) by
1000. Idempotent: post-repair values are < 1e12 so a re-run matches
nothing. SQLite integer column / 1000 = integer division.
"""
from __future__ import annotations

import aiosqlite

_REPAIR: list[str] = [
    "UPDATE wallet_history SET opened_at = opened_at / 1000 "
    "WHERE opened_at >= 1000000000000",
    "UPDATE wallet_history SET closed_at = closed_at / 1000 "
    "WHERE closed_at IS NOT NULL AND closed_at >= 1000000000000",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _REPAIR:
        await conn.execute(stmt)
