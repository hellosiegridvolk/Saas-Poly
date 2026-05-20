"""Persist end_date_iso on positions for restart-recovery of bar watcher.

Without this column, ExitAgent's `_bar_end_ts` map (in-memory only) is
lost on restart, and short-window positions opened in a prior process
sit open forever — blocking MAX_OPEN_POSITIONS as old paper runs leak.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    "ALTER TABLE positions ADD COLUMN end_date_iso TEXT",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
