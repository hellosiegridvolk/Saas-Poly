"""Audit log of mode promotions.

Every transition between BotModes is recorded here so the boot path
can verify there's a fresh, signed promotion before lifting the
READ_ONLY safety lock.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS mode_promotions (
        promotion_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        from_mode      TEXT NOT NULL,
        to_mode        TEXT NOT NULL CHECK (to_mode IN
                       ('READ_ONLY','PAPER','LIVE_DRY','LIVE')),
        ts             INTEGER NOT NULL,
        signed_by      TEXT NOT NULL,
        fingerprint    TEXT,
        reason         TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_mode_promotions_ts ON mode_promotions(ts DESC)",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
