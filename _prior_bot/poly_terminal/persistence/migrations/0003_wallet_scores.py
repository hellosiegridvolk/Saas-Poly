"""Wallet scoring tables.

`wallet_scores` — current rolling-window snapshot per wallet.
`wallet_history` — append-only log of closed positions; the scorer reads from
                   here to compute win_rate, avg_roi, trades_30d, etc.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS wallet_scores (
        wallet              TEXT PRIMARY KEY,        -- lowercase hex
        win_rate            REAL DEFAULT 0,
        avg_roi_pct         REAL DEFAULT 0,
        trades_30d          INTEGER DEFAULT 0,
        median_position_usd REAL DEFAULT 0,
        conviction_score    REAL DEFAULT 0,
        last_updated        INTEGER NOT NULL          -- unix seconds
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS wallet_history (
        history_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet        TEXT NOT NULL,                  -- lowercase hex
        market_id     TEXT NOT NULL,
        token_id      TEXT NOT NULL,
        side          TEXT NOT NULL,
        size_usd      REAL NOT NULL,
        avg_price     REAL NOT NULL,
        exit_price    REAL,
        pnl_usd       REAL,
        opened_at     INTEGER NOT NULL,
        closed_at     INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_wallet_history_wallet ON wallet_history(wallet, closed_at)",
    "CREATE INDEX IF NOT EXISTS idx_wallet_scores_score ON wallet_scores(conviction_score DESC)",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
