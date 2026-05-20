"""Add research_orderbook_ticks for capturing Polymarket WS price-change deltas.

2026-05-05 — the standalone CLOB recorder previously only persisted full
`book` snapshots (event_type='book'). Polymarket's market WebSocket sends
those once per token at subscribe and on resync, but the bulk of orderbook
state changes flow as `price_change` deltas (event_type='price_change')
which the recorder dropped. After 12h of recording on ~220 active crypto
bar tokens, only the initial book snapshot per token was on disk —
useless for fill simulation against intraday signals.

This table captures every tick (price_change + last_trade_price) so the
backtest dataset can reconstruct the full price evolution between snapshots.

Schema:
  - token_id : Polymarket asset_id (string, NOT bigint)
  - ts       : exchange-provided timestamp in milliseconds
  - price    : decimal price the level changed to
  - size     : decimal size at that level after the change
  - side     : 'BUY' (bid side change) | 'SELL' (ask side change)
               | 'TRADE' (last_trade_price print)
  - source   : 'clob_ws' constant for now (room for future alternate sources)

Idempotent: every CREATE uses `IF NOT EXISTS`, so re-running the migration
is a no-op. Additive only — no existing tables are touched.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS research_orderbook_ticks (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id  TEXT NOT NULL,
        ts        INTEGER NOT NULL,
        price     REAL,
        size      REAL,
        side      TEXT,
        source    TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_research_ticks_token_ts
        ON research_orderbook_ticks (token_id, ts)
    """,
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
