"""Add shares_remaining to positions for partial-close support.

2026-05-03 P1 #2 fix (deep-research-report 14/15/16/17 §partial-close).

Background: the existing schema modeled position close as full-only
(`close_position` sets closed_ts + exit_price + realized_pnl from the
full `shares` count). When an operator manually closes only PART of
a stacked position via the Polymarket UI, the User WS unmatched-SELL
event arrives with a `filled_size` smaller than the position's
`shares`. The current LiveFillReconciler closes the entire oldest
matching position, which over-credits realized PnL (we're crediting
20 shares of pnl when we sold 10).

This migration:
- Adds `shares_remaining REAL` (nullable for legacy rows; the
  PositionsRepo treats NULL as "still has full `shares` open").
- Backfills `shares_remaining = shares` for currently-open positions
  so existing flows stay correct.

The matching code change in PositionsRepo introduces:
- `reduce_open_position(...)` — partial close that decrements
  shares_remaining and computes realized PnL on just the closed
  fraction (does NOT mark closed_ts).
- `close_position(...)` updated to set shares_remaining = 0.

This migration is reversible at the data level (rolling back means
ignoring shares_remaining), but the column itself stays.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    "ALTER TABLE positions ADD COLUMN shares_remaining REAL",
    # Backfill: every currently-OPEN position starts with full shares.
    # Closed positions get 0 (they have no remaining inventory).
    "UPDATE positions SET shares_remaining = shares "
    "WHERE closed_ts IS NULL AND shares_remaining IS NULL",
    "UPDATE positions SET shares_remaining = 0 "
    "WHERE closed_ts IS NOT NULL AND shares_remaining IS NULL",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
