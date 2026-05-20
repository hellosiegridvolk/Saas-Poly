"""Add exit_evals table for per-evaluation exit observability.

2026-05-05 — Item #1 of post-canary checklist (deep research report 23).
Every time the exit pipeline evaluates a position (tick, poll, bar
watcher, wallet whale-out, profit_taker), we record a row capturing:

  - which position + price source were involved
  - what price was used and what it implied (pct_move, unrealized_usd)
  - what the decision engine returned (EXIT_TP / HOLD / etc.)
  - any block_reason that suppressed an exit (warmup, no_price, etc.)
  - free-form details_json (adverse_tick_count, max_hold_remaining_s,
    tp_pct/sl_pct snapshot, source-specific fields)

After a soak run we can answer questions like:
  - "for position X, how many ticks blocked by warmup before it could
    have evaluated TP?"
  - "did we ever lose the price source for this position?"
  - "what was the highest unrealized return we observed for token Y
    before it closed by TIME?"

The hot path is: one INSERT per tick per open position. With ~30 open
positions and ticks every few seconds this is bounded; SQLite WAL
absorbs it. Indexes are kept minimal — primarily (position_id,
eval_ts) for per-position drilldown, secondarily (eval_ts) for global
recent-window scans. No further indexes — analytical scans run
infrequently and are fine reading sequentially.

All DDL is `IF NOT EXISTS`; re-running the migration is a no-op.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS exit_evals (
        eval_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        position_id     INTEGER NOT NULL,
        token_id        TEXT NOT NULL,
        strategy        TEXT,
        eval_ts         INTEGER NOT NULL,
        tick_ts         INTEGER,
        price_source    TEXT NOT NULL,
        price_used      REAL NOT NULL,
        entry_price     REAL NOT NULL,
        pct_move        REAL NOT NULL,
        unrealized_usd  REAL NOT NULL,
        decision        TEXT NOT NULL,
        block_reason    TEXT,
        details_json    TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_exit_evals_position
        ON exit_evals (position_id, eval_ts)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_exit_evals_eval_ts
        ON exit_evals (eval_ts)
    """,
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
