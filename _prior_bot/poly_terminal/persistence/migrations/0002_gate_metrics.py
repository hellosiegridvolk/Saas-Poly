"""Per-gate counter table.

One row per (gate_name, day_bucket, outcome). `outcome='pass'` or any
reject code. Counter increments are upserts via `ON CONFLICT DO UPDATE`.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS gate_metrics (
        gate_name   TEXT NOT NULL,
        day_bucket  TEXT NOT NULL,           -- 'YYYY-MM-DD' UTC
        outcome     TEXT NOT NULL,           -- 'pass' or reject code
        count       INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (gate_name, day_bucket, outcome)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_gate_metrics_day ON gate_metrics(day_bucket)",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
