# src/poly_terminal/persistence/migrations/0017_wallet_scores_verified.py
"""Add `verified` to wallet_scores.

0 = leaderboard placeholder seed; 1 = scorer-derived from real
wallet_history. The ranker floor path requires verified=1 so a
placeholder can't be followed regardless of the configured win-rate
floor (the boot fetch_top->ranker path otherwise ranks raw seeded
rows). Additive, NOT NULL DEFAULT 0 — existing rows become unverified
until the next ~5-min scorer cycle re-verifies them.
"""
from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    "ALTER TABLE wallet_scores ADD COLUMN verified INTEGER NOT NULL DEFAULT 0",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
