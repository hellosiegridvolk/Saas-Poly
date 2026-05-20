"""Phase 31 — reconciliation_locks table.

2026-05-09 — when a position's SELL fails (patient timeout, FAK
escalation exhausted, or any path that leaves on-chain shares
unsold), we set a quarantine lock on (token_id, position_id) so the
position_importer skips re-importing those shares as a "new"
position. v50r2 pos 22492→22493, v51 pos 22494→22495, v52 pos
22496→22497 all caught this pattern: one canary BUY filled, the
SELL failed, the importer found the leftover on-chain shares and
imported them as a fresh position, double-counting the loss in the
DB until the redeemer's WORTHLESS_NO_TX truth-up corrected it.

Locks are persisted to SQLite (not just in-memory) so they survive
process restart. v52 specifically restarted between 22496's
SELL_FAILED and 22497's import — in-memory state would not have
caught it.

Schema:
  - token_id is the lock key (one lock per token at a time)
  - position_id is the originating position that triggered the lock
  - reason is a free-form string for forensics
  - created_at + expires_at bound the lock's lifetime
  - cleared_at is set when the redeemer/operator resolves the
    underlying position (NULL while active)

Default lock TTL is 15 min (900s); after expiry the importer falls
back to the legacy behavior. This is a safety valve in case the
lock-holder process dies before clearing the lock and a stuck
on-chain position can't be reconciled forever.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS reconciliation_locks (
        lock_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id      TEXT NOT NULL,
        position_id   INTEGER NOT NULL,
        reason        TEXT NOT NULL,
        created_at    INTEGER NOT NULL,
        expires_at    INTEGER NOT NULL,
        cleared_at    INTEGER
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_recon_locks_token_active
        ON reconciliation_locks (token_id, cleared_at, expires_at)
    """,
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
