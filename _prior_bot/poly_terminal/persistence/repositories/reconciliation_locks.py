"""Phase 31 — reconciliation_locks repository.

2026-05-09 — quarantine on-chain shares while a SELL_FAILED position
is waiting for the redeemer to resolve. v50-v52 saw three
phantom-double chains (22492→22493, 22494→22495, 22496→22497) where
a failed SELL left shares on chain and the position_importer
treated them as a new position to import. Cumulative cost basis
double-counting was ~$6.66 of the $10.67 v50-v55 loss.

This repo:
  - Creates a lock when a SELL fails terminally (patient timeout +
    FAK exhausted, or sign-fail with on-chain BUY confirmed).
  - Returns active locks for a token_id so the importer can skip.
  - Clears locks when the redeemer marks the underlying position
    redeemed (worthless or otherwise) — releasing the quarantine.
  - Auto-expires locks via `expires_at` so a stuck lock-holder
    process can't quarantine a token forever.

Locks are persisted (SQLite, not in-memory) so they survive process
restart. v52 specifically restarted between 22496's SELL_FAILED and
22497's import; an in-memory lock would not have caught it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from poly_terminal.persistence.db import Database


# Default lock TTL (15 min). Long enough for the redeemer to make
# progress on a typical worthless-redeem path; short enough that a
# stuck lock can't quarantine a token forever.
DEFAULT_LOCK_TTL_S = 900


@dataclass(frozen=True)
class ReconciliationLock:
    lock_id: int
    token_id: str
    position_id: int
    reason: str
    created_at: int
    expires_at: int
    cleared_at: int | None


class ReconciliationLockRepo:
    """CRUD on reconciliation_locks. Async because it writes to SQLite
    via aiosqlite under WAL."""

    def __init__(self, db: "Database") -> None:
        self._db = db

    async def upsert(
        self,
        *,
        token_id: str,
        position_id: int,
        reason: str,
        created_at: int,
        ttl_s: int = DEFAULT_LOCK_TTL_S,
    ) -> int:
        """Create a new active lock (or refresh an existing active
        lock on the same token+position by extending its expires_at).

        Returns the lock_id. Uses an idempotent insert pattern: if an
        ACTIVE (cleared_at IS NULL) lock already exists for this
        (token_id, position_id) pair, extend its expires_at instead
        of creating a duplicate.
        """
        expires_at = created_at + ttl_s
        async with self._db.connect() as conn:
            # Check for an existing active lock on the same key.
            cur = await conn.execute(
                """
                SELECT lock_id FROM reconciliation_locks
                WHERE token_id = ? AND position_id = ?
                  AND cleared_at IS NULL
                ORDER BY created_at DESC LIMIT 1
                """,
                (token_id, position_id),
            )
            existing = await cur.fetchone()
            if existing is not None:
                lock_id = int(existing[0])
                # Extend the lock — keep the original reason/created_at.
                await conn.execute(
                    """
                    UPDATE reconciliation_locks
                    SET expires_at = ?
                    WHERE lock_id = ?
                    """,
                    (expires_at, lock_id),
                )
                await conn.commit()
                return lock_id
            # Otherwise insert a new lock.
            cur = await conn.execute(
                """
                INSERT INTO reconciliation_locks
                  (token_id, position_id, reason, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (token_id, position_id, reason, created_at, expires_at),
            )
            await conn.commit()
            return int(cur.lastrowid or 0)

    async def get_active(
        self, token_id: str, now_ts: int
    ) -> ReconciliationLock | None:
        """Return the most recent active lock for `token_id`, or None.

        "Active" = cleared_at IS NULL AND expires_at > now. Any lock
        that has been explicitly cleared OR has aged past its TTL is
        ignored — the importer falls back to the legacy behavior in
        either case.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT lock_id, token_id, position_id, reason,
                       created_at, expires_at, cleared_at
                FROM reconciliation_locks
                WHERE token_id = ?
                  AND cleared_at IS NULL
                  AND expires_at > ?
                ORDER BY created_at DESC LIMIT 1
                """,
                (token_id, now_ts),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return ReconciliationLock(
            lock_id=int(row[0]),
            token_id=str(row[1]),
            position_id=int(row[2]),
            reason=str(row[3]),
            created_at=int(row[4]),
            expires_at=int(row[5]),
            cleared_at=int(row[6]) if row[6] is not None else None,
        )

    async def clear(
        self, *, position_id: int, cleared_at: int
    ) -> int:
        """Mark all active locks for `position_id` as cleared.

        Called by the redeemer after a successful redeem (worthless
        or otherwise) — the underlying chain inventory is resolved,
        importer can stop quarantining the token. Returns the number
        of locks cleared.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE reconciliation_locks
                SET cleared_at = ?
                WHERE position_id = ? AND cleared_at IS NULL
                """,
                (cleared_at, position_id),
            )
            await conn.commit()
        return cur.rowcount or 0

    async def fetch_by_id(self, lock_id: int) -> ReconciliationLock | None:
        """Test-helper: fetch any lock (active OR cleared) by id."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT lock_id, token_id, position_id, reason,
                       created_at, expires_at, cleared_at
                FROM reconciliation_locks
                WHERE lock_id = ?
                """,
                (lock_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return ReconciliationLock(
            lock_id=int(row[0]),
            token_id=str(row[1]),
            position_id=int(row[2]),
            reason=str(row[3]),
            created_at=int(row[4]),
            expires_at=int(row[5]),
            cleared_at=int(row[6]) if row[6] is not None else None,
        )
