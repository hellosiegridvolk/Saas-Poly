"""Repository for persisting WS price-change ticks into research_orderbook_ticks.

Companion to `SnapshotRepo`. Same lifecycle (offline-only research data),
same Database access pattern, same single + batched insert surface.

Schema lives in migration 0013. Ticks complement snapshots: a snapshot
gives you the full ladder at a point in time; a tick is one delta on a
single side. Together they reconstruct intraday orderbook evolution
without needing minute-by-minute snapshots.
"""

from __future__ import annotations

from typing import Any

from poly_terminal.persistence.db import Database


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        s = str(value)
    except Exception:
        return None
    return s if s else None


class TickRepo:
    """Write-side adapter for the `research_orderbook_ticks` table."""

    def __init__(self, db: Database) -> None:
        self._db = db

    async def insert(
        self,
        *,
        token_id: str,
        ts: int,
        price: float | None,
        size: float | None,
        side: str | None,
        source: str = "clob_ws",
    ) -> int:
        """Insert one tick. Returns the autoincrement rowid."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO research_orderbook_ticks
                  (token_id, ts, price, size, side, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(token_id),
                    int(ts),
                    _coerce_optional_float(price),
                    _coerce_optional_float(size),
                    _coerce_optional_str(side),
                    str(source),
                ),
            )
            await conn.commit()
            rowid = cur.lastrowid
        return int(rowid) if rowid is not None else 0

    async def insert_many(self, rows: list[dict[str, Any]]) -> int:
        """Batch-insert ticks. Returns the count actually written.

        Each row should contain the same keys accepted by `insert()`. Bad
        rows (missing token_id / ts) are skipped silently — defensive for
        the live recorder's hot path, mirrors SnapshotRepo's contract.
        """
        if not rows:
            return 0

        prepared: list[tuple[Any, ...]] = []
        for r in rows:
            token_id = r.get("token_id")
            ts = r.get("ts")
            if token_id is None or ts is None:
                continue
            try:
                ts_int = int(ts)
            except (TypeError, ValueError):
                continue
            prepared.append(
                (
                    str(token_id),
                    ts_int,
                    _coerce_optional_float(r.get("price")),
                    _coerce_optional_float(r.get("size")),
                    _coerce_optional_str(r.get("side")),
                    str(r.get("source", "clob_ws")),
                )
            )

        if not prepared:
            return 0

        async with self._db.connect() as conn:
            await conn.executemany(
                """
                INSERT INTO research_orderbook_ticks
                  (token_id, ts, price, size, side, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                prepared,
            )
            await conn.commit()
        return len(prepared)

    async def count_for_token(self, token_id: str) -> int:
        """Count ticks persisted for `token_id`. Diagnostic."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) FROM research_orderbook_ticks WHERE token_id = ?",
                (str(token_id),),
            )
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def latest_ts_for_token(self, token_id: str) -> int | None:
        """Return the most recent ts persisted for `token_id`, or None."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT MAX(ts) FROM research_orderbook_ticks WHERE token_id = ?",
                (str(token_id),),
            )
            row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])
