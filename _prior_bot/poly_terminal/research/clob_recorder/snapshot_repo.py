"""Repository for persisting orderbook snapshots into research_orderbook_snapshots.

Offline research data only — never read or written by the live trading bot.
Schema lives in migration 0012; this repo is a thin adapter that:

  - serializes bids/asks to JSON,
  - computes spread when both sides are present,
  - exposes single + batched insert paths,
  - provides count + latest-ts diagnostics for resume / dedupe logic.

The repo takes a `Database` handle and opens short-lived connections per
operation, matching the pattern used elsewhere in the codebase.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from poly_terminal.persistence.db import Database


def _serialize_levels(levels: Iterable[dict[str, Any]] | None) -> str:
    """Encode a list of level dicts to compact JSON.

    Each level should have at least `price` and `size`. Non-dict iterables
    are coerced to a list before serialization. None is treated as empty.
    """
    if levels is None:
        return "[]"
    out: list[dict[str, float]] = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        try:
            price = float(level["price"])
            size = float(level["size"])
        except (KeyError, TypeError, ValueError):
            continue
        out.append({"price": price, "size": size})
    return json.dumps(out, separators=(",", ":"))


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compute_spread(best_bid: float | None, best_ask: float | None) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    return float(best_ask) - float(best_bid)


class SnapshotRepo:
    """Write-side adapter for the `research_orderbook_snapshots` table."""

    def __init__(self, db: Database) -> None:
        self._db = db

    async def insert(
        self,
        *,
        token_id: str,
        ts: int,
        best_bid: float | None,
        best_ask: float | None,
        bids: Iterable[dict[str, Any]] | None,
        asks: Iterable[dict[str, Any]] | None,
        source: str = "clob_ws",
    ) -> int:
        """Insert one snapshot. Returns the autoincrement rowid."""
        bb = _coerce_optional_float(best_bid)
        ba = _coerce_optional_float(best_ask)
        spread = _compute_spread(bb, ba)
        bids_json = _serialize_levels(bids)
        asks_json = _serialize_levels(asks)

        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO research_orderbook_snapshots
                  (token_id, ts, best_bid, best_ask, spread, bids_json, asks_json, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (str(token_id), int(ts), bb, ba, spread, bids_json, asks_json, source),
            )
            await conn.commit()
            rowid = cur.lastrowid
        return int(rowid) if rowid is not None else 0

    async def insert_many(self, rows: list[dict[str, Any]]) -> int:
        """Batch-insert snapshots. Returns the count actually written.

        Each row should contain the same keys accepted by `insert()`. Bad
        rows (missing token_id / ts) are skipped silently — defensive for
        the live recorder's hot path; the CLI surfaces a count mismatch
        via stats if you care.
        """
        if not rows:
            return 0

        prepared: list[tuple[Any, ...]] = []
        for r in rows:
            token_id = r.get("token_id")
            ts = r.get("ts")
            if token_id is None or ts is None:
                continue
            bb = _coerce_optional_float(r.get("best_bid"))
            ba = _coerce_optional_float(r.get("best_ask"))
            spread = _compute_spread(bb, ba)
            bids_json = _serialize_levels(r.get("bids"))
            asks_json = _serialize_levels(r.get("asks"))
            source = r.get("source", "clob_ws")
            prepared.append(
                (str(token_id), int(ts), bb, ba, spread, bids_json, asks_json, source)
            )

        if not prepared:
            return 0

        async with self._db.connect() as conn:
            await conn.executemany(
                """
                INSERT INTO research_orderbook_snapshots
                  (token_id, ts, best_bid, best_ask, spread, bids_json, asks_json, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                prepared,
            )
            await conn.commit()
        return len(prepared)

    async def count_for_token(self, token_id: str) -> int:
        """Count snapshots persisted for `token_id`. Diagnostic."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) FROM research_orderbook_snapshots WHERE token_id = ?",
                (str(token_id),),
            )
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def latest_ts_for_token(self, token_id: str) -> int | None:
        """Return the most recent ts persisted for `token_id`, or None."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT MAX(ts) FROM research_orderbook_snapshots WHERE token_id = ?",
                (str(token_id),),
            )
            row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])
