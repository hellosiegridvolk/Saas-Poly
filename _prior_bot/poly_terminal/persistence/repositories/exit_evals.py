"""Repository for `exit_evals` — per-evaluation exit observability.

Every tick / poll / bar-watcher / whale-out / profit_taker decision the
exit pipeline makes lands one row here. The row carries enough context
to answer post-incident questions like:

  - "for position X, did warmup ever block a sell that would otherwise
    have fired EXIT_TP?"
  - "what fraction of exits this hour were bar_watcher fallbacks vs
    real tick-driven TP?"
  - "what was the highest unrealized return we observed for token Y
    before it eventually closed by TIME?"

Hot path: one INSERT per tick per open position. We hold the conn for
the duration of the insert; SQLite WAL + the global write-lock are
fine for the volumes we see (≤ 100 inserts/sec realistic).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from poly_terminal.persistence.db import Database


# Canonical price_source values — referenced by callers. Strings rather
# than an Enum to keep DB rows portable / debuggable.
SOURCE_MARKET_WS: str = "market_ws"           # WS price_change / last_trade_price
SOURCE_TICK_POLLER: str = "tick_poller"       # REST /price fallback
SOURCE_BAR_WATCHER: str = "bar_watcher"       # bar_end_iso passed
SOURCE_WHALE_OUT: str = "whale_out"           # tracked-wallet SELL signal
SOURCE_PROFIT_TAKER: str = "profit_taker"     # ProfitTakerAgent tick path


class ExitEvalsRepo:
    """Async writer + read helpers for `exit_evals`. The hot path
    (`record`) is fire-and-forget at call sites that don't want to await
    a DB round-trip — schedule via `asyncio.create_task` if needed."""

    def __init__(self, db: "Database") -> None:
        self._db = db

    async def record(
        self,
        *,
        position_id: int,
        token_id: str,
        strategy: str,
        eval_ts: int,
        tick_ts: int | None,
        price_source: str,
        price_used: float,
        entry_price: float,
        pct_move: float,
        unrealized_usd: float,
        decision: str,
        block_reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Insert one row. Caller MUST pass canonical price_source string
        (see SOURCE_* constants). decision is the string form of the
        ExitDecision enum (e.g. 'EXIT_TP', 'HOLD', 'EXIT_TIME')."""
        details_json = json.dumps(details) if details else None
        async with self._db.connect() as conn:
            await conn.execute(
                """
                INSERT INTO exit_evals (
                    position_id, token_id, strategy,
                    eval_ts, tick_ts, price_source,
                    price_used, entry_price, pct_move, unrealized_usd,
                    decision, block_reason, details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position_id,
                    token_id,
                    strategy or "",
                    eval_ts,
                    tick_ts,
                    price_source,
                    float(price_used),
                    float(entry_price),
                    float(pct_move),
                    float(unrealized_usd),
                    decision,
                    block_reason,
                    details_json,
                ),
            )
            await conn.commit()

    async def fetch_for_position(
        self, position_id: int, *, limit: int = 200
    ) -> list[dict[str, Any]]:
        """Most-recent-first evals for a single position. Useful for
        post-incident drilldown: 'why didn't position 21178 sell?'"""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT eval_id, position_id, token_id, strategy,
                  eval_ts, tick_ts, price_source, price_used,
                  entry_price, pct_move, unrealized_usd,
                  decision, block_reason, details_json
                FROM exit_evals
                WHERE position_id = ?
                ORDER BY eval_id DESC
                LIMIT ?
                """,
                (position_id, limit),
            )
            rows = await cur.fetchall()
        return [_row_to_dict(r) for r in rows]

    async def recent(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Most-recent-first across all positions. Used by the monitor
        route + ad-hoc shells."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT eval_id, position_id, token_id, strategy,
                  eval_ts, tick_ts, price_source, price_used,
                  entry_price, pct_move, unrealized_usd,
                  decision, block_reason, details_json
                FROM exit_evals
                ORDER BY eval_id DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cur.fetchall()
        return [_row_to_dict(r) for r in rows]

    async def count_by_decision_since(self, since_ts: int) -> dict[str, int]:
        """Aggregate decision counts since `since_ts`. Quick health
        check: 'in the last hour, how many evals returned HOLD vs each
        EXIT_* variant?'"""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT decision, COUNT(*) AS cnt
                FROM exit_evals
                WHERE eval_ts >= ?
                GROUP BY decision
                """,
                (int(since_ts),),
            )
            rows = await cur.fetchall()
        return {str(r[0]): int(r[1]) for r in rows}

    async def count_by_block_reason_since(
        self, since_ts: int
    ) -> dict[str, int]:
        """Aggregate non-null block_reason counts since `since_ts`. Used
        to spot degraded-feed conditions: 'how many evals were blocked
        by no_price in the last hour?'"""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT block_reason, COUNT(*) AS cnt
                FROM exit_evals
                WHERE eval_ts >= ? AND block_reason IS NOT NULL
                GROUP BY block_reason
                """,
                (int(since_ts),),
            )
            rows = await cur.fetchall()
        return {str(r[0]): int(r[1]) for r in rows}


def _row_to_dict(r: tuple[Any, ...]) -> dict[str, Any]:
    details: dict[str, Any] | None = None
    if r[13]:
        try:
            details = json.loads(r[13])
        except (TypeError, ValueError):
            details = None
    return {
        "eval_id": int(r[0]),
        "position_id": int(r[1]),
        "token_id": str(r[2]),
        "strategy": str(r[3]) if r[3] else "",
        "eval_ts": int(r[4]),
        "tick_ts": int(r[5]) if r[5] is not None else None,
        "price_source": str(r[6]),
        "price_used": float(r[7]),
        "entry_price": float(r[8]),
        "pct_move": float(r[9]),
        "unrealized_usd": float(r[10]),
        "decision": str(r[11]),
        "block_reason": str(r[12]) if r[12] else None,
        "details": details,
    }
