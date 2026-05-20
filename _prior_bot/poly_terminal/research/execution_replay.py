"""Replays strategy signals against historical L2 books to produce backtest fills.

For each signal, this module:
  1. Looks up the closest `research_orderbook_snapshots` row by (token_id, ts)
     within ±book_age_max_s of the signal_ts.
  2. Parses the asks_json ladder.
  3. Calls fill_simulator.simulate_fak_buy with worst_price = signal_price *
     (1 + max_slippage_pct).
  4. Inserts an outcome row into `backtest_runs`.

Stores fill_status in {'filled', 'partial', 'no_book', 'rejected'}.
"""

from __future__ import annotations

import json
from typing import Any

from poly_terminal.research.fill_simulator import simulate_fak_buy


class ExecutionReplay:
    """Replay a list of signals against persisted historical books."""

    def __init__(self, db) -> None:
        self._db = db

    async def replay_strategy_signals(
        self,
        run_id: str,
        strategy: str,
        signals: list[dict[str, Any]],
        max_slippage_pct: float = 0.05,
        book_age_max_s: int = 5,
    ) -> dict[str, Any]:
        """Replay signals and persist fill outcomes.

        Each signal is a dict with at minimum:
            intent_id (str), token_id (str), signal_ts (int),
            signal_price (float), target_size_usd (float)

        Returns a summary dict with per-status counts.
        """
        counts = {"filled": 0, "partial": 0, "no_book": 0, "rejected": 0}
        if not signals:
            return {"run_id": run_id, "strategy": strategy, **counts, "total": 0}

        async with self._db.connect() as conn:
            for sig in signals:
                intent_id = sig["intent_id"]
                token_id = sig["token_id"]
                signal_ts = int(sig["signal_ts"])
                signal_price = float(sig["signal_price"])
                target_usd = float(sig["target_size_usd"])

                book_row = await self._closest_book(conn, token_id, signal_ts, book_age_max_s)
                if book_row is None:
                    await self._insert_run(
                        conn,
                        run_id=run_id,
                        intent_id=intent_id,
                        strategy=strategy,
                        token_id=token_id,
                        signal_ts=signal_ts,
                        signal_price=signal_price,
                        target_usd=target_usd,
                        fill_status="no_book",
                        filled_shares=0.0,
                        avg_fill_price=None,
                        book_age_s=None,
                    )
                    counts["no_book"] += 1
                    continue

                book_ts, asks_json = book_row
                book_age_s = abs(int(book_ts) - signal_ts)
                try:
                    asks = json.loads(asks_json) if asks_json else []
                except (TypeError, ValueError):
                    asks = []

                worst_price = signal_price * (1.0 + max_slippage_pct)
                result = simulate_fak_buy(asks, target_usd, worst_price)

                if result.reject_reason is not None or result.filled_shares <= 0:
                    fill_status = "rejected"
                    counts["rejected"] += 1
                elif result.partial:
                    fill_status = "partial"
                    counts["partial"] += 1
                else:
                    fill_status = "filled"
                    counts["filled"] += 1

                await self._insert_run(
                    conn,
                    run_id=run_id,
                    intent_id=intent_id,
                    strategy=strategy,
                    token_id=token_id,
                    signal_ts=signal_ts,
                    signal_price=signal_price,
                    target_usd=target_usd,
                    fill_status=fill_status,
                    filled_shares=result.filled_shares,
                    avg_fill_price=result.avg_price,
                    book_age_s=book_age_s,
                )
            await conn.commit()

        return {
            "run_id": run_id,
            "strategy": strategy,
            "total": len(signals),
            **counts,
        }

    @staticmethod
    async def _closest_book(
        conn, token_id: str, signal_ts: int, book_age_max_s: int
    ) -> tuple[int, str] | None:
        """Find the snapshot with the smallest |ts - signal_ts| within window."""
        cur = await conn.execute(
            """
            SELECT ts, asks_json
              FROM research_orderbook_snapshots
             WHERE token_id = ?
               AND ts BETWEEN ? AND ?
             ORDER BY ABS(ts - ?) ASC
             LIMIT 1
            """,
            (
                token_id,
                signal_ts - book_age_max_s,
                signal_ts + book_age_max_s,
                signal_ts,
            ),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0]), row[1]

    @staticmethod
    async def _insert_run(
        conn,
        *,
        run_id: str,
        intent_id: str,
        strategy: str,
        token_id: str,
        signal_ts: int,
        signal_price: float,
        target_usd: float,
        fill_status: str,
        filled_shares: float,
        avg_fill_price: float | None,
        book_age_s: int | None,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO backtest_runs
              (run_id, intent_id, strategy, token_id, signal_ts, signal_price,
               target_usd, fill_status, filled_shares, avg_fill_price, book_age_s)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, intent_id) DO UPDATE SET
                strategy = excluded.strategy,
                token_id = excluded.token_id,
                signal_ts = excluded.signal_ts,
                signal_price = excluded.signal_price,
                target_usd = excluded.target_usd,
                fill_status = excluded.fill_status,
                filled_shares = excluded.filled_shares,
                avg_fill_price = excluded.avg_fill_price,
                book_age_s = excluded.book_age_s
            """,
            (
                run_id,
                intent_id,
                strategy,
                token_id,
                signal_ts,
                signal_price,
                target_usd,
                fill_status,
                filled_shares,
                avg_fill_price,
                book_age_s,
            ),
        )
