"""Repository for paper_fills + positions writes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from poly_terminal.persistence.db import Database


@dataclass(frozen=True)
class PaperFillRow:
    intent_id: str
    strategy: str
    market_id: str
    token_id: str
    side: str
    qty: float
    signal_price: float
    fill_price: float
    signal_at: int
    filled_at: int


class FillsRepo:
    def __init__(self, db: "Database") -> None:
        self._db = db

    async def insert_paper_fill(self, row: PaperFillRow) -> int:
        """Insert a paper fill. Returns the fill_id."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO paper_fills
                  (intent_id, strategy, market_id, token_id, side,
                   qty, signal_price, fill_price, signal_at, filled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.intent_id,
                    row.strategy,
                    row.market_id,
                    row.token_id,
                    row.side,
                    row.qty,
                    row.signal_price,
                    row.fill_price,
                    row.signal_at,
                    row.filled_at,
                ),
            )
            await conn.commit()
            return int(cur.lastrowid or 0)

    async def count(self) -> int:
        async with self._db.connect() as conn:
            cur = await conn.execute("SELECT COUNT(*) FROM paper_fills")
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def fetch_recent(self, limit: int = 100) -> list[dict[str, object]]:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT fill_id, intent_id, strategy, market_id, token_id,
                       side, qty, signal_price, fill_price, signal_at, filled_at
                FROM paper_fills
                ORDER BY filled_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cur.fetchall()
        return [
            {
                "fill_id": r[0],
                "intent_id": r[1],
                "strategy": r[2],
                "market_id": r[3],
                "token_id": r[4],
                "side": r[5],
                "qty": r[6],
                "signal_price": r[7],
                "fill_price": r[8],
                "signal_at": r[9],
                "filled_at": r[10],
            }
            for r in rows
        ]


@dataclass(frozen=True)
class PositionRow:
    market_id: str
    token_id: str
    side: str
    entry_price: float
    shares: float
    cost_basis_usd: float
    entry_intent_id: str
    entry_ts: int
    end_date_iso: str | None = None
    strategy: str | None = None
    lane_id: str | None = None
    source_wallet: str | None = None


class PositionsRepo:
    def __init__(self, db: "Database") -> None:
        self._db = db

    async def open_position(self, row: PositionRow) -> int:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO positions
                  (market_id, token_id, side, entry_price, shares,
                   cost_basis_usd, entry_intent_id, entry_ts, end_date_iso,
                   strategy, lane_id, source_wallet)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.market_id,
                    row.token_id,
                    row.side,
                    row.entry_price,
                    row.shares,
                    row.cost_basis_usd,
                    row.entry_intent_id,
                    row.entry_ts,
                    row.end_date_iso,
                    row.strategy,
                    row.lane_id,
                    row.source_wallet,
                ),
            )
            await conn.commit()
            return int(cur.lastrowid or 0)

    async def realized_pnl_since_for_lane(
        self, lane_id: str, since_ts: int
    ) -> float:
        """Signed realized PnL for one lane since `since_ts` (close ts).
        Negative == net loss. 0.0 when the lane has no closed rows."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0.0) FROM positions "
                "WHERE lane_id = ? AND closed_ts >= ? "
                "AND realized_pnl IS NOT NULL",
                (lane_id, since_ts),
            )
            (total,) = await cur.fetchone()
            return float(total)

    async def fetch_all_open(self) -> list[dict[str, object]]:
        """Every position with closed_ts IS NULL — used by ExitAgent's
        restart-recovery hook to rebuild bar_end_ts after a process
        restart.

        2026-05-05: LEFT JOIN live_orders to recover the originating
        strategy when available. Without it, restored positions get
        the empty-string strategy marker which now disables
        max_hold-in-bar_watcher enforcement (added 2026-05-05). With
        the strategy, max_hold is applied correctly on restart.
        Imported positions have no live_order row → strategy is NULL
        → still skipped, which is the right behavior (imported
        positions are managed by the user, not the bot).
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT p.position_id, p.market_id, p.token_id, p.side,
                       p.entry_price, p.shares, p.cost_basis_usd,
                       p.entry_intent_id, p.entry_ts, p.end_date_iso,
                       lo.strategy
                FROM positions p
                LEFT JOIN live_orders lo
                       ON lo.intent_id = p.entry_intent_id
                WHERE p.closed_ts IS NULL
                """
            )
            rows = await cur.fetchall()
        return [
            {
                "position_id": int(r[0]),
                "market_id": str(r[1]),
                "token_id": str(r[2]),
                "side": str(r[3]),
                "entry_price": float(r[4]),
                "shares": float(r[5]),
                "cost_basis_usd": float(r[6]),
                "entry_intent_id": str(r[7]),
                "entry_ts": int(r[8]),
                "end_date_iso": (str(r[9]) if r[9] is not None else None),
                "strategy": (str(r[10]) if r[10] is not None else ""),
            }
            for r in rows
        ]

    async def fetch_unreconciled_flat_closes(
        self, *, limit: int | None = None
    ) -> list[dict[str, object]]:
        """Return closed positions whose realized_pnl is exactly $0 AND
        entry_price == exit_price — the fingerprint of the shadow-price
        fallback path (2026-05-05). When `_resolve_exit_price` falls back
        to entry_price (because get_best_bid raised or returned None),
        the close is recorded with PnL=0 even though the underlying
        market may have settled YES (+payout) or NO (-cost_basis).

        These rows need post-resolution reconciliation against the
        Gamma `/markets` endpoint to recover the true outcome.
        Filtered to `outcome IN ('TIME','TIME_RESTORE')` to avoid
        touching SL/TP/MANUAL_CLOSE rows (those have legitimate $0
        cases — e.g., entry==exit at time-stop on a quiet market).

        Skips already-reconciled rows (`outcome='TIME_RECONCILED'`)
        so reconcile passes are idempotent.
        """
        sql = (
            "SELECT position_id, market_id, token_id, side, entry_price, "
            "       shares, cost_basis_usd, entry_intent_id, entry_ts, "
            "       closed_ts, exit_price, realized_pnl, outcome, "
            "       end_date_iso "
            "FROM positions "
            "WHERE closed_ts IS NOT NULL "
            "  AND realized_pnl = 0 "
            "  AND exit_price = entry_price "
            "  AND outcome IN ('TIME','TIME_RESTORE') "
            "ORDER BY closed_ts ASC"
        )
        params: tuple[object, ...] = ()
        if limit is not None and int(limit) > 0:
            sql += " LIMIT ?"
            params = (int(limit),)
        async with self._db.connect() as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
        return [
            {
                "position_id": int(r[0]),
                "market_id": str(r[1]),
                "token_id": str(r[2]),
                "side": str(r[3]),
                "entry_price": float(r[4]),
                "shares": float(r[5]),
                "cost_basis_usd": float(r[6]),
                "entry_intent_id": str(r[7]),
                "entry_ts": int(r[8]),
                "closed_ts": int(r[9]),
                "exit_price": float(r[10]),
                "realized_pnl": float(r[11]),
                "outcome": str(r[12]) if r[12] is not None else "",
                "end_date_iso": (str(r[13]) if r[13] is not None else None),
            }
            for r in rows
        ]

    async def update_reconciled_pnl(
        self,
        *,
        position_id: int,
        exit_price: float,
        realized_pnl: float,
        outcome: str = "TIME_RECONCILED",
    ) -> bool:
        """Overwrite realized_pnl + exit_price + outcome on a previously
        flat-closed position. Returns True iff exactly one row was
        updated. Idempotent on `outcome != 'TIME_RECONCILED'` — once
        a row is reconciled we won't touch it again from
        `fetch_unreconciled_flat_closes`.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE positions
                SET exit_price = ?, realized_pnl = ?, outcome = ?
                WHERE position_id = ? AND closed_ts IS NOT NULL
                  AND outcome IN ('TIME','TIME_RESTORE')
                """,
                (float(exit_price), float(realized_pnl), str(outcome),
                 int(position_id)),
            )
            await conn.commit()
            return (cur.rowcount or 0) == 1

    async def open_count(self, include_imported: bool = False) -> int:
        """Open positions counted toward the bot's MAX_OPEN_POSITIONS cap.

        2026-05-03 P1 fix (deep-research-report 17 §imported-cap):
        previously this was an unfiltered `SELECT COUNT(*)` so the
        13 imported on-chain positions ate most of a 15-slot cap and
        starved bot strategies. Default is now bot-managed-only —
        callers that want the truly-everything count (e.g., a
        portfolio dashboard) pass include_imported=True.

        Imported positions are identified by the entry_intent_id
        prefix `imported` (set by PositionImporterAgent). This
        avoids a schema migration and is reversible.
        """
        async with self._db.connect() as conn:
            if include_imported:
                cur = await conn.execute(
                    "SELECT COUNT(*) FROM positions WHERE closed_ts IS NULL"
                )
            else:
                cur = await conn.execute(
                    "SELECT COUNT(*) FROM positions "
                    "WHERE closed_ts IS NULL "
                    "AND (entry_intent_id IS NULL "
                    "     OR entry_intent_id NOT LIKE 'imported%')"
                )
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def fetch_open(self, position_id: int) -> dict[str, object] | None:
        """Fetch an open position by id; returns None if missing or already closed."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT position_id, market_id, token_id, side, entry_price,
                       shares, cost_basis_usd, entry_intent_id, entry_ts
                FROM positions
                WHERE position_id = ? AND closed_ts IS NULL
                """,
                (position_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "position_id": int(row[0]),
            "market_id": str(row[1]),
            "token_id": str(row[2]),
            "side": str(row[3]),
            "entry_price": float(row[4]),
            "shares": float(row[5]),
            "cost_basis_usd": float(row[6]),
            "entry_intent_id": str(row[7]),
            "entry_ts": int(row[8]),
        }

    async def close_position(
        self,
        position_id: int,
        exit_price: float,
        outcome: str,
        closed_ts: int,
    ) -> dict[str, object] | None:
        """Set closed_ts/exit_price/realized_pnl/outcome on an open position.

        Returns the closed snapshot (for the caller to publish on the bus)
        or None if the position was already closed / unknown.
        """
        opened = await self.fetch_open(position_id)
        if opened is None:
            return None
        shares = opened["shares"]
        entry_price = opened["entry_price"]
        # Realized PnL on a BUY position: (exit - entry) * shares.
        # SELL/short positions invert; v3 only opens BUYs today but we
        # keep the side check so this generalizes cleanly.
        side = opened["side"]
        if side == "BUY":
            realized = (exit_price - entry_price) * shares
        else:
            realized = (entry_price - exit_price) * shares
        async with self._db.connect() as conn:
            # 2026-05-03 P1 #2: also zero shares_remaining on full close
            # so a duplicate close attempt or stale partial-close path
            # can't double-decrement.
            cur = await conn.execute(
                """
                UPDATE positions
                SET closed_ts = ?, exit_price = ?, realized_pnl = ?,
                    outcome = ?, shares_remaining = 0
                WHERE position_id = ? AND closed_ts IS NULL
                """,
                (closed_ts, exit_price, realized, outcome, position_id),
            )
            await conn.commit()
        if cur.rowcount == 0:
            return None
        return {
            **opened,
            "closed_ts": closed_ts,
            "exit_price": exit_price,
            "realized_pnl": realized,
            "outcome": outcome,
        }

    async def restate_close_price(
        self,
        position_id: int,
        actual_exit_price: float,
    ) -> dict[str, object] | None:
        """Re-state an already-closed position's exit_price + realized_pnl
        to match the actual on-chain fill.

        2026-05-06 PHASE 4 — when ExitAgent fires EVT_SELL_INTENT,
        ExecutionAgent calls close_position FIRST with the limit-price
        hint, THEN signs+submits the live SELL. If the SELL fills at a
        DIFFERENT (usually better) price than the limit, the position
        row keeps the wrong exit_price and the wrong realized_pnl.

        Production case: pos 22319 closed at limit $0.05 → realized
        -$1.05; actual fill was $0.42 → realized +$11.16. A $12 swing
        the bot was completely blind to.

        Idempotent — only updates rows that are already closed (caller
        guarantees this since restating an open position is a logic
        error). Returns the updated snapshot or None if not found /
        not yet closed.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT side, entry_price, shares, closed_ts, "
                "       exit_price, realized_pnl "
                "FROM positions WHERE position_id=? AND closed_ts IS NOT NULL",
                (position_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            side, entry_price, shares, closed_ts, _old_exit, _old_pnl = row
            if side == "BUY":
                realized = (actual_exit_price - entry_price) * shares
            else:
                realized = (entry_price - actual_exit_price) * shares
            await conn.execute(
                "UPDATE positions "
                "SET exit_price=?, realized_pnl=? "
                "WHERE position_id=?",
                (actual_exit_price, realized, position_id),
            )
            await conn.commit()
        return {
            "position_id": position_id,
            "exit_price": actual_exit_price,
            "realized_pnl": realized,
            "closed_ts": closed_ts,
            "prior_exit_price": float(_old_exit) if _old_exit is not None else None,
            "prior_realized_pnl": float(_old_pnl) if _old_pnl is not None else None,
        }

    async def restate_close_failed(
        self,
        position_id: int,
    ) -> dict[str, object] | None:
        """Mark a closed position as SELL_FAILED — undo phantom
        realized_pnl/exit_price set by close_position when the SELL
        never actually filled on-chain.

        2026-05-06 PHASE 7 — when SELL escalation exhausts (FAK
        no-match across all retries) OR when chain-settle balance
        retries exhaust, the on-chain shares stay in the wallet
        with NO proceeds. close_position has already run with the
        LIMIT price, recording fictional realized_pnl. Phase 7
        reverts that bookkeeping so dashboards + bilan reflect
        reality (no profit/loss until the importer/redeemer picks
        up the leftover shares).

        Operations applied:
          • realized_pnl = 0
          • exit_price   = NULL
          • outcome      = 'SELL_FAILED'

        closed_ts is left intact so the position reads as "closed"
        for ExitAgent (no further EVT_SELL_INTENT loops). The
        importer will reconstruct the on-chain shares as a fresh
        imported position on its next sweep.

        Idempotent — only acts on rows already closed; returns the
        post-update snapshot or None if not found / not yet closed.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT closed_ts, exit_price, realized_pnl FROM positions "
                "WHERE position_id=? AND closed_ts IS NOT NULL",
                (position_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            closed_ts, prior_exit, prior_pnl = row
            await conn.execute(
                "UPDATE positions "
                "SET exit_price=NULL, realized_pnl=0.0, "
                "    outcome='SELL_FAILED' "
                "WHERE position_id=?",
                (position_id,),
            )
            await conn.commit()
        return {
            "position_id": position_id,
            "outcome": "SELL_FAILED",
            "exit_price": None,
            "realized_pnl": 0.0,
            "closed_ts": closed_ts,
            "prior_exit_price": float(prior_exit) if prior_exit is not None else None,
            "prior_realized_pnl": float(prior_pnl) if prior_pnl is not None else None,
        }

    async def reduce_open_position(
        self,
        position_id: int,
        qty_delta: float,
        exit_price: float,
        closed_ts: int,
    ) -> dict[str, object] | None:
        """Partial close — decrement shares_remaining by qty_delta and
        accumulate realized_pnl on the closed fraction. Does NOT set
        closed_ts (position remains open with reduced inventory).

        Returns {position_id, shares_remaining_before, shares_remaining_after,
        partial_pnl, fully_closed} or None if the position is unknown
        or already fully closed.

        2026-05-03 P1 #2 fix (deep-research-report 14/15/16/17 §partial-close).
        Used by LiveFillReconciler when an external SELL fill is smaller
        than the matched open position's remaining shares.

        If qty_delta >= shares_remaining, the position is fully closed
        with outcome='MANUAL_CLOSE' (caller should treat as terminal).
        Caller is responsible for publishing EVT_POSITION_CLOSED only
        when fully_closed is True.
        """
        async with self._db.connect() as conn:
            # Read current state under the same connection.
            cur = await conn.execute(
                """
                SELECT side, entry_price, shares,
                       COALESCE(shares_remaining, shares) AS remaining
                FROM positions
                WHERE position_id = ? AND closed_ts IS NULL
                """,
                (position_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            side = str(row[0])
            entry_price = float(row[1])
            shares = float(row[2])
            remaining = float(row[3])
            if remaining <= 0:
                return None
            sold = min(float(qty_delta), remaining)
            if sold <= 0:
                return None
            new_remaining = remaining - sold
            # Realized PnL on the closed fraction only.
            if side == "BUY":
                partial_pnl = (exit_price - entry_price) * sold
            else:
                partial_pnl = (entry_price - exit_price) * sold
            fully_closed = new_remaining <= 1e-9
            if fully_closed:
                # Accumulate any prior partial pnl into total realized.
                # We approximate by re-computing on the full position
                # (shares × (exit - entry)), but for stacked partials
                # this loses fidelity. Acceptable for v1 — proper
                # ledger-level accumulation is a P2 follow-up.
                if side == "BUY":
                    total_pnl = (exit_price - entry_price) * shares
                else:
                    total_pnl = (entry_price - exit_price) * shares
                await conn.execute(
                    """
                    UPDATE positions
                    SET closed_ts = ?, exit_price = ?, realized_pnl = ?,
                        outcome = 'MANUAL_CLOSE', shares_remaining = 0
                    WHERE position_id = ? AND closed_ts IS NULL
                    """,
                    (closed_ts, exit_price, total_pnl, position_id),
                )
            else:
                # Partial close: track running realized_pnl on the
                # decremented portion. We use realized_pnl as the
                # running total across all partials so far. exit_price
                # tracks the last fill price.
                cur2 = await conn.execute(
                    "SELECT COALESCE(realized_pnl, 0) FROM positions "
                    "WHERE position_id = ?",
                    (position_id,),
                )
                row2 = await cur2.fetchone()
                running_pnl = float(row2[0] if row2 else 0) + partial_pnl
                await conn.execute(
                    """
                    UPDATE positions
                    SET shares_remaining = ?, exit_price = ?,
                        realized_pnl = ?, outcome = 'PARTIAL_MANUAL_CLOSE'
                    WHERE position_id = ? AND closed_ts IS NULL
                    """,
                    (new_remaining, exit_price, running_pnl, position_id),
                )
            await conn.commit()
        return {
            "position_id": position_id,
            "shares_remaining_before": remaining,
            "shares_remaining_after": new_remaining,
            "partial_pnl": partial_pnl,
            "fully_closed": fully_closed,
        }

    async def fetch_all_open_token_ids(self) -> list[str]:
        """Distinct token_ids across ALL currently-open positions
        (bot-managed AND imported). Used by the WS subscription
        re-sync watchdog so every open position gets ticks even if
        the EVT_WALLET_FILL → market_ws.subscribe_tokens path missed
        an event (e.g., race at boot, dedupe set hit, importer-only
        position).

        2026-05-04 patch: bar_watcher uses pos.last_price; if no tick
        ever arrives, it falls back to entry_price → exit=entry → $0
        PnL on every close. Periodic re-sync prevents the silent
        coverage drift that produced 100% flat closes after restart.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT DISTINCT token_id FROM positions "
                "WHERE closed_ts IS NULL AND token_id IS NOT NULL "
                "AND token_id != ''"
            )
            rows = await cur.fetchall()
        return [str(r[0]) for r in rows]

    async def fetch_closed_unredeemed(self) -> list[dict[str, object]]:
        """Closed positions that haven't been classified as redeemable
        / worthless yet. RedeemerAgent's working set.

        Returns enough fields for Gamma lookup, inventory gate, and
        classification:
          - position_id (PK)
          - entry_intent_id (joins to live_orders.client_order_id
            via the `poly-v3-{intent_id}` convention to verify
            on-chain inventory)
          - market_id (Polymarket conditionId)
          - token_id (the specific outcome the position holds)
          - shares (size used for $ estimate of REDEEMABLE total)
          - exit_price (for filtering noisy "already settled paper" rows)
          - closed_ts (for nudge cadence)
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT position_id, entry_intent_id, market_id, token_id,
                       shares, exit_price, closed_ts
                FROM positions
                WHERE closed_ts IS NOT NULL AND redeemed_ts IS NULL
                """
            )
            rows = await cur.fetchall()
        return [
            {
                "position_id": int(r[0]),
                "entry_intent_id": str(r[1] or ""),
                "market_id": str(r[2]),
                "token_id": str(r[3]),
                "shares": float(r[4]),
                "exit_price": float(r[5] or 0),
                "closed_ts": int(r[6]),
            }
            for r in rows
        ]

    async def has_open_position_for_token(self, token_id: str) -> bool:
        """Used by PositionImporterAgent to dedupe — don't double-import
        a token we already have an open position on."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT 1 FROM positions WHERE token_id = ? "
                "AND closed_ts IS NULL LIMIT 1",
                (token_id,),
            )
            row = await cur.fetchone()
        return row is not None

    async def fetch_all_open_imported_tokens(self) -> list[str]:
        """Distinct token_ids of currently-open IMPORTED positions.

        Used by PositionImporterAgent.delta_sweep() to detect tokens
        whose on-chain inventory dropped to 0 (manual close on
        Polymarket UI that the User WS missed). Returns only imported
        positions because delta-sweep should never close bot-managed
        positions — those are owned by ProfitTaker / SELL escalator
        and may have legitimately mid-flight orders.

        2026-05-03 P1 #1 fix (deep-research-report 17 §importer-delta-sweep).
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT DISTINCT token_id FROM positions "
                "WHERE closed_ts IS NULL "
                "AND entry_intent_id LIKE 'imported%'"
            )
            rows = await cur.fetchall()
        return [str(r[0]) for r in rows]

    async def close_open_for_token(
        self,
        *,
        token_id: str,
        outcome: str,
        closed_ts: int,
        only_imported: bool = True,
    ) -> int:
        """Close every OPEN position on the given token. Returns count
        closed. Defaults to imported-only so delta-sweep can't nuke a
        bot-managed position that might have a legitimate in-flight
        SELL escalator attempt. Sets exit_price = entry_price (we
        don't know the manual fill price; the User WS path uses
        close_position with the actual price, this path is the
        backstop when the WS event was missed).
        """
        async with self._db.connect() as conn:
            if only_imported:
                cur = await conn.execute(
                    """
                    UPDATE positions
                    SET closed_ts = ?,
                        exit_price = entry_price,
                        realized_pnl = 0.0,
                        outcome = ?
                    WHERE token_id = ?
                      AND closed_ts IS NULL
                      AND entry_intent_id LIKE 'imported%'
                    """,
                    (closed_ts, outcome, token_id),
                )
            else:
                cur = await conn.execute(
                    """
                    UPDATE positions
                    SET closed_ts = ?,
                        exit_price = entry_price,
                        realized_pnl = 0.0,
                        outcome = ?
                    WHERE token_id = ?
                      AND closed_ts IS NULL
                    """,
                    (closed_ts, outcome, token_id),
                )
            await conn.commit()
        return int(cur.rowcount or 0)

    async def fetch_oldest_open_for_token(
        self, token_id: str
    ) -> dict[str, object] | None:
        """FIFO open-position lookup for a given token. Used by
        LiveFillReconciler to map an unmatched manual-close trade
        (a SELL on Polymarket UI by the operator) to the position
        that was first opened on that token. Returns None if no
        open position exists.

        FIFO is intentional — when multiple BUYs stacked on the
        same token, the manual SELL most likely closed the oldest
        first (bot fills go through ProfitTaker / bar-resolver
        which manage newest first, so the surviving open positions
        are the older ones).
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT position_id, market_id, token_id, side,
                       entry_price, shares, cost_basis_usd,
                       entry_intent_id, entry_ts, end_date_iso
                FROM positions
                WHERE token_id = ? AND closed_ts IS NULL
                ORDER BY entry_ts ASC, position_id ASC
                LIMIT 1
                """,
                (token_id,),
            )
            r = await cur.fetchone()
        if r is None:
            return None
        return {
            "position_id": int(r[0]),
            "market_id": str(r[1]),
            "token_id": str(r[2]),
            "side": str(r[3]),
            "entry_price": float(r[4]),
            "shares": float(r[5]),
            "cost_basis_usd": float(r[6]),
            "entry_intent_id": str(r[7] or ""),
            "entry_ts": int(r[8]),
            "end_date_iso": (str(r[9]) if r[9] is not None else None),
        }

    async def realized_pnl_since(self, since_ts: int) -> tuple[int, float]:
        """(close_count, realized_pnl_sum) for positions closed since
        `since_ts`. Used by AutoTunerAgent to read rolling PnL."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(realized_pnl), 0) "
                "FROM positions WHERE closed_ts >= ?",
                (since_ts,),
            )
            row = await cur.fetchone()
        if row is None:
            return 0, 0.0
        return int(row[0] or 0), float(row[1] or 0.0)

    async def count_open_for_token(self, token_id: str) -> int:
        """How many positions we currently hold on this exact outcome
        token. Used by MarketConcentrationGate to cap stacking on the
        same market+side."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) FROM positions "
                "WHERE token_id = ? AND closed_ts IS NULL",
                (token_id,),
            )
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def has_imported_position(self, token_id: str) -> bool:
        """Like above but covers BOTH open and closed imports — used
        for the redeemable-position branch where we insert a row in
        a single sweep and immediately close it. Without this we'd
        re-import the same redeemable position every sweep."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT 1 FROM positions WHERE token_id = ? "
                "AND entry_intent_id = ? LIMIT 1",
                (token_id, f"imported:{token_id}"),
            )
            row = await cur.fetchone()
        return row is not None

    async def mark_redeemed(
        self,
        position_id: int,
        redeemed_ts: int,
        redeem_tx_hash: str,
        payout_usd: float | None = None,
    ) -> bool:
        """Mark a closed position as redeemed (or known-worthless).

        - For REDEEMABLE rows that the operator (or future relayer
          path) actually settles: pass the on-chain tx hash.
        - For WORTHLESS rows (we held the losing outcome): pass the
          sentinel `'WORTHLESS_NO_TX'` so the queue clears without
          touching chain. The string is intentionally non-hex so any
          downstream "is_tx_hash" check refuses to look it up.

        2026-05-09 — when the sentinel is WORTHLESS_NO_TX, we ALSO
        overwrite realized_pnl with -cost_basis_usd. v48 found pos
        22488 had realized_pnl=0 despite being worthless: when the
        SELL never fills, close_position records exit_price=entry_price
        and realized_pnl=0 (a fictitious flat). The redeemer's
        worthless verdict is the FIRST point at which we know with
        certainty that the proceeds are zero, so it's the correct
        moment to truth-up the row.

        2026-05-09 PHASE 31 — also truth-up when redeem_tx_hash is a
        real on-chain hash but `payout_usd` is approximately zero.
        v50r2 pos 22492 hit this: real tx 0x83e01c..., payout $0
        (auto_redeem_logged "payout≈$0.00"), but realized_pnl stayed
        at $0.00 instead of being truth-upped. This is silent loss
        accounting. When `payout_usd` is provided AND <= $0.01, treat
        it as economic worthlessness — set realized_pnl =
        payout_usd - cost_basis_usd.

        PAPER_NO_TX is left alone — its realized_pnl was already
        settled by close_position with a paper exit price.

        Idempotent: only updates when redeemed_ts IS NULL, so repeat
        calls return False.
        """
        zero_payout_real_tx = (
            redeem_tx_hash != "WORTHLESS_NO_TX"
            and redeem_tx_hash != "PAPER_NO_TX"
            and payout_usd is not None
            and float(payout_usd) <= 0.01
        )
        # 2026-05-10 PHASE 33 — PAPER positions get the SAME truth-up
        # treatment as on-chain redeems when payout_usd is supplied,
        # but ONLY when realized_pnl is approximately zero. A clean
        # TP/SL fill that already booked the real gain/loss must be
        # preserved (the truth-up exists to repair silent-loss cases,
        # not to rewrite cleanly-filled trades).
        paper_truth_up = (
            redeem_tx_hash == "PAPER_NO_TX"
            and payout_usd is not None
        )
        async with self._db.connect() as conn:
            if redeem_tx_hash == "WORTHLESS_NO_TX":
                # Truth-up: the position resolved to $0 of proceeds, so
                # the entire cost basis is the realized loss.
                # 2026-05-11 PHASE 35 — also flip `outcome` to
                # WORTHLESS_REDEEM so dashboards can distinguish this
                # from a clean TP/SL/TIME fill. Without this, outcome
                # was stuck at whatever the ExitDecisionEngine wrote
                # eagerly — leaving rows like position 22920 with
                # outcome='TP' AND realized_pnl=-cost_basis (impossible).
                cur = await conn.execute(
                    """
                    UPDATE positions
                    SET redeemed_ts = ?,
                        redeem_tx_hash = ?,
                        realized_pnl = -cost_basis_usd,
                        outcome = CASE
                            WHEN cost_basis_usd > 0
                            THEN 'WORTHLESS_REDEEM'
                            ELSE outcome
                        END
                    WHERE position_id = ? AND redeemed_ts IS NULL
                    """,
                    (redeemed_ts, redeem_tx_hash, position_id),
                )
            elif zero_payout_real_tx:
                # Real on-chain tx but payout was effectively zero.
                # Same truth-up: realized_pnl = payout - cost_basis.
                # PHASE 35 — flip outcome to WORTHLESS_REDEEM iff the
                # truth-up writes a loss (it almost always does for
                # this branch).
                payout = float(payout_usd or 0)
                cur = await conn.execute(
                    """
                    UPDATE positions
                    SET redeemed_ts = ?,
                        redeem_tx_hash = ?,
                        realized_pnl = ? - cost_basis_usd,
                        outcome = CASE
                            WHEN ? - cost_basis_usd < 0
                            THEN 'WORTHLESS_REDEEM'
                            ELSE outcome
                        END
                    WHERE position_id = ? AND redeemed_ts IS NULL
                    """,
                    (redeemed_ts, redeem_tx_hash,
                     payout, payout, position_id),
                )
            elif paper_truth_up:
                # Phase 33 — PAPER position resolved via Gamma. Update
                # realized_pnl ONLY when it's approximately zero (the
                # silent-loss signature); preserve clean TP/SL fills.
                # PHASE 35 — when the predicate fires AND the truth-up
                # writes a loss, also flip outcome to WORTHLESS_REDEEM
                # for the same reasons as the WORTHLESS_NO_TX branch.
                # SQLite evaluates SET expressions against OLD row
                # values, so the outcome-CASE reads the pre-update
                # realized_pnl — same predicate semantics as the
                # realized_pnl CASE on the row above.
                payout = float(payout_usd or 0)
                cur = await conn.execute(
                    """
                    UPDATE positions
                    SET redeemed_ts = ?,
                        redeem_tx_hash = ?,
                        realized_pnl = CASE
                            WHEN ABS(COALESCE(realized_pnl, 0)) < 0.01
                            THEN ? - cost_basis_usd
                            ELSE realized_pnl
                        END,
                        outcome = CASE
                            WHEN ABS(COALESCE(realized_pnl, 0)) < 0.01
                                AND ? - cost_basis_usd < 0
                            THEN 'WORTHLESS_REDEEM'
                            ELSE outcome
                        END
                    WHERE position_id = ? AND redeemed_ts IS NULL
                    """,
                    (redeemed_ts, redeem_tx_hash,
                     payout, payout, position_id),
                )
            else:
                cur = await conn.execute(
                    """
                    UPDATE positions
                    SET redeemed_ts = ?, redeem_tx_hash = ?
                    WHERE position_id = ? AND redeemed_ts IS NULL
                    """,
                    (redeemed_ts, redeem_tx_hash, position_id),
                )
            await conn.commit()
        return cur.rowcount > 0
