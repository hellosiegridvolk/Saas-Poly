"""Repository for live_orders — signed and (in LIVE mode) submitted orders.

Phase 1 surface (LIVE_DRY): insert + fetch_by_client_id.
Phase 3 will add update_status / record_fill for the fill reconciler.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from poly_terminal.persistence.db import Database


@dataclass(frozen=True)
class LiveOrderRow:
    intent_id: str
    strategy: str
    market_id: str
    token_id: str
    side: str             # "BUY" | "SELL"
    limit_price: float
    size_usd: float
    shares: float
    order_type: str       # "GTC" | "FOK" | "GTD" | "FAK"
    mode: str             # "LIVE_DRY" | "LIVE"
    client_order_id: str
    signed_order_json: str
    signed_at: int
    status: str = "signed"
    submitted_at: Optional[int] = None
    order_response_json: Optional[str] = None


class LiveOrdersRepo:
    def __init__(self, db: "Database") -> None:
        self._db = db

    async def insert(self, row: LiveOrderRow) -> int:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO live_orders
                  (intent_id, strategy, market_id, token_id, side,
                   limit_price, size_usd, shares, order_type, mode,
                   client_order_id, signed_order_json, status,
                   signed_at, submitted_at, order_response_json,
                   last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.intent_id, row.strategy, row.market_id, row.token_id,
                    row.side, row.limit_price, row.size_usd, row.shares,
                    row.order_type, row.mode, row.client_order_id,
                    row.signed_order_json, row.status, row.signed_at,
                    row.submitted_at, row.order_response_json, row.signed_at,
                ),
            )
            await conn.commit()
            return int(cur.lastrowid or 0)

    async def fetch_by_client_id(
        self, client_order_id: str
    ) -> Optional[dict[str, object]]:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT order_pk, intent_id, strategy, market_id, token_id,
                       side, limit_price, size_usd, shares, order_type,
                       mode, client_order_id, status, signed_at,
                       submitted_at, filled_qty, avg_fill_price,
                       polymarket_order_id, slippage_usd, fee_usd
                FROM live_orders
                WHERE client_order_id = ?
                """,
                (client_order_id,),
            )
            r = await cur.fetchone()
        if r is None:
            return None
        return {
            "order_pk": int(r[0]), "intent_id": str(r[1]),
            "strategy": str(r[2]), "market_id": str(r[3]),
            "token_id": str(r[4]), "side": str(r[5]),
            "limit_price": float(r[6]), "size_usd": float(r[7]),
            "shares": float(r[8]), "order_type": str(r[9]),
            "mode": str(r[10]), "client_order_id": str(r[11]),
            "status": str(r[12]), "signed_at": int(r[13]),
            "submitted_at": int(r[14]) if r[14] is not None else None,
            "filled_qty": float(r[15]),
            "avg_fill_price": float(r[16]) if r[16] is not None else None,
            "polymarket_order_id": str(r[17]) if r[17] is not None else None,
            "slippage_usd": float(r[18] or 0.0),
            "fee_usd": float(r[19] or 0.0),
        }

    async def count_by_mode_and_status(
        self, mode: str, status: str
    ) -> int:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) FROM live_orders WHERE mode=? AND status=?",
                (mode, status),
            )
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def has_recent_sell_for_token(
        self, token_id: str, since_ts: int
    ) -> bool:
        """2026-05-07 PHASE 14 — used by PositionImporter to detect the
        SELL settlement race. When the bot has just submitted a SELL
        but the chain hasn't yet reflected the resulting balance
        decrease, /positions still returns the pre-sell shares; the
        importer would otherwise create a phantom position for them.

        Returns True if any SELL row for `token_id` was signed at or
        after `since_ts`. We check on `signed_at` rather than
        `submitted_at` because the row is inserted at sign time —
        before the POST round-trip — so the gate is armed the instant
        the bot decides to sell, even if the POST is still in flight.

        Status filter is intentionally permissive (any status, not
        just 'submitted'): a 'signed' row that hasn't been
        submitted yet still represents pending intent to sell.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT 1 FROM live_orders
                WHERE token_id = ? AND side = 'SELL' AND signed_at >= ?
                LIMIT 1
                """,
                (token_id, int(since_ts)),
            )
            row = await cur.fetchone()
        return row is not None

    async def mark_submitted(
        self,
        *,
        client_order_id: str,
        response_json: str,
        submitted_at: int,
    ) -> bool:
        """Advance a 'signed' row to 'submitted' after a successful POST.

        Returns True if exactly one row updated, False otherwise.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE live_orders
                SET status='submitted',
                    submitted_at=?,
                    order_response_json=?,
                    last_updated=?
                WHERE client_order_id=? AND status='signed'
                """,
                (submitted_at, response_json, submitted_at, client_order_id),
            )
            await conn.commit()
        return (cur.rowcount or 0) == 1

    async def mark_rejected(
        self,
        *,
        client_order_id: str,
        response_json: str,
        ts: int,
    ) -> bool:
        """Advance a row to 'rejected' after a failed POST or explicit
        Polymarket rejection. Audit trail keeps the original signed
        order untouched.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE live_orders
                SET status='rejected',
                    order_response_json=?,
                    last_updated=?
                WHERE client_order_id=?
                """,
                (response_json, ts, client_order_id),
            )
            await conn.commit()
        return (cur.rowcount or 0) == 1

    async def set_status(
        self, *, polymarket_order_id: str, status: str, ts: int
    ) -> bool:
        """Phase 31 P1b — generic status setter keyed on
        `polymarket_order_id`. Used by the patient SELL helper to
        advance a row from 'submitted' to a terminal status when the
        GTC times out and is cancelled ('cancelled' or 'cancel_failed').

        Returns True if exactly one row updated.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE live_orders
                SET status = ?, last_updated = ?
                WHERE polymarket_order_id = ?
                """,
                (status, ts, polymarket_order_id),
            )
            await conn.commit()
        return (cur.rowcount or 0) == 1

    async def set_polymarket_order_id(
        self, *, client_order_id: str, polymarket_order_id: str, ts: int
    ) -> bool:
        """Store Polymarket's order hash so the fill reconciler can
        look up by it. Called after post_order succeeds and the
        response contains an `orderID` field.
        """
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE live_orders
                SET polymarket_order_id=?, last_updated=?
                WHERE client_order_id=?
                """,
                (polymarket_order_id, ts, client_order_id),
            )
            await conn.commit()
        return (cur.rowcount or 0) == 1

    async def fetch_by_polymarket_order_id(
        self, polymarket_order_id: str
    ) -> Optional[dict[str, object]]:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT order_pk, intent_id, strategy, market_id, token_id,
                       side, limit_price, size_usd, shares, order_type,
                       mode, client_order_id, status, signed_at,
                       submitted_at, filled_qty, avg_fill_price,
                       polymarket_order_id, slippage_usd, fee_usd
                FROM live_orders
                WHERE polymarket_order_id = ?
                """,
                (polymarket_order_id,),
            )
            r = await cur.fetchone()
        if r is None:
            return None
        return {
            "order_pk": int(r[0]), "intent_id": str(r[1]),
            "strategy": str(r[2]), "market_id": str(r[3]),
            "token_id": str(r[4]), "side": str(r[5]),
            "limit_price": float(r[6]), "size_usd": float(r[7]),
            "shares": float(r[8]), "order_type": str(r[9]),
            "mode": str(r[10]), "client_order_id": str(r[11]),
            "status": str(r[12]), "signed_at": int(r[13]),
            "submitted_at": int(r[14]) if r[14] is not None else None,
            "filled_qty": float(r[15]),
            "avg_fill_price": float(r[16]) if r[16] is not None else None,
            "polymarket_order_id": str(r[17]) if r[17] is not None else None,
            "slippage_usd": float(r[18] or 0.0),
            "fee_usd": float(r[19] or 0.0),
        }

    async def record_fill(
        self,
        *,
        polymarket_order_id: str,
        fill_qty: float,
        fill_price: float,
        ts: int,
        terminal: bool = False,
        fill_fee_usd: float = 0.0,
    ) -> Optional[dict[str, object]]:
        """Apply a fill event to the matching row. Aggregates partial
        fills into avg_fill_price weighted by qty. `terminal=True`
        marks the row 'filled' (full match); else it stays at 'partial'.

        Also accumulates slippage_usd (positive = favorable vs limit)
        and fee_usd. Returns the post-update row snapshot or None.
        """
        existing = await self.fetch_by_polymarket_order_id(polymarket_order_id)
        if existing is None:
            return None
        prior_qty = float(existing["filled_qty"])
        prior_avg = (
            float(existing["avg_fill_price"])
            if existing.get("avg_fill_price") is not None
            else 0.0
        )
        new_qty = prior_qty + fill_qty
        if new_qty <= 0:
            new_avg = fill_price
        else:
            new_avg = (prior_qty * prior_avg + fill_qty * fill_price) / new_qty

        # Slippage: signed favorability versus limit_price, on the
        # marginal qty just filled. We accumulate the marginal because
        # avg_fill_price changes over partial fills — the running
        # total is sum of (qty_i * favorability_i).
        side = str(existing["side"])
        limit_price = float(existing["limit_price"])
        if side == "BUY":
            marginal_slip = (limit_price - fill_price) * fill_qty
        else:
            marginal_slip = (fill_price - limit_price) * fill_qty
        prior_slip = await self._fetch_slippage_and_fee(polymarket_order_id)
        new_slip = prior_slip[0] + marginal_slip
        new_fee = prior_slip[1] + float(fill_fee_usd)

        new_status = "filled" if terminal else "partial"
        async with self._db.connect() as conn:
            await conn.execute(
                """
                UPDATE live_orders
                SET filled_qty=?, avg_fill_price=?, status=?,
                    slippage_usd=?, fee_usd=?, last_updated=?
                WHERE polymarket_order_id=?
                """,
                (
                    new_qty, new_avg, new_status,
                    new_slip, new_fee, ts, polymarket_order_id,
                ),
            )
            await conn.commit()
        return {
            **existing,
            "filled_qty": new_qty,
            "avg_fill_price": new_avg,
            "status": new_status,
            "slippage_usd": new_slip,
            "fee_usd": new_fee,
            "last_updated": ts,
        }

    async def _fetch_slippage_and_fee(
        self, polymarket_order_id: str
    ) -> tuple[float, float]:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT slippage_usd, fee_usd FROM live_orders "
                "WHERE polymarket_order_id=?",
                (polymarket_order_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return (0.0, 0.0)
        return (float(row[0] or 0.0), float(row[1] or 0.0))

    async def mark_cancelled(
        self, *, polymarket_order_id: str, ts: int
    ) -> bool:
        """Polymarket cancelled or expired the order before full fill."""
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE live_orders
                SET status='cancelled', last_updated=?
                WHERE polymarket_order_id=? AND status IN ('submitted','partial')
                """,
                (ts, polymarket_order_id),
            )
            await conn.commit()
        return (cur.rowcount or 0) == 1
