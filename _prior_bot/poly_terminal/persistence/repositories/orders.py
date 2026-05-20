"""Phase 30(a) — user-channel order/trade lifecycle persistence.

The bot's `live_orders` table records SIGNED+SUBMITTED orders from
the bot's side. The Polymarket user-channel WS publishes `order` and
`trade` events for the SAME orders as their on-chain lifecycle
progresses (LIVE → MATCHED → CANCELLED / EXPIRED).

This repo writes those user-channel events to the existing `orders`
table (defined in migrations/0001_initial.py) so post-mortem audits
can distinguish between:
  - "no SELL chosen by the bot"
  - "SELL chosen and signed but never reached the chain"
  - "SELL matched on chain but failed downstream"

Used by `OrdersRecorderAgent` which subscribes to EVT_ORDER_*
events from `UserDispatcher`.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from poly_terminal.persistence.db import Database

logger = logging.getLogger(__name__)


class OrdersRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def upsert(
        self,
        *,
        order_id: str,
        token_id: str,
        side: str,
        state: str,
        price: float = 0.0,
        size: float = 0.0,
        filled_size: float = 0.0,
        intent_id: str = "",
        condition_id: str = "",
        strategy: str = "",
        paper: int = 0,
    ) -> None:
        """Upsert a row keyed on order_id. State transitions are
        recorded by overwriting `state` and `updated_at`; all other
        fields are written once at first insert and updated on each
        event so partial-fill progress is visible.

        Defensive: any persistence error is logged and swallowed —
        observability must never block the event loop.
        """
        if not order_id:
            return
        now = int(time.time())
        try:
            async with self._db.connect() as conn:
                # Try INSERT first; ON CONFLICT update mutable fields.
                await conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, intent_id, token_id, condition_id,
                        side, price, size, filled_size, state,
                        paper, strategy, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        state = excluded.state,
                        filled_size = MAX(orders.filled_size, excluded.filled_size),
                        updated_at = excluded.updated_at
                    """,
                    (
                        order_id, intent_id, token_id, condition_id,
                        side, price, size, filled_size, state,
                        paper, strategy, now, now,
                    ),
                )
                await conn.commit()
        except Exception:
            logger.warning(
                "orders_repo: upsert failed for order_id=%s state=%s",
                order_id, state, exc_info=True,
            )

    async def fetch_by_order_id(self, order_id: str) -> dict[str, Any] | None:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                "SELECT order_id, intent_id, token_id, condition_id, "
                "       side, price, size, filled_size, state, "
                "       paper, strategy, created_at, updated_at "
                "FROM orders WHERE order_id = ?",
                (order_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "order_id": row[0], "intent_id": row[1], "token_id": row[2],
            "condition_id": row[3], "side": row[4], "price": row[5],
            "size": row[6], "filled_size": row[7], "state": row[8],
            "paper": row[9], "strategy": row[10],
            "created_at": row[11], "updated_at": row[12],
        }
