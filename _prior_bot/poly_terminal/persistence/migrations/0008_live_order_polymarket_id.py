"""Add polymarket_order_id to live_orders so the fill reconciler can
match incoming User-WS TRADE/ORDER events back to the row we POSTed.

The User WebSocket publishes EVT_ORDER_FILLED / EVT_ORDER_CANCELLED with
`order_id` set to Polymarket's hash. We store that hash on the
live_orders row when post_order succeeds; the reconciler then looks up
by it on the way in.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    "ALTER TABLE live_orders ADD COLUMN polymarket_order_id TEXT",
    "CREATE INDEX idx_live_orders_polymarket_id ON live_orders(polymarket_order_id)",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
