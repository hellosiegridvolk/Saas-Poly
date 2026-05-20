"""Slippage + fee columns on live_orders.

slippage_usd = (limit_price - avg_fill_price) * filled_qty   for BUY
             = (avg_fill_price - limit_price) * filled_qty   for SELL
Positive = favorable (filled better than limit).

fee_usd captures the explicit Polymarket fee charged on the fill. As of
this writing Polymarket charges 0% maker/taker on Polygon; this column
exists so the bot is ready when that changes.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    "ALTER TABLE live_orders ADD COLUMN slippage_usd REAL NOT NULL DEFAULT 0",
    "ALTER TABLE live_orders ADD COLUMN fee_usd REAL NOT NULL DEFAULT 0",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
