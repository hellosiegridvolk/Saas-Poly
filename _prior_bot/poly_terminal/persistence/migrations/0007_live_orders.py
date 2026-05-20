"""live_orders — record signed (and optionally submitted) live orders.

In LIVE_DRY mode we sign orders via py-clob-client but never POST them; in
LIVE we sign + submit. Both paths land here for audit + reconciliation
(Phase 3 ties incoming WS fills back to rows by client_order_id).

Status lifecycle:
    signed → submitted → (filled | partial | cancelled | rejected | expired)
LIVE_DRY orders never advance past `signed` — they are the audit trail
proving the order-construction path works end-to-end without spending
money.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    """
    CREATE TABLE live_orders (
        order_pk          INTEGER PRIMARY KEY AUTOINCREMENT,
        intent_id         TEXT NOT NULL,
        strategy          TEXT NOT NULL,
        market_id         TEXT NOT NULL,
        token_id          TEXT NOT NULL,
        side              TEXT NOT NULL,
        limit_price       REAL NOT NULL,
        size_usd          REAL NOT NULL,
        shares            REAL NOT NULL,
        order_type        TEXT NOT NULL DEFAULT 'GTC',
        mode              TEXT NOT NULL,        -- LIVE_DRY | LIVE
        client_order_id   TEXT NOT NULL UNIQUE, -- our deterministic ID for fill reconciliation
        signed_order_json TEXT NOT NULL,        -- full SignedOrder serialized (audit)
        order_response_json TEXT,               -- Polymarket POST /order response
        status            TEXT NOT NULL DEFAULT 'signed',
        signed_at         INTEGER NOT NULL,
        submitted_at      INTEGER,
        filled_qty        REAL NOT NULL DEFAULT 0,
        avg_fill_price    REAL,
        last_updated      INTEGER NOT NULL
    )
    """,
    "CREATE INDEX idx_live_orders_status ON live_orders(status)",
    "CREATE INDEX idx_live_orders_client_id ON live_orders(client_order_id)",
    "CREATE INDEX idx_live_orders_intent ON live_orders(intent_id)",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
