"""Initial schema for v3 — minimal core tables.

Subsequent migrations add wallet_scores, gate_metrics, latency_budget,
config_fingerprint. See docs/03_REPO_BLUEPRINT.md §10.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    # ── markets ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS markets (
        condition_id  TEXT PRIMARY KEY,
        slug          TEXT,
        question      TEXT NOT NULL,
        end_date_iso  TEXT,
        volume_24hr   REAL DEFAULT 0,
        liquidity_usd REAL DEFAULT 0,
        active        INTEGER DEFAULT 1,
        closed        INTEGER DEFAULT 0,
        enable_orderbook INTEGER DEFAULT 1,
        tokens        TEXT DEFAULT '{}',   -- JSON: token_id → side label
        tick_sizes    TEXT DEFAULT '{}',   -- JSON: token_id → tick float
        updated_at    INTEGER NOT NULL     -- unix seconds
    )
    """,
    # ── orders ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id      TEXT PRIMARY KEY,
        intent_id     TEXT NOT NULL,
        token_id      TEXT NOT NULL,
        condition_id  TEXT,
        side          TEXT NOT NULL,
        price         REAL NOT NULL,
        size          REAL NOT NULL,
        filled_size   REAL DEFAULT 0,
        state         TEXT NOT NULL,         -- LIVE|MATCHED|CANCELLED|EXPIRED
        paper         INTEGER DEFAULT 1,
        strategy      TEXT,
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL
    )
    """,
    # ── paper_fills (signal_at + filled_at NOT NULL — Bug #4 fix) ──
    """
    CREATE TABLE IF NOT EXISTS paper_fills (
        fill_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        intent_id     TEXT NOT NULL,
        strategy      TEXT NOT NULL,
        market_id     TEXT NOT NULL,
        token_id      TEXT NOT NULL,
        side          TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
        qty           REAL NOT NULL CHECK (qty > 0),
        signal_price  REAL NOT NULL,
        fill_price    REAL NOT NULL,
        signal_at     INTEGER NOT NULL,
        filled_at     INTEGER NOT NULL,
        realized_pnl  REAL,
        outcome       TEXT,                  -- TP|SL|TIME|WHALE_OUT|MANUAL
        CHECK (signal_at IS NOT NULL),
        CHECK (filled_at IS NOT NULL),
        CHECK (filled_at >= signal_at)
    )
    """,
    # ── positions ────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS positions (
        position_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        market_id       TEXT NOT NULL,
        token_id        TEXT NOT NULL,
        side            TEXT NOT NULL,
        entry_price     REAL NOT NULL,
        shares          REAL NOT NULL CHECK (shares > 0),
        cost_basis_usd  REAL NOT NULL,
        entry_intent_id TEXT NOT NULL,
        entry_ts        INTEGER NOT NULL,
        closed_ts       INTEGER,
        exit_price      REAL,
        realized_pnl    REAL,
        outcome         TEXT
    )
    """,
    # ── tick_history (bounded ring; pruned by repo) ─────────────────
    """
    CREATE TABLE IF NOT EXISTS tick_history (
        tick_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id    TEXT NOT NULL,
        best_bid    REAL,
        best_ask    REAL,
        midpoint    REAL,
        last_trade  REAL,
        ts          INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tick_history_token_ts ON tick_history(token_id, ts)",
    "CREATE INDEX IF NOT EXISTS idx_paper_fills_strategy_filled ON paper_fills(strategy, filled_at)",
    "CREATE INDEX IF NOT EXISTS idx_orders_intent ON orders(intent_id)",
    "CREATE INDEX IF NOT EXISTS idx_positions_market ON positions(market_id, closed_ts)",
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
