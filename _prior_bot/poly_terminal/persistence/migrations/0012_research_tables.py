"""Add research/backtest tables.

2026-05-04 — Phase 2 backtesting prerequisite. Adds four offline-only
tables that the live trading hot-path NEVER reads or writes:

  - research_markets          : closed Polymarket markets with metadata
  - research_trades           : historical trade prints per market
  - research_orderbook_snapshots : recorded L2 ladders for fill simulation
  - backtest_runs             : per-signal replay outcomes

Plus five indexes that the loader/replay code depends on for its hot reads
(closest-book-by-token-and-ts, trades-by-wallet, etc.).

All DDL is `IF NOT EXISTS` so re-running the migration is a no-op. The
shape is additive — no existing tables are touched.
"""

from __future__ import annotations

import aiosqlite

_DDL: list[str] = [
    # ── research_markets ────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS research_markets (
        market_slug      TEXT PRIMARY KEY,
        condition_id     TEXT,
        question         TEXT,
        closed           INTEGER DEFAULT 0,
        winning_outcome  TEXT,
        start_ts         INTEGER,
        end_ts           INTEGER,
        volume_total     REAL,
        raw_json         TEXT
    )
    """,
    # ── research_trades ─────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS research_trades (
        id            TEXT PRIMARY KEY,
        market_slug   TEXT,
        condition_id  TEXT,
        token_id      TEXT,
        side          TEXT,
        price         REAL,
        size          REAL,
        ts            INTEGER,
        wallet        TEXT,
        raw_json      TEXT
    )
    """,
    # ── research_orderbook_snapshots ───────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS research_orderbook_snapshots (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id   TEXT NOT NULL,
        ts         INTEGER NOT NULL,
        best_bid   REAL,
        best_ask   REAL,
        spread     REAL,
        bids_json  TEXT,
        asks_json  TEXT,
        source     TEXT
    )
    """,
    # ── backtest_runs ───────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS backtest_runs (
        run_id          TEXT NOT NULL,
        intent_id       TEXT NOT NULL,
        strategy        TEXT,
        token_id        TEXT,
        signal_ts       INTEGER,
        signal_price    REAL,
        target_usd      REAL,
        fill_status     TEXT,
        filled_shares   REAL,
        avg_fill_price  REAL,
        book_age_s      INTEGER,
        PRIMARY KEY (run_id, intent_id)
    )
    """,
    # ── indexes ─────────────────────────────────────────────────────
    """
    CREATE INDEX IF NOT EXISTS idx_research_markets_closed
        ON research_markets (closed, end_ts)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_research_trades_token_ts
        ON research_trades (token_id, ts)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_research_trades_wallet
        ON research_trades (wallet, ts)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_research_book_token_ts
        ON research_orderbook_snapshots (token_id, ts)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_backtest_strategy
        ON backtest_runs (run_id, strategy)
    """,
]


async def apply(conn: aiosqlite.Connection) -> None:
    for stmt in _DDL:
        await conn.execute(stmt)
