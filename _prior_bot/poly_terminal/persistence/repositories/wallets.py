"""Repository for wallet_scores + wallet_history."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from poly_terminal.persistence.db import Database


@dataclass(frozen=True)
class WalletScore:
    wallet: str
    win_rate: float
    avg_roi_pct: float
    trades_30d: int
    median_position_usd: float
    conviction_score: float
    last_updated: int
    category: str = "unknown"
    verified: bool = False


@dataclass(frozen=True)
class WalletHistoryRow:
    wallet: str
    market_id: str
    token_id: str
    side: str
    size_usd: float
    avg_price: float
    exit_price: float | None
    pnl_usd: float | None
    opened_at: int
    closed_at: int | None


class WalletsRepo:
    def __init__(self, db: "Database") -> None:
        self._db = db

    # ── wallet_history ────────────────────────────────────────────────

    async def insert_history(self, row: WalletHistoryRow) -> None:
        async with self._db.connect() as conn:
            await conn.execute(
                """
                INSERT INTO wallet_history
                  (wallet, market_id, token_id, side, size_usd, avg_price,
                   exit_price, pnl_usd, opened_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.wallet.lower(),
                    row.market_id,
                    row.token_id,
                    row.side,
                    row.size_usd,
                    row.avg_price,
                    row.exit_price,
                    row.pnl_usd,
                    row.opened_at,
                    row.closed_at,
                ),
            )
            await conn.commit()

    async def history_since(
        self, wallet: str, since_ts: int
    ) -> list[WalletHistoryRow]:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT wallet, market_id, token_id, side, size_usd, avg_price,
                       exit_price, pnl_usd, opened_at, closed_at
                FROM wallet_history
                WHERE wallet = ?
                  AND closed_at IS NOT NULL
                  AND closed_at >= ?
                ORDER BY closed_at DESC
                """,
                (wallet.lower(), since_ts),
            )
            rows = await cur.fetchall()
        return [
            WalletHistoryRow(
                wallet=str(r[0]),
                market_id=str(r[1]),
                token_id=str(r[2]),
                side=str(r[3]),
                size_usd=float(r[4]),
                avg_price=float(r[5]),
                exit_price=float(r[6]) if r[6] is not None else None,
                pnl_usd=float(r[7]) if r[7] is not None else None,
                opened_at=int(r[8]),
                closed_at=int(r[9]) if r[9] is not None else None,
            )
            for r in rows
        ]

    # ── wallet_scores ─────────────────────────────────────────────────

    async def upsert_score(self, score: WalletScore) -> None:
        async with self._db.connect() as conn:
            await conn.execute(
                """
                INSERT INTO wallet_scores
                  (wallet, win_rate, avg_roi_pct, trades_30d,
                   median_position_usd, conviction_score, last_updated,
                   category, verified)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                  win_rate = excluded.win_rate,
                  avg_roi_pct = excluded.avg_roi_pct,
                  trades_30d = excluded.trades_30d,
                  median_position_usd = excluded.median_position_usd,
                  conviction_score = excluded.conviction_score,
                  last_updated = excluded.last_updated,
                  category = excluded.category,
                  verified = MAX(wallet_scores.verified, excluded.verified)
                """,
                (
                    score.wallet.lower(),
                    score.win_rate,
                    score.avg_roi_pct,
                    score.trades_30d,
                    score.median_position_usd,
                    score.conviction_score,
                    score.last_updated,
                    score.category,
                    int(score.verified),
                ),
            )
            await conn.commit()

    async def fetch_score(self, wallet: str) -> WalletScore | None:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT wallet, win_rate, avg_roi_pct, trades_30d,
                       median_position_usd, conviction_score, last_updated,
                       category, verified
                FROM wallet_scores WHERE wallet = ?
                """,
                (wallet.lower(),),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return WalletScore(
            wallet=str(row[0]),
            win_rate=float(row[1]),
            avg_roi_pct=float(row[2]),
            trades_30d=int(row[3]),
            median_position_usd=float(row[4]),
            conviction_score=float(row[5]),
            last_updated=int(row[6]),
            category=str(row[7]) if row[7] is not None else "unknown",
            verified=bool(row[8]),
        )

    async def fetch_top(
        self, limit: int = 50, category: str | None = None
    ) -> list[WalletScore]:
        """Top wallets by conviction. If `category` is given, restrict to it."""
        async with self._db.connect() as conn:
            if category is None:
                cur = await conn.execute(
                    """
                    SELECT wallet, win_rate, avg_roi_pct, trades_30d,
                           median_position_usd, conviction_score, last_updated,
                           category, verified
                    FROM wallet_scores
                    ORDER BY conviction_score DESC, last_updated DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            else:
                cur = await conn.execute(
                    """
                    SELECT wallet, win_rate, avg_roi_pct, trades_30d,
                           median_position_usd, conviction_score, last_updated,
                           category, verified
                    FROM wallet_scores
                    WHERE category = ?
                    ORDER BY conviction_score DESC, last_updated DESC
                    LIMIT ?
                    """,
                    (category, limit),
                )
            rows = await cur.fetchall()
        return [
            WalletScore(
                wallet=str(r[0]),
                win_rate=float(r[1]),
                avg_roi_pct=float(r[2]),
                trades_30d=int(r[3]),
                median_position_usd=float(r[4]),
                conviction_score=float(r[5]),
                last_updated=int(r[6]),
                category=str(r[7]) if r[7] is not None else "unknown",
                verified=bool(r[8]),
            )
            for r in rows
        ]
