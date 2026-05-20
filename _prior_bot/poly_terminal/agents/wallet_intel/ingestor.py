"""Subscribes to EVT_WALLET_FILL / EVT_WALLET_REDEEM and writes wallet_history.

State model:
  - BUY fill (TRADE.BUY)  → open a wallet_history row with closed_at NULL.
  - SELL fill (TRADE.SELL) → match the most recent open row by token_id,
    set closed_at + exit_price + pnl_usd.
  - REDEEM event (binary market resolved while the whale held YES) →
    close ALL open BUY rows for (wallet, market_id=conditionId). Polymarket
    /activity emits one REDEEM per resolved market with the USDC payout in
    `usdcSize`; the contract pays winners directly so no SELL ever fires.
    For losing positions, no event is ever emitted — they need the
    time-decay sweep below.
  - sweep_stale_buys(max_age_seconds): closes any open BUY older than the
    cutoff at exit_price=0 (presumed loss). This is the only way losing
    positions get reflected in win_rate, since /activity is silent on them.
"""

from __future__ import annotations

import logging
from typing import Any

import aiosqlite

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_WALLET_FILL, EVT_WALLET_REDEEM
from poly_terminal.persistence.db import Database
from poly_terminal.persistence.repositories.wallets import (
    WalletHistoryRow,
    WalletsRepo,
)

logger = logging.getLogger(__name__)


def _normalize_ts(ts: int) -> int:
    """Polymarket /activity occasionally delivers millisecond epochs.
    Valid unix-seconds won't reach 1e12 for millennia, so a value at or
    above that is ms — divide once. Idempotent for second-scale input."""
    return ts // 1000 if ts >= 1_000_000_000_000 else ts


class WalletFillIngestor:
    def __init__(
        self,
        bus: EventBus,
        repo: WalletsRepo,
        tracked: set[str],
        db: Database | None = None,
    ) -> None:
        self._bus = bus
        self._repo = repo
        self._tracked = {w.lower() for w in tracked}
        # Reach back into the DB for sell-side reconciliation. The repo doesn't
        # expose update operations because they're only needed here.
        self._db = db or repo._db  # type: ignore[attr-defined]
        self._started = False

    def set_tracked(self, wallets: set[str]) -> None:
        self._tracked = {w.lower() for w in wallets}

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_WALLET_FILL, self._on_fill)
        self._bus.subscribe(EVT_WALLET_REDEEM, self._on_redeem)
        self._started = True

    async def _on_fill(self, _event: str, payload: Any) -> None:
        wallet = str(payload.get("wallet", "")).lower()
        if wallet not in self._tracked:
            return
        side = str(payload.get("side", "")).upper()
        try:
            size = float(payload.get("size", 0))
            price = float(payload.get("price", 0))
            ts = _normalize_ts(int(payload.get("ts", 0)))
        except (TypeError, ValueError):
            return
        if size <= 0 or price <= 0:
            return
        token_id = str(payload.get("token_id", ""))
        market_id = str(payload.get("market_id", ""))

        if side == "BUY":
            await self._open_buy(wallet, market_id, token_id, size, price, ts)
        elif side == "SELL":
            await self._close_with_sell(wallet, token_id, size, price, ts)

    async def _open_buy(
        self,
        wallet: str,
        market_id: str,
        token_id: str,
        size: float,
        price: float,
        ts: int,
    ) -> None:
        await self._repo.insert_history(
            WalletHistoryRow(
                wallet=wallet,
                market_id=market_id,
                token_id=token_id,
                side="BUY",
                size_usd=size * price,
                avg_price=price,
                exit_price=None,
                pnl_usd=None,
                opened_at=ts,
                closed_at=None,
            )
        )

    async def _close_with_sell(
        self,
        wallet: str,
        token_id: str,
        size: float,
        price: float,
        ts: int,
    ) -> None:
        async with self._db.connect() as conn:
            row = await self._fetch_open_buy(conn, wallet, token_id)
            if row is None:
                return
            history_id, opened_size_usd, avg_price = row
            shares = opened_size_usd / avg_price if avg_price > 0 else 0.0
            pnl_usd = (price - avg_price) * shares
            await conn.execute(
                """
                UPDATE wallet_history
                SET closed_at = ?, exit_price = ?, pnl_usd = ?
                WHERE history_id = ?
                """,
                (ts, price, pnl_usd, history_id),
            )
            await conn.commit()

    @staticmethod
    async def _fetch_open_buy(
        conn: aiosqlite.Connection, wallet: str, token_id: str
    ) -> tuple[int, float, float] | None:
        cur = await conn.execute(
            """
            SELECT history_id, size_usd, avg_price
            FROM wallet_history
            WHERE wallet = ?
              AND token_id = ?
              AND closed_at IS NULL
              AND side = 'BUY'
            ORDER BY opened_at DESC
            LIMIT 1
            """,
            (wallet, token_id),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0]), float(row[1]), float(row[2])

    # ── REDEEM handler ────────────────────────────────────────────────

    async def _on_redeem(self, _event: str, payload: Any) -> None:
        wallet = str(payload.get("wallet", "")).lower()
        if wallet not in self._tracked:
            return
        market_id = str(payload.get("market_id", ""))
        if not market_id:
            return
        try:
            payout_usd = float(payload.get("payout_usd", 0) or 0)
            ts = int(payload.get("ts", 0))
        except (TypeError, ValueError):
            return
        if payout_usd <= 0:
            return
        async with self._db.connect() as conn:
            rows = await self._fetch_open_buys_for_market(conn, wallet, market_id)
            if not rows:
                return
            total_cost = sum(r[1] for r in rows)
            for history_id, size_usd, _avg_price in rows:
                # Pro-rata the payout across same-market open buys. If the
                # whale only bought the winning side, this matches reality
                # exactly. If they hedged both sides, the loser is
                # over-credited and the winner under-credited — but the
                # net pnl across the pair is correct.
                share_of_payout = (
                    payout_usd * (size_usd / total_cost) if total_cost > 0 else 0.0
                )
                pnl_usd = share_of_payout - size_usd
                # exit_price encoded as payout_per_dollar — the scorer
                # only consumes pnl_usd, so the field is informational.
                exit_price = (
                    share_of_payout / size_usd if size_usd > 0 else 0.0
                )
                await conn.execute(
                    """
                    UPDATE wallet_history
                    SET closed_at = ?, exit_price = ?, pnl_usd = ?
                    WHERE history_id = ?
                    """,
                    (ts, exit_price, pnl_usd, history_id),
                )
            await conn.commit()

    @staticmethod
    async def _fetch_open_buys_for_market(
        conn: aiosqlite.Connection, wallet: str, market_id: str
    ) -> list[tuple[int, float, float]]:
        cur = await conn.execute(
            """
            SELECT history_id, size_usd, avg_price
            FROM wallet_history
            WHERE wallet = ?
              AND market_id = ?
              AND closed_at IS NULL
              AND side = 'BUY'
            ORDER BY opened_at ASC
            """,
            (wallet, market_id),
        )
        rows = await cur.fetchall()
        return [(int(r[0]), float(r[1]), float(r[2])) for r in rows]

    # ── Time-decay sweep for presumed losses ──────────────────────────

    async def sweep_stale_buys(
        self, *, max_age_seconds: int, now_ts: int
    ) -> int:
        """Close every still-open BUY older than max_age_seconds at
        exit_price=0 (presumed loss). Polymarket /activity is silent on
        losing positions — without this, losers leak forever and the
        Scorer's win_rate is dominated by winners only.

        Returns the count of rows closed. Idempotent: re-running it on a
        clean state is a no-op.
        """
        cutoff = now_ts - int(max_age_seconds)
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                UPDATE wallet_history
                SET closed_at = ?,
                    exit_price = 0.0,
                    pnl_usd = -size_usd
                WHERE closed_at IS NULL
                  AND side = 'BUY'
                  AND opened_at < ?
                """,
                (now_ts, cutoff),
            )
            await conn.commit()
            return int(cur.rowcount or 0)
