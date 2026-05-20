"""Polymarket Data API client.

Endpoints used by v3:
  GET /leaderboard?period=&order_by=&limit=    — wallet ranking source
  GET /positions?address=                      — per-wallet positions

Verified working endpoint base via `polymarket-cli` and the polymarket-apis
PyPI wrapper. Wraps every call in `@latency_tracked(budget)`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Literal

import aiohttp

from poly_terminal.data.data_api.leaderboard import LeaderboardEntry
from poly_terminal.data.latency_budget import LatencyBudget, latency_tracked

logger = logging.getLogger(__name__)

Period = Literal["1d", "7d", "30d", "all"]
OrderBy = Literal["pnl", "volume"]

_VALID_PERIODS: frozenset[str] = frozenset({"1d", "7d", "30d", "all"})
_VALID_ORDER_BY: frozenset[str] = frozenset({"pnl", "volume"})

# Connection-level transients that justify a retry-with-backoff. Server
# 4xx / 5xx responses are explicitly NOT in this list — those need
# different handling (auth refresh, rate-limit pacing) that backoff
# alone won't solve.
_RETRYABLE_EXCEPTIONS = (
    ConnectionResetError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientOSError,
    aiohttp.ClientPayloadError,
    asyncio.TimeoutError,
)


class DataApiClient:
    """Async client for `https://data-api.polymarket.com`."""

    def __init__(
        self,
        base_url: str = "https://data-api.polymarket.com",
        session: aiohttp.ClientSession | None = None,
        budget: LatencyBudget | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = session
        self._owned_session = session is None
        self._budget = budget or LatencyBudget(
            name="data_api", ceiling_ms=2000, window_size=50
        )

    async def __aenter__(self) -> "DataApiClient":
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owned_session = True
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owned_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owned_session = True
        return self._session

    async def fetch_leaderboard(
        self,
        period: Period = "30d",
        order_by: OrderBy = "pnl",
        limit: int = 100,
    ) -> list[LeaderboardEntry]:
        if period not in _VALID_PERIODS:
            msg = f"period must be one of {sorted(_VALID_PERIODS)}, got {period!r}"
            raise ValueError(msg)
        if order_by not in _VALID_ORDER_BY:
            msg = f"order_by must be one of {sorted(_VALID_ORDER_BY)}, got {order_by!r}"
            raise ValueError(msg)

        @latency_tracked(self._budget)
        async def _call() -> list[LeaderboardEntry]:
            session = self._ensure_session()
            params = {"period": period, "order_by": order_by, "limit": limit}
            async with session.get(
                f"{self.base_url}/leaderboard", params=params
            ) as resp:
                if resp.status != 200:
                    return []
                items: list[dict[str, Any]] = await resp.json()
            out: list[LeaderboardEntry] = []
            for item in items:
                addr = str(item.get("address", "")).lower()
                if not addr:
                    continue
                try:
                    out.append(
                        LeaderboardEntry(
                            address=addr,
                            pnl=float(item.get("pnl", 0)),
                            volume=float(item.get("volume", 0)),
                            trades=int(item.get("trades", 0)),
                            raw=item,
                        )
                    )
                except (TypeError, ValueError):
                    continue
            return out

        return await _call()

    async def fetch_positions(self, wallet: str) -> list[dict[str, Any]]:
        @latency_tracked(self._budget)
        async def _call() -> list[dict[str, Any]]:
            session = self._ensure_session()
            # Param name is `user` (verified live 2026-05-02). Passing
            # `address` returns HTTP 400 — the v3 wrapper used the
            # wrong key, so PositionImporter saw an empty response.
            params = {"user": wallet.lower()}
            async with session.get(
                f"{self.base_url}/positions", params=params
            ) as resp:
                if resp.status != 200:
                    return []
                items: list[dict[str, Any]] = await resp.json()
            return items

        return await _call()

    async def fetch_user_trades(
        self, wallet: str, limit: int = 20
    ) -> list[dict[str, Any]]:
        """`/trades?user=<wallet>&limit=N` — newest first.

        2026-05-07 PHASE 18 — used by ExecutionAgent's
        matching-in-flight reconcile path. When a SELL POST returns
        the 'sum of matched orders' error format (engine matched a
        previous attempt mid-retry), poll this to confirm the actual
        on-chain fill price and restate the closed position.

        Each item carries: side ('BUY'|'SELL'), asset (token_id),
        conditionId (market_id), size (shares), price, timestamp
        (unix s), transactionHash. The proxyWallet is always the
        queried wallet, so no client-side filter on `user` is needed.
        """

        @latency_tracked(self._budget)
        async def _call() -> list[dict[str, Any]]:
            session = self._ensure_session()
            params = {"user": wallet.lower(), "limit": limit}
            async with session.get(
                f"{self.base_url}/trades", params=params
            ) as resp:
                if resp.status != 200:
                    return []
                items: list[dict[str, Any]] = await resp.json()
            return items

        return await _call()

    async def fetch_activity(
        self, wallet: str, limit: int = 20
    ) -> list[dict[str, Any]]:
        """`/activity?user=<wallet>&limit=N` — newest items first.

        Used by WalletActivityPoller to detect new TRADE events without
        depending on the User WebSocket (which only streams events for
        the authenticated user, not arbitrary tracked wallets).

        Polymarket's edge occasionally drops keep-alive connections
        mid-request — surfaces as ConnectionResetError or TLS handshake
        timeout. We retry transient connection errors twice with
        exponential backoff (0.3s, 0.9s) before letting the exception
        propagate. HTTP 4xx / 5xx responses are NOT retried (they're
        either auth issues or rate limits — backoff doesn't help).
        """

        @latency_tracked(self._budget)
        async def _call() -> list[dict[str, Any]]:
            session = self._ensure_session()
            params = {"user": wallet.lower(), "limit": limit}
            attempt = 0
            backoff = (0.3, 0.9)
            while True:
                try:
                    async with session.get(
                        f"{self.base_url}/activity", params=params
                    ) as resp:
                        if resp.status != 200:
                            return []
                        items: list[dict[str, Any]] = await resp.json()
                    return items
                except _RETRYABLE_EXCEPTIONS as exc:
                    if attempt >= len(backoff):
                        logger.warning(
                            "fetch_activity: giving up on %s after %d retries (%s)",
                            wallet, attempt, type(exc).__name__,
                        )
                        raise
                    delay = backoff[attempt]
                    logger.debug(
                        "fetch_activity: transient %s on %s — retry %d in %.1fs",
                        type(exc).__name__, wallet, attempt + 1, delay,
                    )
                    await asyncio.sleep(delay)
                    attempt += 1

        return await _call()

    @property
    def latency(self) -> LatencyBudget:
        return self._budget
