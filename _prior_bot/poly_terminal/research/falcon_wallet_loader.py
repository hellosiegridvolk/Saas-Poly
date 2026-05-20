"""Loader for wallet performance statistics via Falcon.

The agent_id MUST come from `FALCON_WALLET_STATS_AGENT_ID`; we deliberately
do not bake in a default because the wallet-stats endpoint isn't a stable
core integration and the ID rotates. If unset, raise with a pointer to the
upstream reference.
"""

from __future__ import annotations

import os
from typing import Any

from poly_terminal.research.falcon_client import FalconClient


def _resolve_wallet_agent_id() -> int:
    raw = os.environ.get("FALCON_WALLET_STATS_AGENT_ID")
    if raw is None or raw == "":
        raise RuntimeError(
            "FALCON_WALLET_STATS_AGENT_ID not set — look it up in the Falcon "
            "API reference at https://narrative.agent.heisenberg.so"
        )
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(
            "FALCON_WALLET_STATS_AGENT_ID must be an integer agent id "
            "(see https://narrative.agent.heisenberg.so)"
        ) from exc


class FalconWalletLoader:
    """Wallet performance lookup via Falcon."""

    def __init__(self, client: FalconClient) -> None:
        self._client = client
        self._agent_id = _resolve_wallet_agent_id()

    async def fetch_wallet_stats(
        self, wallet: str, metrics: list[str], timeframe: str = "90d"
    ) -> dict[str, Any]:
        """Fetch a single-wallet stats blob.

        `metrics` is a list of metric names (e.g. ['winrate', 'pnl_total',
        'trade_count']) — the actual menu is endpoint-defined.
        """
        params: dict[str, Any] = {
            "wallet": wallet,
            "metrics": list(metrics),
            "timeframe": timeframe,
        }
        rows = await self._client.query(self._agent_id, params, limit=1, offset=0)
        if not rows:
            return {}
        # Most parameterized agents return one row per wallet.
        first = rows[0]
        if isinstance(first, dict):
            return first
        return {}

    async def rank_wallets(
        self,
        wallets: list[str],
        min_trades: int = 100,
        min_winrate: float = 0.55,
    ) -> list[dict[str, Any]]:
        """Fetch stats for many wallets, filter by floors, sort by winrate desc.

        Returned rows are augmented with the wallet address under 'wallet' if
        the upstream payload doesn't already include it.
        """
        ranked: list[dict[str, Any]] = []
        metrics = ["winrate", "pnl_total", "trade_count"]
        for w in wallets:
            stats = await self.fetch_wallet_stats(w, metrics)
            if not stats:
                continue
            stats.setdefault("wallet", w)

            try:
                trades = int(stats.get("trade_count") or 0)
                winrate = float(stats.get("winrate") or 0.0)
            except (TypeError, ValueError):
                continue

            if trades < min_trades:
                continue
            if winrate < min_winrate:
                continue
            ranked.append(stats)

        ranked.sort(key=lambda r: float(r.get("winrate") or 0.0), reverse=True)
        return ranked
