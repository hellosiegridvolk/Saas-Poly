"""Per-wallet positions / PnL."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from poly_terminal.data.data_api.client import DataApiClient


class PositionsClient:
    """Thin facade over `DataApiClient.fetch_positions` that normalizes the
    raw API shape into the v3 internal representation.
    """

    def __init__(self, client: "DataApiClient") -> None:
        self._client = client

    async def fetch_for_wallet(self, wallet: str) -> list[dict[str, Any]]:
        raw = await self._client.fetch_positions(wallet)
        out: list[dict[str, Any]] = []
        for item in raw:
            try:
                # Live wire shape (verified 2026-05-02 against
                # data-api.polymarket.com/positions?user=…):
                #   asset       → token_id (the CTF outcome token)
                #   conditionId → market_id
                #   size        → shares held
                #   avgPrice    → entry cost per share
                #   curPrice    → live mid price
                #   cashPnl     → realized + unrealized PnL in USD
                #   redeemable  → market resolved + we hold winning side
                # Side defaults to BUY since Polymarket binary outcomes
                # only allow long positions in YES/NO tokens.
                out.append(
                    {
                        "market_id": str(item.get("conditionId", "")),
                        "token_id": str(item.get("asset", "")),
                        "side": str(item.get("side", "BUY")).upper(),
                        "size": float(item.get("size", 0)),
                        "avg_price": float(item.get("avgPrice", 0)),
                        "current_price": float(item.get("curPrice", 0)),
                        "pnl": float(item.get("cashPnl", 0)),
                        "redeemable": bool(item.get("redeemable", False)),
                    }
                )
            except (TypeError, ValueError):
                continue
        return out
