"""Loader for historical Polymarket trades for a given market via Falcon.

The agent_id MUST come from `FALCON_TRADES_AGENT_ID`. If unset, raise with a
pointer to the upstream reference rather than guessing.
"""

from __future__ import annotations

import json
import os
from typing import Any

from poly_terminal.research.falcon_client import FalconClient


def _resolve_trades_agent_id() -> int:
    raw = os.environ.get("FALCON_TRADES_AGENT_ID")
    if raw is None or raw == "":
        raise RuntimeError(
            "FALCON_TRADES_AGENT_ID not set — look it up in the Falcon API "
            "reference at https://narrative.agent.heisenberg.so"
        )
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(
            "FALCON_TRADES_AGENT_ID must be an integer agent id "
            "(see https://narrative.agent.heisenberg.so)"
        ) from exc


class FalconTradesLoader:
    """Pulls historical trade prints for a market from Falcon."""

    def __init__(self, client: FalconClient) -> None:
        self._client = client
        self._agent_id = _resolve_trades_agent_id()

    async def fetch_trades_for_market(
        self, market_slug: str, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Fetch up to `limit` trades for a single market slug."""
        params = {"market_slug": market_slug}
        out: list[dict[str, Any]] = []
        async for row in self._client.query_all(self._agent_id, params, page_size=200):
            out.append(row)
            if len(out) >= limit:
                break
        return out

    async def upsert_to_db(
        self,
        db,
        trades: list[dict[str, Any]],
        market_slug: str,
        condition_id: str | None,
    ) -> int:
        """Upsert trades into `research_trades`. Returns count written.

        Trades use the Falcon-provided `id` as primary key. Upsert semantics:
        re-running a load against the same trade ids replaces metadata but
        does not duplicate rows.
        """
        if not trades:
            return 0

        rows = []
        for t in trades:
            tid = t.get("id") or t.get("trade_id")
            if tid is None:
                continue
            rows.append(
                (
                    str(tid),
                    market_slug,
                    condition_id,
                    t.get("token_id") or t.get("asset_id"),
                    (t.get("side") or "").upper() or None,
                    _safe_float(t.get("price")),
                    _safe_float(t.get("size") or t.get("shares")),
                    _safe_int(t.get("ts") or t.get("timestamp")),
                    t.get("wallet") or t.get("maker") or t.get("trader"),
                    json.dumps(t, separators=(",", ":")),
                )
            )

        if not rows:
            return 0

        async with db.connect() as conn:
            await conn.executemany(
                """
                INSERT INTO research_trades
                  (id, market_slug, condition_id, token_id, side, price, size,
                   ts, wallet, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    market_slug = excluded.market_slug,
                    condition_id = excluded.condition_id,
                    token_id = excluded.token_id,
                    side = excluded.side,
                    price = excluded.price,
                    size = excluded.size,
                    ts = excluded.ts,
                    wallet = excluded.wallet,
                    raw_json = excluded.raw_json
                """,
                rows,
            )
            await conn.commit()

        return len(rows)


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None
