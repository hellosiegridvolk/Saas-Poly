"""Loader for closed Polymarket markets via the Falcon semantic-retrieve API.

Pulls a paginated list of closed markets that meet a minimum-volume threshold
and upserts them into the `research_markets` table for downstream backtesting.
The agent_id defaults to 574 (Polymarket markets agent) but is overridable
via the `FALCON_MARKETS_AGENT_ID` env var so the integration can rotate
without code changes.
"""

from __future__ import annotations

import json
import os
from typing import Any

from poly_terminal.research.falcon_client import FalconClient

_DEFAULT_MARKETS_AGENT_ID = 574


def _resolve_markets_agent_id() -> int:
    raw = os.environ.get("FALCON_MARKETS_AGENT_ID")
    if raw is None or raw == "":
        return _DEFAULT_MARKETS_AGENT_ID
    try:
        return int(raw)
    except ValueError:
        return _DEFAULT_MARKETS_AGENT_ID


class FalconMarketLoader:
    """Pulls closed markets from Falcon and persists them to research_markets."""

    def __init__(self, client: FalconClient) -> None:
        self._client = client
        self._agent_id = _resolve_markets_agent_id()

    async def fetch_closed_markets(
        self, min_volume: float = 100, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Fetch up to `limit` closed markets with volume >= `min_volume`.

        Walks the Falcon paginator. Returns a flat list of raw market dicts.
        """
        params = {
            "closed": True,
            "min_volume": float(min_volume),
        }
        out: list[dict[str, Any]] = []
        async for row in self._client.query_all(self._agent_id, params, page_size=100):
            out.append(row)
            if len(out) >= limit:
                break
        return out

    async def upsert_to_db(self, db, markets: list[dict[str, Any]]) -> int:
        """Upsert markets into `research_markets`. Returns count written."""
        if not markets:
            return 0

        rows = []
        for m in markets:
            slug = m.get("market_slug") or m.get("slug")
            if not slug:
                continue
            rows.append(
                (
                    slug,
                    m.get("condition_id"),
                    m.get("question"),
                    int(bool(m.get("closed", False))),
                    m.get("winning_outcome"),
                    m.get("start_ts") or m.get("start_date"),
                    m.get("end_ts") or m.get("end_date"),
                    float(m.get("volume_total") or m.get("volume") or 0.0),
                    json.dumps(m, separators=(",", ":")),
                )
            )

        if not rows:
            return 0

        async with db.connect() as conn:
            await conn.executemany(
                """
                INSERT INTO research_markets
                  (market_slug, condition_id, question, closed, winning_outcome,
                   start_ts, end_ts, volume_total, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_slug) DO UPDATE SET
                    condition_id = excluded.condition_id,
                    question = excluded.question,
                    closed = excluded.closed,
                    winning_outcome = excluded.winning_outcome,
                    start_ts = excluded.start_ts,
                    end_ts = excluded.end_ts,
                    volume_total = excluded.volume_total,
                    raw_json = excluded.raw_json
                """,
                rows,
            )
            await conn.commit()

        return len(rows)
