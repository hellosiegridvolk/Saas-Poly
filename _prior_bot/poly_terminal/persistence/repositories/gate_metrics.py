"""Repository for `gate_metrics` upserts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from poly_terminal.persistence.db import Database


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class GateMetricsRepo:
    def __init__(self, db: "Database") -> None:
        self._db = db

    async def increment(
        self,
        gate_name: str,
        outcome: str,
        *,
        count: int = 1,
        day_bucket: str | None = None,
    ) -> None:
        bucket = day_bucket or _today_utc()
        async with self._db.connect() as conn:
            await conn.execute(
                """
                INSERT INTO gate_metrics (gate_name, day_bucket, outcome, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(gate_name, day_bucket, outcome)
                DO UPDATE SET count = count + excluded.count
                """,
                (gate_name, bucket, outcome, count),
            )
            await conn.commit()

    async def fetch_today(self) -> list[dict[str, object]]:
        bucket = _today_utc()
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT gate_name, outcome, count
                FROM gate_metrics
                WHERE day_bucket = ?
                ORDER BY gate_name, outcome
                """,
                (bucket,),
            )
            rows = await cur.fetchall()
        return [
            {"gate_name": r[0], "outcome": r[1], "count": int(r[2])}
            for r in rows
        ]
