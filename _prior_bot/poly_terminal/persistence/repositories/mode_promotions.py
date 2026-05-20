"""Repository for mode_promotions audit rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from poly_terminal.persistence.db import Database


@dataclass(frozen=True)
class ModePromotion:
    promotion_id: int
    from_mode: str
    to_mode: str
    ts: int
    signed_by: str
    fingerprint: str | None
    reason: str | None


class ModePromotionsRepo:
    def __init__(self, db: "Database") -> None:
        self._db = db

    async def insert(
        self,
        from_mode: str,
        to_mode: str,
        ts: int,
        signed_by: str,
        fingerprint: str | None,
        reason: str | None,
    ) -> int:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO mode_promotions
                  (from_mode, to_mode, ts, signed_by, fingerprint, reason)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (from_mode, to_mode, ts, signed_by, fingerprint, reason),
            )
            await conn.commit()
            return int(cur.lastrowid or 0)

    async def latest(self) -> ModePromotion | None:
        async with self._db.connect() as conn:
            cur = await conn.execute(
                """
                SELECT promotion_id, from_mode, to_mode, ts,
                       signed_by, fingerprint, reason
                FROM mode_promotions
                ORDER BY ts DESC, promotion_id DESC
                LIMIT 1
                """
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return ModePromotion(
            promotion_id=int(row[0]),
            from_mode=str(row[1]),
            to_mode=str(row[2]),
            ts=int(row[3]),
            signed_by=str(row[4]),
            fingerprint=str(row[5]) if row[5] is not None else None,
            reason=str(row[6]) if row[6] is not None else None,
        )
