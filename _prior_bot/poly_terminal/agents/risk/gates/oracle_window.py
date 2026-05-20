"""Gate 12: oracle / UMA dispute window proximity.

If a market is within `pre_oracle_seconds` of resolution, refuse new entries
— the price diverges sharply from fair value during the dispute window.
This is a stricter version of `time_left` that only kicks in for the
near-resolution edge case.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from poly_terminal.shared.typed_reject import Reject

NowReader = Callable[[], float]


class OracleWindowGate:
    def __init__(
        self,
        pre_oracle_seconds: int,
        now_reader: NowReader | None = None,
    ) -> None:
        self._pre = pre_oracle_seconds
        self._now = now_reader or (lambda: datetime.now(timezone.utc).timestamp())

    async def __call__(self, intent: object) -> Reject | None:
        end_iso = getattr(intent, "end_date_iso", None)
        if not end_iso:
            return None  # other gate handles missing end_date
        try:
            end_dt = datetime.fromisoformat(str(end_iso).replace("Z", "+00:00"))
        except ValueError:
            return None
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        seconds_left = end_dt.timestamp() - self._now()
        if 0 <= seconds_left < self._pre:
            return Reject(
                code="oracle_window_proximity",
                detail=f"{int(seconds_left)}s < {self._pre}s",
            )
        return None
