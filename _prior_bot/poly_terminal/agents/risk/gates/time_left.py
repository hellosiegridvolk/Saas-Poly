"""Gate 8: minimum time-to-resolution floor."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from poly_terminal.shared.typed_reject import Reject

NowReader = Callable[[], float]  # epoch seconds


class TimeLeftGate:
    def __init__(
        self,
        min_seconds: int,
        now_reader: NowReader | None = None,
    ) -> None:
        self._min = min_seconds
        self._now = now_reader or (lambda: datetime.now(timezone.utc).timestamp())

    async def __call__(self, intent: object) -> Reject | None:
        end_iso = getattr(intent, "end_date_iso", None)
        if not end_iso:
            return Reject(code="missing_end_date")
        try:
            # Accept both Z-suffix and +00:00 forms.
            end_dt = datetime.fromisoformat(str(end_iso).replace("Z", "+00:00"))
        except ValueError:
            return Reject(code="invalid_end_date", detail=str(end_iso))
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        seconds_left = end_dt.timestamp() - self._now()
        if seconds_left < self._min:
            return Reject(
                code="time_left_below_floor",
                detail=f"{int(seconds_left)}s < {self._min}s",
            )
        return None
