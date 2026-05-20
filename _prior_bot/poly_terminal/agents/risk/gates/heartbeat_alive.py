"""Gate 16: CLOB authenticated session must have heartbeated recently."""

from __future__ import annotations

import time
from typing import Callable

from poly_terminal.shared.typed_reject import Reject

LastHeartbeatReader = Callable[[], float]  # monotonic seconds


class HeartbeatAliveGate:
    def __init__(
        self,
        max_age_seconds: float = 10.0,
        reader: LastHeartbeatReader | None = None,
    ) -> None:
        self._max_age = max_age_seconds
        self._read = reader or time.monotonic

    async def __call__(self, _intent: object) -> Reject | None:
        last = self._read()
        age = time.monotonic() - last
        if age > self._max_age:
            return Reject(
                code="heartbeat_stale",
                detail=f"{age:.1f}s > {self._max_age}s",
            )
        return None
