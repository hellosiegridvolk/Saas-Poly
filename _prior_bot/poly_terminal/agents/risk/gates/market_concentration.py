"""Gate: per-token (market+side) concentration cap.

Rejects intents that would push us over a max number of open
positions on the SAME outcome token. Without this, copy_trade can
stack 6+ positions in the same Bitcoin Up/Down market within a
minute when the followed wallets all fire on the same signal.
"""

from __future__ import annotations

from typing import Awaitable, Callable

from poly_terminal.shared.typed_reject import Reject

CountReader = Callable[[str], Awaitable[int]]  # token_id -> open count


class MarketConcentrationGate:
    def __init__(self, max_per_token: int, reader: CountReader) -> None:
        self._max = max_per_token
        self._read = reader

    async def __call__(self, intent: object) -> Reject | None:
        token_id = str(getattr(intent, "token_id", ""))
        if not token_id:
            return None
        count = await self._read(token_id)
        if count >= self._max:
            return Reject(
                code="market_concentration_cap_exceeded",
                detail=f"{count} open on token >= {self._max}",
            )
        return None
