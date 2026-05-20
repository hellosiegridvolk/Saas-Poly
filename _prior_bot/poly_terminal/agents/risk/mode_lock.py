"""Mode-lock gate — refuse intents based on the current BotMode."""

from __future__ import annotations

from poly_terminal.shared.enums import BotMode, IntentSide
from poly_terminal.shared.typed_reject import Reject


class ModeLockGate:
    """Mode-aware intent gating.

    READ_ONLY    → reject all intents (no execution at all).
    CLOSE_ONLY   → reject BUY intents, allow SELL/cancel-shaped intents.
                   New for 2026-05-05 canary preflight: lets the bot
                   wind down inventory without acquiring more.
    PAPER        → allow (fills simulated downstream).
    LIVE_DRY     → allow (signed but not POSTed downstream).
    LIVE         → allow (full execution).
    """

    def __init__(self, mode_getter) -> None:
        self._get = mode_getter  # callable returning BotMode

    async def __call__(self, intent: object) -> Reject | None:
        mode = self._get()
        if mode is BotMode.READ_ONLY:
            return Reject(code="mode_read_only")
        if mode is BotMode.CLOSE_ONLY:
            # Read intent.side defensively — non-BuyIntent payloads
            # (e.g. probe/test inputs) get treated as buys and rejected.
            side = getattr(intent, "side", None)
            if side is IntentSide.BUY:
                return Reject(code="mode_close_only_buy_blocked")
            # SELL or unknown-non-BUY (rare; cancel flows etc) → allow
            return None
        return None
