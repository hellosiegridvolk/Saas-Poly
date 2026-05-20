"""File-based kill switch.

Operator-friendly way to pause all trading without restarting the bot.
Touching `exports/paused.flag` forces the effective mode to READ_ONLY
on every gate evaluation, so any in-flight intent gets rejected at the
mode_lock gate. Removing the file resumes trading at the boot mode.

The check is per-intent, so the round-trip from pause → first reject
is at most one event-bus tick. Persistent across restarts (the flag
file outlives the bot process); operator must explicitly `rm` it.
"""

from __future__ import annotations

import os
from typing import Callable

from poly_terminal.shared.enums import BotMode

_DEFAULT_FLAG_PATH = "exports/paused.flag"


def is_paused(flag_path: str = _DEFAULT_FLAG_PATH) -> bool:
    return os.path.exists(flag_path)


def set_paused(reason: str = "", flag_path: str = _DEFAULT_FLAG_PATH) -> bool:
    """Programmatic kill-switch: write the flag from inside the bot.

    Used by SessionGuardAgent (and other internal halters) to stop
    trading when a session-level threshold trips. Idempotent — if the
    flag is already present, the existing contents are kept untouched
    so the operator's original "why" survives.

    Returns True if THIS call wrote the flag (first to halt), False if
    it was already paused.
    """
    if is_paused(flag_path):
        return False
    os.makedirs(os.path.dirname(flag_path) or ".", exist_ok=True)
    with open(flag_path, "w", encoding="utf-8") as fh:
        fh.write(reason or "auto-paused by session guard")
    return True


def make_pause_aware_mode_getter(
    real_mode_getter: Callable[[], BotMode],
    flag_path: str = _DEFAULT_FLAG_PATH,
) -> Callable[[], BotMode]:
    """Wraps a mode getter so it returns READ_ONLY when the flag file
    is present. Otherwise delegates to the real getter."""

    def _wrapped() -> BotMode:
        if is_paused(flag_path):
            return BotMode.READ_ONLY
        return real_mode_getter()

    return _wrapped
