"""Gate 1: duplicate intent (in-memory set; nanoseconds)."""

from __future__ import annotations

from poly_terminal.shared.typed_reject import Reject


class DuplicateIntentGate:
    def __init__(self) -> None:
        self._seen: set[str] = set()

    async def __call__(self, intent: object) -> Reject | None:
        intent_id = getattr(intent, "intent_id", "")
        if not intent_id:
            return Reject(code="missing_intent_id")
        if intent_id in self._seen:
            return Reject(code="duplicate_intent_id", detail=intent_id)
        self._seen.add(intent_id)
        return None

    def reset(self) -> None:
        self._seen.clear()
