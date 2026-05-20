"""Async in-process event bus — port of v2 `shared/bus.py`.

All agents communicate exclusively through this bus. No agent calls
another agent's methods. See ADR 0001.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

Handler = Callable[[str, Any], Awaitable[None]]


class EventBus:
    """Asyncio-native pub/sub bus.

    Handlers run sequentially in subscription order. An exception in one
    handler is logged and does not stop the publisher or subsequent
    handlers.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, list[Handler]] = defaultdict(list)

    def subscribe(self, event: str, handler: Handler) -> None:
        self._handlers[event].append(handler)

    def unsubscribe(self, event: str, handler: Handler) -> None:
        try:
            self._handlers[event].remove(handler)
        except ValueError:
            pass

    async def publish(self, event: str, payload: Any = None) -> None:
        # Snapshot the list so handlers can subscribe/unsubscribe mid-publish
        # without mutating what we're iterating over.
        handlers = list(self._handlers.get(event, []))
        for handler in handlers:
            try:
                await handler(event, payload)
            except Exception:
                logger.exception(
                    "Unhandled exception in handler %s for event %s",
                    getattr(handler, "__qualname__", repr(handler)),
                    event,
                )

    def subscriber_count(self, event: str) -> int:
        return len(self._handlers.get(event, []))


bus = EventBus()
