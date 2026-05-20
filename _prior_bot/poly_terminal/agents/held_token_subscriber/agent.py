"""Auto-subscribe MarketWebSocket to every held token on entry.

Watches EVT_POSITION_OPENED and forwards the token_id into the WS
subscription manager. The MarketWebSocket loop drains pending subs
on its next flush iteration, so a token observed at second T is
typically subscribed within ≤ 1s.

Design notes:
  - This agent does not OWN MarketWebSocket; it just calls
    `ws.subscribe_tokens([t])`. SubscriptionManager handles dedup
    against already-subscribed + already-pending.
  - We don't auto-unsubscribe on EVT_POSITION_CLOSED in this MVP.
    Multiple positions on the same token are common, and the WS
    overhead per token is negligible. A future reaper agent can
    prune subscriptions older than X hours with no open positions.
  - In tests + PAPER without a real WS, pass `ws=None`; the agent
    no-ops cleanly.

Stats surface:
  - tokens_subscribed_total: cumulative observed-and-forwarded count
  - distinct_tokens_seen: unique token_ids observed since boot
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_POSITION_OPENED

logger = logging.getLogger(__name__)


class _WSLike(Protocol):
    """Minimal MarketWebSocket interface this agent depends on."""

    def subscribe_tokens(self, token_ids: list[str]) -> None: ...


class HeldTokenSubscriberAgent:
    def __init__(
        self,
        bus: EventBus,
        ws: "_WSLike | None" = None,
    ) -> None:
        self._bus = bus
        self._ws = ws
        self._seen_tokens: set[str] = set()
        self._started = False
        self.stats: dict[str, int] = {
            "events_received": 0,
            "tokens_subscribed_total": 0,
            "distinct_tokens_seen": 0,
            "no_ws_skipped": 0,
            "malformed_skipped": 0,
        }

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_POSITION_OPENED, self._on_open)
        self._started = True

    async def _on_open(self, _e: str, payload: Any) -> None:
        self.stats["events_received"] += 1
        if not isinstance(payload, dict):
            self.stats["malformed_skipped"] += 1
            return
        token_id = str(payload.get("token_id", "") or "")
        if not token_id:
            self.stats["malformed_skipped"] += 1
            return
        if self._ws is None:
            # PAPER / tests — agent no-ops cleanly.
            self.stats["no_ws_skipped"] += 1
            return
        # Idempotent: SubscriptionManager.subscribe filters against
        # already-subscribed + already-pending.
        try:
            self._ws.subscribe_tokens([token_id])
        except Exception:
            logger.exception(
                "HeldTokenSubscriber: ws.subscribe_tokens failed for %s",
                token_id[:16],
            )
            return
        self.stats["tokens_subscribed_total"] += 1
        if token_id not in self._seen_tokens:
            self._seen_tokens.add(token_id)
            self.stats["distinct_tokens_seen"] += 1
            logger.info(
                "held_token_subscriber: subscribed token=%s… (total=%d)",
                token_id[:16],
                self.stats["distinct_tokens_seen"],
            )
