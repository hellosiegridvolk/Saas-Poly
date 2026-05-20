"""Phase 30(a) — OrdersRecorderAgent.

Subscribes to the bus events that `UserDispatcher` publishes from
the authenticated user-channel WS — EVT_ORDER_SUBMITTED,
EVT_ORDER_FILLED, EVT_ORDER_CANCELLED — and writes each event to
the `orders` table via `OrdersRepo`.

This closes the audit-trail gap flagged in deep-research-report
(26)/(27): without this, the bot logs that a SELL was signed +
submitted but has no record of whether the chain ultimately
matched, mined, or reverted that order.

Defensive — every event handler swallows persistence errors so
the bus stays unblocked. The repo also defensively swallows.
"""

from __future__ import annotations

import logging
from typing import Any

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_ORDER_CANCELLED,
    EVT_ORDER_FILLED,
    EVT_ORDER_SUBMITTED,
)
from poly_terminal.persistence.repositories.orders import OrdersRepo

logger = logging.getLogger(__name__)


# Map bus event name → orders.state column value.
_STATE_FROM_EVENT = {
    EVT_ORDER_SUBMITTED: "LIVE",
    EVT_ORDER_FILLED: "MATCHED",
    EVT_ORDER_CANCELLED: "CANCELLED",
}


class OrdersRecorderAgent:
    """Subscribes to user-channel order events and persists them."""

    def __init__(self, bus: EventBus, repo: OrdersRepo) -> None:
        self._bus = bus
        self._repo = repo
        self._started = False
        self.stats: dict[str, int] = {
            "order_submitted_recorded": 0,
            "order_filled_recorded": 0,
            "order_cancelled_recorded": 0,
            "errors": 0,
        }

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_ORDER_SUBMITTED, self._on_order)
        self._bus.subscribe(EVT_ORDER_FILLED, self._on_order)
        self._bus.subscribe(EVT_ORDER_CANCELLED, self._on_order)
        self._started = True

    async def _on_order(self, event_name: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        try:
            order_id = str(payload.get("order_id") or "")
            token_id = str(payload.get("token_id") or "")
            side = str(payload.get("side") or "").upper()
            state = _STATE_FROM_EVENT.get(event_name, "UNKNOWN")
            # price/size may be string-encoded by upstream parser
            try:
                price = float(payload.get("price") or 0)
            except (TypeError, ValueError):
                price = 0.0
            try:
                size = float(payload.get("size") or 0)
            except (TypeError, ValueError):
                size = 0.0
            try:
                filled = float(payload.get("filled_size") or 0)
            except (TypeError, ValueError):
                filled = 0.0
        except Exception:
            self.stats["errors"] += 1
            return
        if not order_id:
            return
        await self._repo.upsert(
            order_id=order_id,
            token_id=token_id,
            side=side,
            state=state,
            price=price,
            size=size,
            filled_size=filled,
        )
        if event_name == EVT_ORDER_SUBMITTED:
            self.stats["order_submitted_recorded"] += 1
        elif event_name == EVT_ORDER_FILLED:
            self.stats["order_filled_recorded"] += 1
        elif event_name == EVT_ORDER_CANCELLED:
            self.stats["order_cancelled_recorded"] += 1
