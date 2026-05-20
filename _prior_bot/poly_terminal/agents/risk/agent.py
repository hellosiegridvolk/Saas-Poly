"""Risk Agent — wires the gate pipeline to the bus.

Subscribes to EVT_BUY_INTENT (and EVT_SELL_INTENT for symmetry); runs the
configured pipeline; publishes EVT_INTENT_APPROVED on pass or
EVT_INTENT_REJECTED on fail.

Bug #2 (cap race): when an `OpenPositionsReservationLedger` is supplied,
the agent also subscribes to terminal order events (FILLED / REJECTED /
CANCELLED) so it can release reservations the OpenPositionsGate made
during evaluation. It additionally releases any reservation if a later
gate in the pipeline rejects the intent — the gate that reserved doesn't
know about downstream rejection, so the agent owns that cleanup.

The TTL on the ledger is the last-resort safety net (covers paths that
don't emit terminal events at all, e.g. execution-side aborts before
submission).
"""

from __future__ import annotations

import logging
from typing import Any

from poly_terminal.agents.risk.pipeline import GatePipeline
from poly_terminal.agents.risk.reservation_ledger import (
    OpenPositionsReservationLedger,
)
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_BUY_INTENT,
    EVT_INTENT_APPROVED,
    EVT_INTENT_REJECTED,
    EVT_ORDER_CANCELLED,
    EVT_ORDER_FILLED,
    EVT_ORDER_REJECTED,
    EVT_SELL_INTENT,
)
from poly_terminal.shared.typed_reject import Reject

logger = logging.getLogger(__name__)


class RiskAgent:
    def __init__(
        self,
        bus: EventBus,
        buy_pipeline: GatePipeline,
        sell_pipeline: GatePipeline | None = None,
        reservation_ledger: OpenPositionsReservationLedger | None = None,
    ) -> None:
        self._bus = bus
        self._buy_pipeline = buy_pipeline
        self._sell_pipeline = sell_pipeline
        self._ledger = reservation_ledger
        self._started = False
        self._intent_count = 0
        self._reject_count = 0
        self._reservations_released = 0

    @property
    def stats(self) -> dict[str, int]:
        return {
            "intents_seen": self._intent_count,
            "intents_rejected": self._reject_count,
            "reservations_released": self._reservations_released,
        }

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_BUY_INTENT, self._on_buy_intent)
        if self._sell_pipeline is not None:
            self._bus.subscribe(EVT_SELL_INTENT, self._on_sell_intent)
        if self._ledger is not None:
            self._bus.subscribe(EVT_ORDER_FILLED, self._on_order_terminal)
            self._bus.subscribe(EVT_ORDER_REJECTED, self._on_order_terminal)
            self._bus.subscribe(EVT_ORDER_CANCELLED, self._on_order_terminal)
        self._started = True

    async def _on_buy_intent(self, _event: str, intent: Any) -> None:
        self._intent_count += 1
        allowed, reject = await self._buy_pipeline.evaluate(intent)
        # If a downstream gate rejected after OpenPositionsGate reserved,
        # release the reservation so the slot isn't held until TTL.
        if not allowed and self._ledger is not None:
            intent_id = str(getattr(intent, "intent_id", "") or "")
            if intent_id and self._ledger.is_reserved(intent_id):
                self._ledger.release(intent_id)
                self._reservations_released += 1
        await self._publish_result(intent, allowed, reject)

    async def _on_sell_intent(self, _event: str, intent: Any) -> None:
        self._intent_count += 1
        if self._sell_pipeline is None:
            await self._publish_result(intent, True, None)
            return
        allowed, reject = await self._sell_pipeline.evaluate(intent)
        await self._publish_result(intent, allowed, reject)

    async def _on_order_terminal(self, _event: str, payload: Any) -> None:
        """Release reservation on order-lifecycle terminal events.

        Paper fills include `intent_id` directly. Live UserWebSocket
        events do NOT carry `intent_id` (only `order_id`), so the TTL is
        the release path for those — once the live order fills, the
        position-row write makes `_read()` return the new count anyway,
        so the in-flight value going stale via TTL is benign.
        """
        if self._ledger is None or not isinstance(payload, dict):
            return
        intent_id = str(payload.get("intent_id") or "")
        if not intent_id:
            return
        if self._ledger.is_reserved(intent_id):
            self._ledger.release(intent_id)
            self._reservations_released += 1

    async def _publish_result(
        self, intent: Any, allowed: bool, reject: Reject | None
    ) -> None:
        if allowed:
            await self._bus.publish(EVT_INTENT_APPROVED, intent)
        else:
            self._reject_count += 1
            await self._bus.publish(
                EVT_INTENT_REJECTED,
                {
                    "intent_id": getattr(intent, "intent_id", ""),
                    "reason": reject.code if reject is not None else "unknown",
                    "detail": reject.detail if reject is not None else "",
                    "intent": intent,
                },
            )
