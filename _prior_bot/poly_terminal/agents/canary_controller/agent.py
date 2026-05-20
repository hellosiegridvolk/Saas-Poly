"""CanaryControllerAgent — auto-flips bot mode from LIVE → CLOSE_ONLY
after the first real LIVE fill.

2026-05-05 — bounds canary blast radius to exactly one position. Without
this agent, a LIVE-mode bot could open many positions before the
operator realises the canary already fired. With it:

  1. Bot boots in LIVE mode (operator's call).
  2. First BUY intent flows through gates (mode_lock allows in LIVE),
     gets signed + submitted, fills on Polymarket.
  3. Execution agent records the fill via PositionsRepo.open_position
     and publishes EVT_POSITION_OPENED.
  4. THIS agent picks up the event, confirms it was a real LIVE
     fill (not LIVE_DRY signing-only or PAPER-simulated), invokes
     the on_canary_fired callback.
  5. main.py's callback flips a runtime mode override to CLOSE_ONLY.
  6. Subsequent BUY intents are rejected at mode_lock with
     `mode_close_only_buy_blocked`.
  7. SELL intents continue flowing — CLOSE_ONLY allows the canary
     position to be closed via the normal exit path.

Idempotent: only fires once per process. Subsequent EVT_POSITION_OPENED
events are no-ops after the first fire (defensive against any race).

Filtering rules — agent ignores:
  - non-LIVE-mode openings (PAPER/LIVE_DRY don't trigger canary lockout)
  - imported on-chain positions (entry_intent_id LIKE 'imported%')
  - openings whose live_orders row has filled_qty=0 (no real on-chain
    inventory; the canary intent was rejected/no-match-FAK)
  - openings whose live_orders row mode != 'LIVE' (LIVE_DRY signs
    audit rows but isn't a real fill)
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Protocol

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_POSITION_OPENED
from poly_terminal.shared.enums import BotMode

logger = logging.getLogger(__name__)


class _LiveOrdersRepoProto(Protocol):
    async def fetch_by_client_id(
        self, client_order_id: str
    ) -> dict[str, object] | None: ...


class CanaryControllerAgent:
    """Auto-flips mode after first LIVE fill. Construct with a
    callable that flips a mode override. Single instance per process.
    """

    def __init__(
        self,
        bus: EventBus,
        mode_getter: Callable[[], BotMode],
        live_orders_repo: _LiveOrdersRepoProto,
        on_canary_fired: Callable[[], Awaitable[None] | None],
    ) -> None:
        self._bus = bus
        self._mode_getter = mode_getter
        self._live_repo = live_orders_repo
        self._on_canary_fired = on_canary_fired
        self._fired = False
        self._started = False
        self.stats: dict[str, Any] = {
            "observed_opens": 0,
            "ignored_not_live_mode": 0,
            "ignored_imported": 0,
            "ignored_no_live_order": 0,
            "ignored_live_dry": 0,
            "ignored_zero_fill": 0,
            "fired": False,
            "fired_at_position_id": None,
        }

    @property
    def fired(self) -> bool:
        """True once the canary has tripped. Idempotent — subsequent
        opens become no-ops."""
        return self._fired

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_POSITION_OPENED, self._on_open)
        self._started = True

    async def _on_open(self, _event: str, payload: Any) -> None:
        self.stats["observed_opens"] += 1
        # Idempotent: only flip once.
        if self._fired:
            return
        if not isinstance(payload, dict):
            return

        # Only fire when the bot is actually in LIVE mode at boot.
        # If we're somehow in CLOSE_ONLY/PAPER/LIVE_DRY when this fires,
        # it's not a canary scenario — bail.
        if self._mode_getter() is not BotMode.LIVE:
            self.stats["ignored_not_live_mode"] += 1
            return

        # 2026-05-06 FIX: EVT_POSITION_OPENED publishes the key as
        # `intent_id`, not `entry_intent_id` (the latter is the column
        # name in the positions table). Pre-fix, this filter ALWAYS
        # returned `ignored_imported` because the lookup got "" — the
        # canary controller therefore never flipped LIVE → CLOSE_ONLY,
        # and the 2026-05-06 12:05 canary opened a SECOND real-money
        # BUY (pos 22316) before being manually paused.
        # Tolerate both spellings for forward-compat with older payloads.
        intent_id = str(
            payload.get("intent_id")
            or payload.get("entry_intent_id")
            or ""
        )
        # Loud counter split: missing key vs. genuinely imported. Pre-fix
        # both bucketed as `ignored_imported` and the regression was
        # invisible. If `ignored_missing_intent_id` ever ticks > 0 in
        # LIVE mode, something published EVT_POSITION_OPENED without
        # the required `intent_id` field — file a bug.
        if not intent_id:
            self.stats["ignored_imported"] += 1
            self.stats.setdefault("ignored_missing_intent_id", 0)
            self.stats["ignored_missing_intent_id"] += 1
            logger.error(
                "canary_controller: EVT_POSITION_OPENED payload "
                "missing intent_id — controller cannot verify LIVE "
                "fill. Position id=%s. Treating as imported (no fire) "
                "but this is a publisher bug.",
                payload.get("position_id"),
            )
            return
        if intent_id.startswith("imported"):
            self.stats["ignored_imported"] += 1
            return

        # Look up the BUY's live_orders row to confirm a real LIVE fill.
        # client_order_id is "poly-v3-{intent_id}" (set in execution agent).
        try:
            order = await self._live_repo.fetch_by_client_id(
                f"poly-v3-{intent_id}",
            )
        except Exception:
            logger.exception(
                "canary_controller: fetch_by_client_id raised for %s",
                intent_id,
            )
            return
        if order is None:
            self.stats["ignored_no_live_order"] += 1
            return
        order_mode = str(order.get("mode", ""))
        if order_mode != "LIVE":
            self.stats["ignored_live_dry"] += 1
            return
        try:
            filled_qty = float(order.get("filled_qty") or 0)
        except (TypeError, ValueError):
            filled_qty = 0.0
        if filled_qty <= 0:
            self.stats["ignored_zero_fill"] += 1
            return

        # ── Canary fired ──────────────────────────────────────────
        self._fired = True
        self.stats["fired"] = True
        position_id = int(payload.get("position_id", 0))
        self.stats["fired_at_position_id"] = position_id
        logger.warning(
            "canary_controller: LIVE fill on position %d (intent=%s, "
            "filled_qty=%.4f) — auto-flipping mode to CLOSE_ONLY. "
            "No further BUYs will be accepted; SELLs continue.",
            position_id, intent_id, filled_qty,
        )
        try:
            result = self._on_canary_fired()
            # Support both sync and async callbacks.
            if hasattr(result, "__await__"):
                await result  # type: ignore[misc]
        except Exception:
            logger.exception(
                "canary_controller: on_canary_fired callback raised",
            )
