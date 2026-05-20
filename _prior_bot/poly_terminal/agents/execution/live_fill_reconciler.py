"""LiveFillReconciler — bridges Polymarket User-WS fill events back to
the live_orders audit row.

Why an agent and not just inline in ExecutionAgent: fills arrive
asynchronously via WebSocket (potentially many minutes after submit),
not in the same event loop turn that did the post_order. This
reconciler subscribes to:

  EVT_ORDER_FILLED     → record a fill (partial or terminal)
  EVT_ORDER_CANCELLED  → mark the row cancelled

Both events also fire for paper-mode trades; we distinguish by the
presence of an `order_id` (Polymarket's hash) — paper events use
`intent_id` and set `paper: True`, so they're filtered out cheaply.

Position-side reconciliation (replacing the limit-price snapshot in
`positions` with the actual fill price) is intentionally NOT done
here yet — that's a follow-up so this phase has a tight blast radius.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_ORDER_CANCELLED,
    EVT_ORDER_FILLED,
    EVT_POSITION_CLOSED,
)
from poly_terminal.persistence.repositories.fills import PositionsRepo
from poly_terminal.persistence.repositories.live_orders import LiveOrdersRepo

logger = logging.getLogger(__name__)


class LiveFillReconciler:
    # P0 hardening (2026-05-03 audit, deep-research-report 14/15/16):
    # Polymarket's User WS can emit the SAME trade up to 4 times across
    # the lifecycle (MATCHED → MINED → CONFIRMED → optional RETRYING).
    # Without dedupe the manual-close path would close a DIFFERENT
    # position on each event (since fetch_oldest_open_for_token is
    # FIFO). On stacked tokens (7 of our open tokens currently have 3
    # positions each), this would silently nuke our entire stack on
    # one external SELL. Bound the dedupe set to prevent unbounded
    # growth — 4096 keys handles ≥1h of fast trading.
    _DEDUPE_MAX = 4096

    def __init__(
        self,
        bus: EventBus,
        live_orders_repo: LiveOrdersRepo,
        positions_repo: PositionsRepo | None = None,
    ) -> None:
        self._bus = bus
        self._repo = live_orders_repo
        # 2026-05-03 manual-close detection: when a SELL fill arrives
        # for an order_id NOT in live_orders (operator manually closed
        # on Polymarket UI), look up the open bot-managed position on
        # that token and close it. Without this, ProfitTaker keeps
        # firing SELLs against zero on-chain inventory ("balance: 0"
        # errors). Optional so existing callers / tests that only
        # care about audit-row reconciliation don't have to wire it.
        self._positions = positions_repo
        # FIFO bounded dedupe set for external-fill keys. Prevents the
        # MATCHED→MINED→CONFIRMED multi-event problem from closing
        # multiple positions for one external trade. Persist across
        # the process lifetime — a restart legitimately allows a
        # re-apply since we just lost the position state too.
        self._seen_external_fills: dict[str, None] = {}
        self._started = False
        self._stats = {
            "fills_recorded": 0,
            "fills_unmatched": 0,
            "manual_closes_detected": 0,
            "partial_manual_closes": 0,
            "manual_close_dedupe_hits": 0,
            "cancellations_recorded": 0,
            "cancellations_unmatched": 0,
            "paper_events_skipped": 0,
            "errors": 0,
        }

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_ORDER_FILLED, self._on_fill)
        self._bus.subscribe(EVT_ORDER_CANCELLED, self._on_cancel)
        self._started = True

    # ── EVT_ORDER_FILLED ─────────────────────────────────────────────

    async def _on_fill(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            self._stats["errors"] += 1
            return
        if payload.get("paper") is True:
            self._stats["paper_events_skipped"] += 1
            return
        polymarket_order_id = str(payload.get("order_id", ""))
        if not polymarket_order_id:
            # Live events without an order_id are malformed; a paper
            # event will already have been short-circuited above.
            self._stats["errors"] += 1
            return
        try:
            fill_qty = float(payload.get("filled_size") or payload.get("size") or 0)
            fill_price = float(payload.get("price") or 0)
        except (TypeError, ValueError):
            self._stats["errors"] += 1
            return
        if fill_qty <= 0 or fill_price <= 0:
            self._stats["errors"] += 1
            return
        # Polymarket may include a per-fill fee in the WS payload as
        # `fee` (USDC) or `fee_rate_bps`. Today both are 0 on Polygon
        # but capture whatever's there so the column reflects reality
        # if/when fees switch on.
        try:
            fill_fee_usd = float(payload.get("fee") or 0)
        except (TypeError, ValueError):
            fill_fee_usd = 0.0
        # Polymarket WS sends status='MATCHED' for a fully-filled order.
        # Lower-cased here; treat 'matched' / 'filled' as terminal.
        status = str(payload.get("status", "")).upper()
        terminal = status in ("MATCHED", "FILLED")
        try:
            updated = await self._repo.record_fill(
                polymarket_order_id=polymarket_order_id,
                fill_qty=fill_qty,
                fill_price=fill_price,
                ts=int(time.time()),
                terminal=terminal,
                fill_fee_usd=fill_fee_usd,
            )
        except Exception:
            logger.exception(
                "live_reconciler: record_fill failed for order %s",
                polymarket_order_id,
            )
            self._stats["errors"] += 1
            return
        if updated is None:
            # Fill arrived for an order we didn't post (e.g. the user
            # placed an order outside the bot, or a Polymarket replay).
            self._stats["fills_unmatched"] += 1
            # 2026-05-03 manual-close detection: if this is a SELL
            # for a token we have an open bot-managed position on,
            # the operator most likely closed manually on Polymarket
            # UI. Close the matching position so ProfitTaker stops
            # firing SELLs against zero on-chain inventory.
            side = str(payload.get("side", "")).upper()
            # P0 hardening: Polymarket's User WS uses `asset_id` as
            # the token identifier in some message shapes. Accept
            # either name so manual closes don't silently drop on the
            # wrong key (deep-research-report 14/15 finding).
            token_id = str(
                payload.get("token_id")
                or payload.get("asset_id")
                or ""
            )
            if (
                side == "SELL"
                and token_id
                and self._positions is not None
            ):
                # P0 hardening: dedupe by (order_id, side, token, qty,
                # price) so MATCHED → MINED → CONFIRMED multi-events
                # don't close 2-3 different positions on the same
                # external trade. Reasonably stable across status
                # transitions — Polymarket reports the same numeric
                # values throughout the lifecycle.
                trade_id = str(payload.get("trade_id") or payload.get("id") or "")
                tx_hash = str(payload.get("tx_hash") or payload.get("transaction_hash") or "")
                dedupe_key = trade_id or tx_hash or (
                    f"{polymarket_order_id}:{token_id}:{fill_qty:.4f}:{fill_price:.4f}"
                )
                if dedupe_key in self._seen_external_fills:
                    self._stats["manual_close_dedupe_hits"] += 1
                    logger.debug(
                        "live_reconciler: dedup'd external SELL event "
                        "(key=%s, status=%s) — already applied",
                        dedupe_key, status,
                    )
                    return
                # Mark BEFORE the close so a concurrent dispatch can't
                # race past us. Bound the set to _DEDUPE_MAX entries.
                self._seen_external_fills[dedupe_key] = None
                if len(self._seen_external_fills) > self._DEDUPE_MAX:
                    # FIFO eviction (Python dicts preserve insertion order).
                    oldest = next(iter(self._seen_external_fills))
                    del self._seen_external_fills[oldest]
                await self._maybe_close_manual(
                    token_id, fill_price, polymarket_order_id,
                    fill_qty=fill_qty,
                )
            return
        self._stats["fills_recorded"] += 1

    async def _maybe_close_manual(
        self, token_id: str, fill_price: float, order_id: str,
        fill_qty: float = 0.0,
    ) -> None:
        """Close (fully or partially) the oldest open bot-managed
        position on `token_id` based on the external SELL fill size.

        2026-05-03 P1 #2: when fill_qty < oldest position's
        shares_remaining, do a PARTIAL close (decrement
        shares_remaining, accumulate partial PnL, leave open). Only
        fire EVT_POSITION_CLOSED when fully closed. Pass fill_qty=0
        to force full close (legacy behavior).
        """
        assert self._positions is not None  # narrow for the type checker
        try:
            opened = await self._positions.fetch_oldest_open_for_token(token_id)
        except Exception:
            logger.exception(
                "live_reconciler: fetch_oldest_open_for_token failed for %s",
                token_id,
            )
            self._stats["errors"] += 1
            return
        if opened is None:
            return
        position_id = int(opened["position_id"])
        # Decide partial vs full. shares_remaining isn't on the dict
        # returned by fetch_oldest_open_for_token, so use shares as
        # the upper bound — reduce_open_position reads the authoritative
        # remaining under the same connection.
        position_shares = float(opened.get("shares", 0))
        # fill_qty=0 (legacy callers / no qty in payload) → full close.
        # Otherwise treat fill_qty as the SELL size and decide.
        if fill_qty > 0 and fill_qty < position_shares - 1e-9:
            # Partial close path.
            try:
                result = await self._positions.reduce_open_position(
                    position_id=position_id,
                    qty_delta=fill_qty,
                    exit_price=fill_price,
                    closed_ts=int(time.time()),
                )
            except Exception:
                logger.exception(
                    "live_reconciler: reduce_open_position failed for pid=%d",
                    position_id,
                )
                self._stats["errors"] += 1
                return
            if result is None:
                return
            if result["fully_closed"]:
                # Edge: race made it fully close (pre-existing partials
                # + this fill consumed remaining). Treat as full close.
                self._stats["manual_closes_detected"] += 1
                logger.info(
                    "live_reconciler: MANUAL_CLOSE (final partial) for "
                    "token %s — pid %d, fill_qty=%.4f, exit=%.4f, "
                    "trigger_order=%s",
                    token_id, position_id, fill_qty, fill_price, order_id,
                )
                try:
                    await self._bus.publish(
                        EVT_POSITION_CLOSED,
                        {
                            "position_id": position_id,
                            "token_id": token_id,
                            "exit_price": fill_price,
                            "realized_pnl": result["partial_pnl"],
                            "outcome": "MANUAL_CLOSE",
                        },
                    )
                except Exception:
                    logger.exception(
                        "live_reconciler: EVT_POSITION_CLOSED publish "
                        "failed for pid=%d", position_id,
                    )
            else:
                self._stats["partial_manual_closes"] += 1
                logger.info(
                    "live_reconciler: PARTIAL_MANUAL_CLOSE for token %s "
                    "— pid %d, sold=%.4f, remaining=%.4f, partial_pnl=$%.4f, "
                    "trigger_order=%s",
                    token_id, position_id, fill_qty,
                    result["shares_remaining_after"],
                    result["partial_pnl"], order_id,
                )
                # No EVT_POSITION_CLOSED — position is still open.
            return
        # Full close path (fill_qty == 0, or fill_qty >= position shares).
        try:
            closed = await self._positions.close_position(
                position_id=position_id,
                exit_price=fill_price,
                outcome="MANUAL_CLOSE",
                closed_ts=int(time.time()),
            )
        except Exception:
            logger.exception(
                "live_reconciler: close_position failed for pid=%d", position_id
            )
            self._stats["errors"] += 1
            return
        if closed is None:
            # Race — someone else closed it between fetch + update.
            return
        self._stats["manual_closes_detected"] += 1
        logger.info(
            "live_reconciler: MANUAL_CLOSE detected for token %s — closed "
            "position %d at exit=%.4f (entry=%.4f, shares=%.4f, pnl=$%.4f, "
            "trigger_order=%s)",
            token_id, position_id, fill_price, opened["entry_price"],
            opened["shares"], closed["realized_pnl"], order_id,
        )
        try:
            await self._bus.publish(
                EVT_POSITION_CLOSED,
                {
                    "position_id": position_id,
                    "token_id": token_id,
                    "exit_price": fill_price,
                    "realized_pnl": closed["realized_pnl"],
                    "outcome": "MANUAL_CLOSE",
                },
            )
        except Exception:
            logger.exception(
                "live_reconciler: EVT_POSITION_CLOSED publish failed for "
                "pid=%d", position_id,
            )

    # ── EVT_ORDER_CANCELLED ──────────────────────────────────────────

    async def _on_cancel(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            self._stats["errors"] += 1
            return
        if payload.get("paper") is True:
            self._stats["paper_events_skipped"] += 1
            return
        polymarket_order_id = str(payload.get("order_id", ""))
        if not polymarket_order_id:
            self._stats["errors"] += 1
            return
        try:
            ok = await self._repo.mark_cancelled(
                polymarket_order_id=polymarket_order_id,
                ts=int(time.time()),
            )
        except Exception:
            logger.exception(
                "live_reconciler: mark_cancelled failed for order %s",
                polymarket_order_id,
            )
            self._stats["errors"] += 1
            return
        if ok:
            self._stats["cancellations_recorded"] += 1
        else:
            self._stats["cancellations_unmatched"] += 1
