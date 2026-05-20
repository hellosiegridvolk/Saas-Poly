"""PositionImporterAgent — discover on-chain positions opened outside the bot.

Polls Polymarket Data API `/positions?address=<funder>` periodically.
For every on-chain position the bot doesn't already know about (i.e.
no row in `positions` for that token), the agent:

  1. Inserts a `positions` row with `entry_intent_id="imported:{token_id}"`
     and the on-chain avg_price + size as the cost basis.
  2. Inserts a synthetic `live_orders` BUY row with
     `client_order_id="poly-v3-imported:{token_id}"`, status='filled',
     filled_qty = shares. Without this, the SELL inventory gate
     (`_handle_live_sell` requires a filled BUY row to exist for the
     intent_id) would block any exit attempt the import drives.
  3. Subscribes the market WebSocket to the token so ticks flow into
     `ProfitTakerAgent` and `ExitDecisionEngine`.
  4. Publishes `EVT_POSITION_OPENED` so those agents start tracking.

The agent never closes positions on its own — it only imports.
Existing exit machinery (ProfitTaker, ExitDecisionEngine,
BarResolutionWatcher) handles closes once the position is in
the bot's awareness.

Usage: wire only when LIVE/LIVE_DRY (PAPER has no on-chain holdings;
READ_ONLY skips trading entirely).
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Protocol

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_POSITION_OPENED
from poly_terminal.persistence.repositories.fills import (
    PositionRow,
    PositionsRepo,
)
from poly_terminal.persistence.repositories.live_orders import (
    LiveOrderRow,
    LiveOrdersRepo,
)

logger = logging.getLogger(__name__)


class _PositionsSource(Protocol):
    async def fetch_for_wallet(
        self, wallet: str
    ) -> list[dict[str, Any]]:
        """Each item: {market_id, token_id, side, size, avg_price, …}."""
        ...


class _MarketWS(Protocol):
    def subscribe_tokens(self, token_ids: list[str]) -> None: ...


@dataclass(frozen=True)
class PositionImporterConfig:
    interval_s: float = 60.0   # 60s sweep — manual trades aren't high-frequency
    min_size: float = 0.0001   # ignore dust positions (<1 share-tenth-of-thousandth)
    # 2026-05-07 PHASE 12 — dust cost-basis floor.
    # Polymarket V2 has a $1 minimum order size for SELLs. When a
    # partial-fill leaves <$1 of inventory on chain, the bot's
    # importer used to reconstruct it as a phantom open position
    # every minute, ProfitTaker would fire SELL, the SDK would
    # refuse to sign (below minimum), and close_position would
    # record a fictional realized loss. Each sweep created a NEW
    # phantom (22371, 22372, 22373... infinite loop on May 7 v16).
    # Skip imports below this threshold — those shares are
    # effectively dust pending market resolution / redemption.
    dust_threshold_usd: float = 1.0
    # 2026-05-07 PHASE 14 — SELL settlement window.
    # Polymarket /positions reflects on-chain CTF balances. After the
    # bot signs+POSTs a SELL, those shares stay on-chain until the
    # exchange's matcher settles the trade — typically 5-15s. During
    # that window /positions still returns the pre-sell shares; the
    # importer (without this gate) would create a phantom open
    # position for them. On the next bot start, InventoryReconciler
    # sees chain=0 vs db=phantom-shares → hard drift → bot crash.
    #
    # Phase 14 skips imports for tokens with a SELL signed within the
    # last `recent_sell_window_s` seconds. v24 (pos 22441), v25
    # (pos 22443), and v27 (pos 22446) on May 7 all surfaced this
    # exact race. Default 60s gives ample margin even for slow
    # finalization.
    recent_sell_window_s: int = 60


@dataclass
class PositionImporterStats:
    sweeps: int = 0
    imported_total: int = 0           # active on-chain positions
    imported_redeemable_total: int = 0  # already-resolved positions handed
                                       # off to the redeemer agent
    delta_sweep_closed: int = 0       # tokens closed because inventory gone
    inventory_gone_reconciled: int = 0  # Phase 8 — closed positions whose
                                       # actual SELL fill we recovered from
                                       # the Data API /activity endpoint
    dust_skipped: int = 0             # Phase 12 — leftover positions below
                                       # V2 minimum (cost_basis < dust_threshold)
                                       # skipped to prevent phantom-loop
    recent_sell_skipped: int = 0      # Phase 14 — imports skipped because
                                       # a SELL for the token was signed
                                       # within recent_sell_window_s
                                       # (settlement-window race guard)
    reconciliation_lock_skipped: int = 0  # Phase 31 — imports skipped
                                       # because the token has an active
                                       # reconciliation lock (SELL_FAILED
                                       # quarantine pending redemption)
    last_sweep_ts: int = 0
    errors: int = 0


# Phase 8 (2026-05-06) — pluggable activity fetcher signature.
# Mirrors `DataApiClient.fetch_activity(wallet, limit) -> list[dict]`.
# Tests inject a fake; production wiring uses the real client.
_ActivityFetcher = "Callable[[str, int], Awaitable[list[dict[str, Any]]]]"


class PositionImporterAgent:
    def __init__(
        self,
        bus: EventBus,
        positions_source: _PositionsSource,
        positions_repo: PositionsRepo,
        live_orders_repo: LiveOrdersRepo,
        funder_address: str,
        market_ws: _MarketWS | None = None,
        cfg: PositionImporterConfig | None = None,
        activity_fetcher: Any = None,  # Phase 8 — see _ActivityFetcher
        reconciliation_lock_repo: Any = None,  # Phase 31 — see below
    ) -> None:
        self._bus = bus
        self._source = positions_source
        self._positions = positions_repo
        self._live_orders = live_orders_repo
        # 2026-05-06 PHASE 8: optional callable to fetch the wallet's
        # recent TRADE activity from Polymarket /activity. When the
        # delta-sweep closes a position with INVENTORY_GONE, we use
        # this to recover the actual SELL fill price (chain-state
        # only tells us the shares are gone, not what they sold for).
        # When None, behavior is unchanged (no reconciliation).
        self._activity_fetcher = activity_fetcher
        # 2026-05-09 PHASE 31: reconciliation lock repo. When the
        # execution agent records a SELL_FAILED (escalation exhausted),
        # it sets a quarantine lock on (token_id, position_id). The
        # importer skips re-importing leftover on-chain shares while
        # the lock is active. Closes the v50r2/v51/v52 phantom-double
        # chain (22493/22495/22497). None = legacy behavior.
        self._reconciliation_lock_repo = reconciliation_lock_repo
        self._funder = funder_address.lower()
        self._market_ws = market_ws
        self._cfg = cfg or PositionImporterConfig()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.stats = PositionImporterStats()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _loop(self) -> None:
        # Run once immediately so manual positions get tracked at boot.
        try:
            await self.run_once()
        except Exception:
            logger.exception("position_importer: initial sweep failed")
            self.stats.errors += 1
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._cfg.interval_s
                )
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                return
            try:
                await self.run_once()
            except Exception:
                logger.exception("position_importer: sweep failed")
                self.stats.errors += 1

    async def run_once(self) -> int:
        """Returns count of positions imported this sweep. Public so
        tests / scripts can drive it manually."""
        on_chain = await self._source.fetch_for_wallet(self._funder)
        imported = 0
        now_ts = int(time.time())
        for pos in on_chain:
            try:
                token_id = str(pos["token_id"])
                size = float(pos.get("size", 0))
                avg_price = float(pos.get("avg_price", 0))
                market_id = str(pos.get("market_id", ""))
            except (KeyError, TypeError, ValueError):
                continue
            if size < self._cfg.min_size or avg_price <= 0 or not token_id:
                continue
            # Already-resolved markets: redeemable=true. ProfitTaker
            # can't do anything (market closed) but the redeemer
            # agent's working set is `closed_ts IS NOT NULL AND
            # redeemed_ts IS NULL` — so we import them as PRE-CLOSED
            # rows + synthetic filled BUY, then the redeemer
            # classifies WORTHLESS / REDEEMABLE on its next sweep.
            if pos.get("redeemable"):
                if await self._positions.has_imported_position(token_id):
                    continue
                cur_price = float(pos.get("current_price", 0) or 0)
                pid = await self._import_redeemable(
                    token_id=token_id,
                    market_id=market_id,
                    size=size,
                    avg_price=avg_price,
                    exit_price=cur_price,
                    now_ts=now_ts,
                )
                if pid is not None:
                    self.stats.imported_redeemable_total += 1
                    logger.info(
                        "position_importer: imported redeemable "
                        "position %s (token=%s, shares=%.4f, "
                        "exit_price=$%.4f, pnl=$%.4f) — handed off "
                        "to redeemer",
                        pid, token_id, size, cur_price,
                        float(pos.get("pnl", 0) or 0),
                    )
                continue
            # Dedupe — already tracked? (covers both bot-opened AND
            # previously-imported active positions.)
            if await self._positions.has_open_position_for_token(token_id):
                continue

            cost_basis = avg_price * size

            # 2026-05-07 PHASE 12 — dust skip. Polymarket V2 has a $1
            # minimum SELL size. Importing positions below $1 cost
            # basis triggers an infinite phantom-loop: importer sees
            # them, ProfitTaker fires SELL, SDK refuses to sign
            # (below V2 min), close_position records fictional loss,
            # next sweep recreates them. Pos 22371-22374 (May 7 v16)
            # surfaced this — 4 phantoms in 4 minutes for the same
            # 3.1 stuck shares. Skip below threshold; those shares
            # are dust pending resolution / redemption.
            if cost_basis < self._cfg.dust_threshold_usd:
                self.stats.dust_skipped += 1
                logger.info(
                    "position_importer: skipping dust position "
                    "(token=%s..., shares=%.4f, avg_price=$%.4f, "
                    "cost=$%.4f < $%.2f V2 min — Phase 12 dust gate)",
                    token_id[:20], size, avg_price, cost_basis,
                    self._cfg.dust_threshold_usd,
                )
                continue

            # 2026-05-09 PHASE 31 — reconciliation lock.
            # If execution recorded a terminal SELL_FAILED on this
            # token (escalation exhausted, on-chain BUY confirmed but
            # SELL never matched), there's an active quarantine lock.
            # Skip — those leftover on-chain shares are NOT a new
            # position; they are the same shares from the failed SELL.
            # Closes the v50r2/v51/v52 phantom-double chain
            # (22493/22495/22497). The redeemer will resolve the
            # underlying position and clear the lock; on the next
            # sweep after clear, legacy import behavior resumes.
            if self._reconciliation_lock_repo is not None:
                try:
                    lock = (
                        await self._reconciliation_lock_repo.get_active(
                            token_id, now_ts,
                        )
                    )
                except Exception:
                    logger.exception(
                        "position_importer: reconciliation_lock "
                        "lookup failed for token %s (failing open)",
                        token_id,
                    )
                    lock = None
                if lock is not None:
                    self.stats.reconciliation_lock_skipped += 1
                    logger.info(
                        "position_importer: skipping import for token "
                        "%s... — active reconciliation lock "
                        "(position_id=%s, reason=%s, expires_at=%d)",
                        token_id[:20], lock.position_id, lock.reason,
                        lock.expires_at,
                    )
                    continue

            # 2026-05-07 PHASE 14 — SELL settlement window.
            # If we recently signed a SELL for this token, the chain
            # likely hasn't reflected the resulting balance decrease
            # yet. /positions returns the pre-sell shares; without
            # this gate the importer creates a phantom (22441/22443/
            # 22446 on May 7). Skip until the matcher settles. The
            # next sweep (60s later by default) re-evaluates: if the
            # chain has caught up, on_chain payload will no longer
            # include this token; if not, we wait again.
            since_ts = now_ts - self._cfg.recent_sell_window_s
            if await self._live_orders.has_recent_sell_for_token(
                token_id, since_ts,
            ):
                self.stats.recent_sell_skipped += 1
                logger.info(
                    "position_importer: skipping import for token "
                    "%s... — SELL signed in last %ds, chain not yet "
                    "settled (Phase 14 settlement-window gate)",
                    token_id[:20], self._cfg.recent_sell_window_s,
                )
                continue

            entry_intent_id = f"imported:{token_id}"

            # 1. positions row.
            try:
                pid = await self._positions.open_position(
                    PositionRow(
                        market_id=market_id,
                        token_id=token_id,
                        side="BUY",
                        entry_price=avg_price,
                        shares=size,
                        cost_basis_usd=cost_basis,
                        entry_intent_id=entry_intent_id,
                        entry_ts=now_ts,
                    )
                )
            except Exception:
                logger.exception(
                    "position_importer: positions.open_position failed "
                    "for token %s", token_id,
                )
                self.stats.errors += 1
                continue

            # 2. Synthetic live_orders BUY row so the SELL inventory
            # gate (_handle_live_sell) sees this position as having
            # on-chain inventory. Without this, ProfitTakerAgent's
            # SELL_INTENT for the imported position would be skipped
            # by the gate and never actually fire on Polymarket.
            client_order_id = f"poly-v3-{entry_intent_id}"
            try:
                await self._live_orders.insert(
                    LiveOrderRow(
                        intent_id=entry_intent_id,
                        strategy="imported",
                        market_id=market_id,
                        token_id=token_id,
                        side="BUY",
                        limit_price=avg_price,
                        size_usd=cost_basis,
                        shares=size,
                        order_type="GTC",  # nominal — this row never POSTs
                        mode="LIVE",
                        client_order_id=client_order_id,
                        signed_order_json=json.dumps(
                            {"imported": True, "source": "data_api/positions"}
                        ),
                        signed_at=now_ts,
                        status="signed",
                    )
                )
            except sqlite3.IntegrityError:
                # Re-import of a token that has a leftover live_orders
                # row from a prior session (positions row was deleted
                # but live_orders persisted). The existing row is fine
                # — just promote it to filled and continue.
                logger.info(
                    "position_importer: live_orders row for %s already "
                    "exists, reusing", client_order_id,
                )
            try:
                # Promote to filled (idempotent — UPDATE of an
                # already-filled row is a no-op).
                await self._mark_imported_buy_filled(
                    client_order_id=client_order_id,
                    fill_qty=size,
                    fill_price=avg_price,
                    ts=now_ts,
                )
            except Exception:
                logger.exception(
                    "position_importer: synthetic-fill UPDATE failed "
                    "for %s — exit gate may block SELL",
                    client_order_id,
                )
                self.stats.errors += 1
                # Position row already committed; leave it open.

            # 3. Subscribe market WS so ticks reach the exit agents.
            if self._market_ws is not None:
                try:
                    self._market_ws.subscribe_tokens([token_id])
                except Exception:
                    logger.exception(
                        "position_importer: market_ws subscribe failed "
                        "for token %s", token_id,
                    )

            # 4. Publish EVT_POSITION_OPENED so ProfitTakerAgent +
            # ExitDecisionEngine start tracking.
            await self._bus.publish(
                EVT_POSITION_OPENED,
                {
                    "position_id": pid,
                    "token_id": token_id,
                    "market_id": market_id,
                    "side": "BUY",
                    "entry_price": avg_price,
                    "shares": size,
                    "cost_basis_usd": cost_basis,
                    "strategy": "imported",
                },
            )
            imported += 1
            logger.info(
                "position_importer: imported position %s "
                "(token=%s, shares=%.4f, avg_price=$%.4f, cost=$%.4f)",
                pid, token_id, size, avg_price, cost_basis,
            )

        self.stats.sweeps += 1
        self.stats.imported_total += imported
        self.stats.last_sweep_ts = now_ts

        # 2026-05-03 P1 #1 fix (deep-research-report 17 §delta-sweep):
        # detect imported tokens whose on-chain inventory has dropped
        # to 0 (operator manually closed on Polymarket UI AND the User
        # WS unmatched-SELL event was missed). Close those positions
        # with outcome='INVENTORY_GONE' so ProfitTaker stops firing
        # SELLs against zero on-chain inventory ("balance: 0" loops).
        # Only acts on imported positions — bot-managed positions are
        # owned by ProfitTaker/SELL-escalator and may have legitimate
        # in-flight close attempts (their close authority lives there).
        try:
            await self._delta_sweep(on_chain, now_ts)
        except Exception:
            logger.exception("position_importer: delta_sweep failed")
            self.stats.errors += 1

        return imported

    async def _delta_sweep(
        self, on_chain: list[dict[str, Any]], now_ts: int
    ) -> int:
        """Close open IMPORTED positions whose token is no longer in
        the wallet's on-chain position list. Returns count closed.

        Conservative: only acts when the on-chain fetch itself
        succeeded and returned a non-empty list. An empty `on_chain`
        could be a Data API hiccup and we don't want to mass-close
        every imported position on a transient empty response.
        """
        if not on_chain:
            return 0
        # Set of tokens still on-chain (size above dust threshold).
        on_chain_tokens = {
            str(p.get("token_id"))
            for p in on_chain
            if str(p.get("token_id"))
            and float(p.get("size", 0) or 0) >= self._cfg.min_size
        }
        # Tokens we have OPEN imported positions for.
        tracked = await self._positions.fetch_all_open_imported_tokens()
        gone = [t for t in tracked if t not in on_chain_tokens]
        if not gone:
            return 0
        closed_total = 0
        for token_id in gone:
            try:
                n = await self._positions.close_open_for_token(
                    token_id=token_id,
                    outcome="INVENTORY_GONE",
                    closed_ts=now_ts,
                    only_imported=True,
                )
            except Exception:
                logger.exception(
                    "position_importer: close_open_for_token failed for %s",
                    token_id,
                )
                self.stats.errors += 1
                continue
            if n > 0:
                closed_total += n
                logger.info(
                    "position_importer: delta-sweep closed %d imported "
                    "position(s) for token %s — on-chain inventory gone "
                    "(manual close or redemption that the User WS missed)",
                    n, token_id,
                )
                # 2026-05-06 PHASE 8 — fill reconciliation. The bot
                # closed the position with INVENTORY_GONE + $0
                # realized because chain-state only tells us "shares
                # are gone", not what they sold for. Poll Polymarket
                # /activity for the actual SELL trade(s) and restate
                # exit_price + realized_pnl. Best-effort: API
                # failures are logged but never block the close.
                if self._activity_fetcher is not None:
                    try:
                        await self._reconcile_inventory_gone_token(
                            token_id=token_id,
                        )
                    except Exception:
                        logger.exception(
                            "position_importer: Phase 8 reconcile "
                            "failed for token %s — keeping $0 "
                            "phantom realized", token_id,
                        )
                        self.stats.errors += 1
        if closed_total:
            self.stats.delta_sweep_closed += closed_total
        return closed_total

    async def _reconcile_inventory_gone_token(
        self, token_id: str,
    ) -> None:
        """Phase 8 — recover the actual SELL fill price for a token
        whose on-chain inventory is gone.

        Polls /activity?user=<funder>&limit=20 and looks for SELL
        TRADE events on this token. Aggregates total size + total
        usdcSize, computes weighted-avg fill price, and restates the
        most recently-closed imported position via
        `positions_repo.restate_close_price`.

        Conservative: only acts when the matching trades aggregate
        to a meaningful size (>= dust threshold). No data → leave the
        position at $0 realized + INVENTORY_GONE outcome.
        """
        if self._activity_fetcher is None:
            return
        # Polymarket /activity returns newest-first. 20 entries is
        # usually enough to cover a recent close — most positions
        # exit within a few minutes of opening.
        items = await self._activity_fetcher(self._funder, 20)
        if not items:
            return
        total_size = 0.0
        total_usdc = 0.0
        for it in items:
            if str(it.get("type", "")).upper() != "TRADE":
                continue
            if str(it.get("side", "")).upper() != "SELL":
                continue
            if str(it.get("asset", "")) != token_id:
                continue
            try:
                size = float(it.get("size") or 0)
                usdc = float(it.get("usdcSize") or 0)
            except (TypeError, ValueError):
                continue
            if size <= 0 or usdc <= 0:
                continue
            total_size += size
            total_usdc += usdc
        if total_size < self._cfg.min_size:
            return
        avg_fill_price = total_usdc / total_size

        # Find the most recently-closed imported position for this
        # token (the one we just closed via delta-sweep).
        async with self._positions._db.connect() as conn:  # type: ignore[attr-defined]
            cur = await conn.execute(
                "SELECT position_id FROM positions "
                "WHERE token_id=? AND outcome='INVENTORY_GONE' "
                "  AND closed_ts IS NOT NULL "
                "ORDER BY closed_ts DESC LIMIT 1",
                (token_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return
        position_id = int(row[0])
        try:
            updated = await self._positions.restate_close_price(
                position_id=position_id,
                actual_exit_price=avg_fill_price,
            )
        except Exception:
            logger.exception(
                "position_importer: restate_close_price failed for "
                "position %s", position_id,
            )
            return
        if updated is not None:
            self.stats.inventory_gone_reconciled += 1
            logger.info(
                "position_importer: Phase 8 reconciled position %d "
                "(token %s) — actual SELL %.0f shares @ $%.4f avg "
                "(usdc=%.4f). exit_price restated, realized_pnl "
                "now reflects on-chain truth instead of $0 phantom.",
                position_id, token_id, total_size,
                avg_fill_price, total_usdc,
            )

    async def _import_redeemable(
        self,
        *,
        token_id: str,
        market_id: str,
        size: float,
        avg_price: float,
        exit_price: float,
        now_ts: int,
    ) -> int | None:
        """Insert a pre-closed positions row + synthetic filled BUY
        for a redeemable on-chain position. The redeemer agent will
        pick it up on its next sweep and classify WORTHLESS / REDEEMABLE
        based on the Gamma resolution.

        Doesn't emit EVT_POSITION_OPENED (position is already closed)
        nor subscribe market WS (no exits to monitor).
        """
        cost_basis = avg_price * size
        entry_intent_id = f"imported:{token_id}"
        try:
            pid = await self._positions.open_position(
                PositionRow(
                    market_id=market_id,
                    token_id=token_id,
                    side="BUY",
                    entry_price=avg_price,
                    shares=size,
                    cost_basis_usd=cost_basis,
                    entry_intent_id=entry_intent_id,
                    entry_ts=now_ts,
                )
            )
        except Exception:
            logger.exception(
                "position_importer: open_position failed for "
                "redeemable token %s", token_id,
            )
            self.stats.errors += 1
            return None
        try:
            # Outcome label is just informational; redeemer will
            # re-classify against Gamma's resolution. Use 'TIME'
            # (the BarResolutionWatcher equivalent — market ended).
            await self._positions.close_position(
                position_id=pid,
                exit_price=exit_price,
                outcome="TIME",
                closed_ts=now_ts,
            )
        except Exception:
            logger.exception(
                "position_importer: close_position failed for "
                "redeemable %s — row will linger as open until "
                "next reconciliation", pid,
            )
            self.stats.errors += 1
            # Don't bail; row is in the DB as open. Operator can fix.
        # Synthetic live_orders BUY so the redeemer's inventory gate
        # (which mirrors _handle_live_sell's filled-qty check)
        # confirms on-chain inventory.
        client_order_id = f"poly-v3-{entry_intent_id}"
        try:
            await self._live_orders.insert(
                LiveOrderRow(
                    intent_id=entry_intent_id,
                    strategy="imported",
                    market_id=market_id,
                    token_id=token_id,
                    side="BUY",
                    limit_price=avg_price,
                    size_usd=cost_basis,
                    shares=size,
                    order_type="GTC",
                    mode="LIVE",
                    client_order_id=client_order_id,
                    signed_order_json=json.dumps(
                        {"imported": True, "redeemable": True}
                    ),
                    signed_at=now_ts,
                    status="signed",
                )
            )
        except sqlite3.IntegrityError:
            # Re-import collision (same as the active-position branch
            # above) — the existing live_orders row is fine, just
            # promote it to filled.
            logger.info(
                "position_importer: live_orders row for redeemable %s "
                "already exists, reusing", client_order_id,
            )
        try:
            await self._mark_imported_buy_filled(
                client_order_id=client_order_id,
                fill_qty=size,
                fill_price=avg_price,
                ts=now_ts,
            )
        except Exception:
            logger.exception(
                "position_importer: synthetic-fill UPDATE failed for "
                "redeemable %s — redeemer's inventory gate may skip it",
                pid,
            )
            self.stats.errors += 1
        return pid

    async def _mark_imported_buy_filled(
        self,
        *,
        client_order_id: str,
        fill_qty: float,
        fill_price: float,
        ts: int,
    ) -> None:
        """Direct UPDATE — bypass record_fill (which keys on
        polymarket_order_id we don't have for an imported position)."""
        async with self._live_orders._db.connect() as conn:  # type: ignore[attr-defined]
            await conn.execute(
                """
                UPDATE live_orders
                   SET status='filled',
                       filled_qty=?,
                       avg_fill_price=?,
                       submitted_at=?
                 WHERE client_order_id=?
                """,
                (fill_qty, fill_price, ts, client_order_id),
            )
            await conn.commit()
