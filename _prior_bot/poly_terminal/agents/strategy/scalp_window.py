"""Scalp on short-window crypto Up/Down binaries (15m or 1h).

Strategy: enter inside an entry window after bar start (5–20 min into a 1h
bar; 5–10 min into a 15m bar), exit at least N seconds before bar close
(handled by ExitConfig max_hold). One intent per (asset, bar) — debounce.

Watchlist payload (from Discovery):
  {
    "markets": [
      {"asset": "btc", "window": "15m", "market_id": ..., "token_yes": ...,
       "token_no": ..., "bar_start_ts": ..., "bar_end_ts": ...,
       "end_date_iso": ...},
      ...
    ]
  }
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from poly_terminal.agents.risk.intent import BuyIntent
from poly_terminal.agents.strategy.base import BaseStrategy
from poly_terminal.agents.strategy.debounce import OneIntentPerBar
from poly_terminal.agents.strategy.exit_config import for_strategy
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_BUY_INTENT,
    EVT_CONTEXT_BLOCK,
    EVT_CONTEXT_OK,
    EVT_MARKET_TICK,
    EVT_WATCHLIST_UPDATED,
)
from poly_terminal.shared.enums import IntentSide, IntentSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScalpConfig:
    window: str = "15m"  # "15m" or "1h"
    size_usd: Decimal = Decimal("10")
    # Entry window relative to bar_start_ts.
    entry_after_start_s: int = 300                 # don't enter before this
    # Exit window before bar_end_ts (no entries here).
    exit_before_end_s: int = 300
    # 2026-05-11 PHASE 38 — entry-price bleed-band filter. When
    # `bleed_band_lo < bleed_band_hi`, intents whose tick price falls
    # in `[bleed_band_lo, bleed_band_hi]` (inclusive both ends) are
    # rejected.
    #
    # The 2026-05-11 drilldown found scalp_window's 0.60-0.80 band
    # was -2.09% ROI vs +5.67% in 0.40-0.60 and +5.65% in <0.20 —
    # filtering 0.60-0.80 lifts projected ROI from +3.12% to ~+4.3%
    # on the same fill universe.
    #
    # Default lo=hi=0.0 → degenerate range → filter OFF. Opt in by
    # setting both via preset YAML or env.
    bleed_band_lo: float = 0.0
    bleed_band_hi: float = 0.0

    # 2026-05-14 PHASE 41.6 — entry-price band filter.
    #
    # Reject ticks whose price falls OUTSIDE `[entry_price_lo,
    # entry_price_hi]`. Symmetric counterpart to the bleed-band: that
    # filter blocks a middle "loss zone"; this filter blocks the
    # extreme tails.
    #
    # Motivation: the first patched-code overnight (2026-05-13 →
    # 2026-05-14) produced 19 fills, -$4.90 PnL. One catastrophic
    # WORTHLESS_REDEEM (NO bought at 0.07 with 10min to bar resolution
    # → Phase 33 truth-up wrote -$5) accounted for the entire swing
    # vs. ~break-even on the other 18 fills. The bleed-band
    # [0.60, 0.80] didn't catch it because 0.07 is well below the
    # band. The defensive fix is a floor — block lottery-ticket
    # entries that have an asymmetric tail-risk profile (huge upside
    # if right, total loss if wrong, with thin liquidity at the
    # extreme end making paper-mode SELLs fictional).
    #
    # Default lo=hi=0.0 → degenerate → filter OFF for backwards
    # compat. Opt in via preset YAML or env:
    #   SCALP_WINDOW_ENTRY_PRICE_LO=0.10
    #   SCALP_WINDOW_ENTRY_PRICE_HI=1.00  (or leave 0.0 for "no
    #                                       upper limit")
    # When lo>0 and hi<=0 (or hi<lo), only the lower floor is
    # enforced. When both are set and hi>lo, an inclusive band is
    # required: lo <= price <= hi.
    entry_price_lo: float = 0.0
    entry_price_hi: float = 0.0

    # 2026-05-16 PHASE 41.8 — min-time-to-resolution entry gate.
    #
    # HONEST LABEL: this is a MITIGATION, not the root-cause fix. The
    # confirmed root cause (traced in code, cases 23398 + 23417) is
    # paper-sim exit-fill FIDELITY: profit_taker books a ~flat exit
    # ~11s after entry at a tick price that would not realistically
    # clear near a binary resolution; the bar then resolves worthless
    # and Phase-33 truth-up rewrites the books to -100%. A time gate
    # cannot fix that mechanism — it only refuses entries that lack
    # the runway for a real scalp to plausibly complete, reducing
    # exposure to the pathology.
    #
    # CRITICAL MATH (state plainly, do not hide): the existing gates
    # allow entries in [bar_start+entry_after_start_s,
    # bar_end-exit_before_end_s]. On a 15m bar (900s) with the
    # defaults (300/300) the EARLIEST allowed entry is already only
    # 600s before resolution — so ANY min_seconds_to_resolution > 600
    # eliminates ALL 15m-bar entries. The two observed catastrophes
    # entered at ~576s and ~600s to resolution — i.e. at the
    # max-possible-runway end of the 15m window. Conclusion: the 15m
    # scalp is STRUCTURALLY a near-resolution binary gamble; no time
    # gate can keep 15m trading AND block these losses. Setting this
    # to a catastrophe-blocking value (>=900) is therefore equivalent
    # to "trade 1h bars only" — which is the honest signal, not a
    # bug. The 1h bar (3600s) still permits entries with >=900s to
    # resolution.
    #
    # Default 0 → gate OFF (backwards-compat, like the bands above).
    # Opt in via preset/env: SCALP_WINDOW_MIN_SECONDS_TO_RESOLUTION
    # (recommended 900 — and prefer also just disabling
    # STRATEGY_SCALP_15M, which is the cleaner expression of the same
    # decision).
    min_seconds_to_resolution: int = 0


@dataclass
class _Market:
    asset: str
    market_id: str
    token_yes: str
    token_no: str
    bar_start_ts: int
    bar_end_ts: int
    end_date_iso: str


class ScalpWindowStrategy(BaseStrategy):
    name = "scalp_window"

    def __init__(
        self,
        bus: EventBus,
        cfg: ScalpConfig | None = None,
        *,
        # 2026-05-10 Phase 32 P3 — RiskAllocator gate (BaseStrategy).
        allocator: Any | None = None,
        mode_getter: Any | None = None,
        ledger_snapshot_getter: Any | None = None,
    ) -> None:
        super().__init__(
            bus,
            allocator=allocator,
            mode_getter=mode_getter,
            ledger_snapshot_getter=ledger_snapshot_getter,
        )
        self._cfg = cfg or ScalpConfig()
        # token_id → market record (for either YES or NO leg)
        self._token_to_market: dict[str, _Market] = {}
        self._market_blocked: set[str] = set()
        self._debounce = OneIntentPerBar()

    async def _subscribe(self) -> None:
        self._bus.subscribe(EVT_WATCHLIST_UPDATED, self._on_watchlist)
        self._bus.subscribe(EVT_CONTEXT_OK, self._on_ctx_ok)
        self._bus.subscribe(EVT_CONTEXT_BLOCK, self._on_ctx_block)
        self._bus.subscribe(EVT_MARKET_TICK, self._on_tick)

    async def _on_watchlist(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        for m in payload.get("markets", []):
            if str(m.get("window", "")) != self._cfg.window:
                continue
            mk = _Market(
                asset=str(m.get("asset", "")),
                market_id=str(m.get("market_id", "")),
                token_yes=str(m.get("token_yes", "")),
                token_no=str(m.get("token_no", "")),
                bar_start_ts=int(m.get("bar_start_ts", 0)),
                bar_end_ts=int(m.get("bar_end_ts", 0)),
                end_date_iso=str(m.get("end_date_iso", "")),
            )
            for tok in (mk.token_yes, mk.token_no):
                if tok:
                    self._token_to_market[tok] = mk

    async def _on_ctx_ok(self, _e: str, payload: Any) -> None:
        if isinstance(payload, dict):
            self._market_blocked.discard(str(payload.get("market_id", "")))

    async def _on_ctx_block(self, _e: str, payload: Any) -> None:
        if isinstance(payload, dict):
            self._market_blocked.add(str(payload.get("market_id", "")))

    async def _on_tick(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        token = str(payload.get("token_id", ""))
        market = self._token_to_market.get(token)
        if market is None:
            return
        if market.market_id in self._market_blocked:
            return
        try:
            ts = int(payload.get("ts", 0))
            price = Decimal(str(payload["price"]))
        except (KeyError, TypeError, ValueError):
            return
        if ts < market.bar_start_ts + self._cfg.entry_after_start_s:
            return
        if ts > market.bar_end_ts - self._cfg.exit_before_end_s:
            return
        # 2026-05-16 PHASE 41.8 — min-time-to-resolution gate
        # (MITIGATION, not root-cause cure — see ScalpConfig). Refuse
        # a NEW entry that lacks the runway for a real scalp to
        # complete before binary resolution, capping exposure to the
        # paper-sim fictional-exit pathology. OFF when == 0.
        if self._cfg.min_seconds_to_resolution > 0:
            ttr = market.bar_end_ts - ts
            if ttr < self._cfg.min_seconds_to_resolution:
                logger.info(
                    "%s: min-ttr reject — %ds to resolution < "
                    "min %ds (token=%s)",
                    self.name, ttr,
                    self._cfg.min_seconds_to_resolution, token,
                )
                return
        # 2026-05-11 PHASE 38 — bleed-band entry-price gate. Reject
        # ticks whose price lies inside the operator-configured loss
        # zone. The gate is a no-op when lo>=hi (default), so existing
        # deployments are unaffected until they opt in.
        if self._cfg.bleed_band_lo < self._cfg.bleed_band_hi:
            price_f = float(price)
            if (
                self._cfg.bleed_band_lo <= price_f
                <= self._cfg.bleed_band_hi
            ):
                logger.info(
                    "%s: bleed-band reject — price=%.4f in "
                    "[%.4f, %.4f] (token=%s)",
                    self.name, price_f, self._cfg.bleed_band_lo,
                    self._cfg.bleed_band_hi, token,
                )
                return
        # 2026-05-14 PHASE 41.6 — entry-price floor / ceiling. Reject
        # ticks whose price falls OUTSIDE the configured band. Defaults
        # are both 0.0 → gate OFF. When lo>0, enforce a floor (block
        # lottery-ticket entries below it). When hi>0 AND hi>=lo,
        # additionally enforce a ceiling. The gate stays off entirely
        # when both are 0.0.
        if self._cfg.entry_price_lo > 0.0 or self._cfg.entry_price_hi > 0.0:
            price_f = float(price)
            if (
                self._cfg.entry_price_lo > 0.0
                and price_f < self._cfg.entry_price_lo
            ):
                logger.info(
                    "%s: entry-price reject — price=%.4f below "
                    "floor %.4f (token=%s)",
                    self.name, price_f, self._cfg.entry_price_lo, token,
                )
                return
            if (
                self._cfg.entry_price_hi > 0.0
                and self._cfg.entry_price_hi >= self._cfg.entry_price_lo
                and price_f > self._cfg.entry_price_hi
            ):
                logger.info(
                    "%s: entry-price reject — price=%.4f above "
                    "ceiling %.4f (token=%s)",
                    self.name, price_f, self._cfg.entry_price_hi, token,
                )
                return
        if not self._debounce.should_emit(market.asset, market.bar_start_ts):
            return

        intent = BuyIntent(
            intent_id=str(uuid.uuid4()),
            strategy=self.name,
            market_id=market.market_id,
            token_id=token,
            side=IntentSide.BUY,
            size_usd=self._cfg.size_usd,
            limit_price=price,
            source=IntentSource.SCALP_WINDOW,
            created_at=float(ts),
            end_date_iso=market.end_date_iso,
            exit_config=for_strategy(
                "scalp_15m" if self._cfg.window == "15m" else "scalp_1h"
            ),
        )

        # 2026-05-10 Phase 32 P3 — RiskAllocator gate.
        if not self._allocator_approves_intent(
            market_id=market.market_id,
            token_id=token,
            size_usd=float(self._cfg.size_usd),
            marketable_price=float(price),
        ):
            return

        await self._bus.publish(EVT_BUY_INTENT, intent)
        self.intents_emitted += 1
