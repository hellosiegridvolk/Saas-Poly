"""Copy-Scalp strategy — copy a separate wallet set, but trade as scalp.

Same fundamental signal as CopyTradeStrategy (a followed wallet's BUY
fires our BUY) but tuned for short-duration trades:

  - Smaller per-trade size (proportion + hard cap), so we can absorb
    losses on a fast-turn high-frequency strategy without blowing the
    daily loss cap.
  - Tighter ExitConfig profile via `strategy="copy_scalp"` (registered
    in exit_config.EXIT_CONFIGS) — short max_hold, lower TP, lower SL
    so positions exit before the next signal arrives.
  - Independent followed-wallet set (`WALLET_COPY_SCALP_OVERRIDE`).
    Lets the operator point CopyScalp at a different cohort than
    CopyTrade — typically high-frequency micro-traders that wouldn't
    qualify for the longer-hold copy_trade pool.

Pipeline:
  EVT_WALLET_FILL → both CopyTrade and CopyScalp see it → each routes
  on its own followed set → independent intents emitted with their
  respective `strategy=` tag → ExitDecisionEngine picks the matching
  ExitConfig.

Wallet polling: WalletActivityPoller polls the UNION of (rank-derived
copy_trade set ∪ copy_scalp override set) so all configured wallets
get covered without a second poller instance.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from poly_terminal.agents.risk.intent import BuyIntent
from poly_terminal.agents.strategy.copy_trade import (
    CopyTradeConfig,
    CopyTradeStrategy,
)
from poly_terminal.agents.strategy.exit_config import for_strategy
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_BUY_INTENT
from poly_terminal.shared.enums import IntentSide, IntentSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CopyScalpConfig(CopyTradeConfig):
    """Tighter caps than CopyTradeConfig for scalp-style trades.

    Inherits everything from CopyTradeConfig (so the V2 floor logic,
    fail-open imbalance, max_buy_price filter etc. all apply) but
    overrides:
      - `proportion`  — smaller (5% of source vs 30% for copy_trade)
      - `max_position_usd` — half of copy_trade
      - `max_position_usd_hard` — half of copy_trade
      - `max_buy_price` — slightly tighter ($0.80 vs $0.85) since
        scalp downside on high-priced binaries hurts more
    """

    proportion: Decimal = Decimal("0.05")
    max_position_usd: Decimal = Decimal("20")       # non-binding; shares cap governs
    max_position_usd_hard: Decimal | None = Decimal("20")
    max_buy_price: Decimal | None = Decimal("0.80")
    max_shares: int = 10                            # hard cap: 10 shares per position


class CopyScalpStrategy(CopyTradeStrategy):
    """Wallet-signal entry, scalp exit profile. Separate wallet pool."""

    name = "copy_scalp"

    def __init__(
        self,
        bus: EventBus,
        followed_wallets: set[str],
        cfg: CopyScalpConfig | None = None,
        best_ask_getter: Callable[[str], float | None] | None = None,
        # 2026-05-10 Phase 32 P3 — RiskAllocator gate (mirror copy_trade).
        # Pass-through to the parent so `_allocator_approves_intent`
        # works identically for copy_scalp signals. Default None
        # preserves legacy behavior (tests + paper-only).
        allocator: Any | None = None,
        mode_getter: Any | None = None,
        ledger_snapshot_getter: Any | None = None,
    ) -> None:
        super().__init__(
            bus,
            cfg=cfg or CopyScalpConfig(),
            best_ask_getter=best_ask_getter,
            allocator=allocator,
            mode_getter=mode_getter,
            ledger_snapshot_getter=ledger_snapshot_getter,
        )
        # Pin the followed set at construction; CopyScalp does NOT
        # follow EVT_WALLET_RANK_CHANGED (that drives the parent
        # copy_trade set). The operator picks this cohort explicitly
        # via WALLET_COPY_SCALP_OVERRIDE so the scalp pool can be
        # high-frequency wallets that wouldn't earn into the
        # long-hold copy_trade decile.
        self._followed = {w.lower() for w in followed_wallets}

    async def _subscribe(self) -> None:
        # Skip the parent's EVT_WALLET_RANK_CHANGED subscription —
        # CopyScalp's followed set is fixed at boot. Otherwise wire
        # the same orderbook-imbalance + context + wallet-fill
        # subscriptions so the gating logic stays identical.
        from poly_terminal.bus.events import (
            EVT_BOOK_IMBALANCE,
            EVT_CONTEXT_BLOCK,
            EVT_CONTEXT_OK,
            EVT_WALLET_FILL,
        )
        self._bus.subscribe(EVT_BOOK_IMBALANCE, self._on_imbalance)
        self._bus.subscribe(EVT_CONTEXT_OK, self._on_ctx_ok)
        self._bus.subscribe(EVT_CONTEXT_BLOCK, self._on_ctx_block)
        self._bus.subscribe(EVT_WALLET_FILL, self._on_wallet_fill)

    async def _on_wallet_fill(self, _e: str, payload: Any) -> None:
        """Mirror copy_trade's filter logic but emit with strategy=copy_scalp.

        Entire body is intentionally a copy of CopyTradeStrategy._on_wallet_fill
        with two changes:
          1. `strategy=self.name` (which is 'copy_scalp', not 'copy_trade')
             so EVT_BUY_INTENT downstream picks the scalp ExitConfig.
          2. Slug → exit_strategy mapping always returns the strategy
             name (no scalp_15m / scalp_1h override) since the entire
             point of CopyScalp IS the scalp profile.

        2026-05-09 PHASE PARITY — v53 pos 22498 (copy_scalp) lost on a
        5-min binary that copy_trade's gates would have rejected. The
        original copy was made before Phase 21/23/29/market-duration
        gates were added to copy_trade and silently drifted out of
        sync. Gates now mirrored here with the same semantics; tech
        debt to refactor into a shared helper is tracked separately.
        """
        if not isinstance(payload, dict):
            return
        wallet = str(payload.get("wallet", "")).lower()
        # 2026-05-09 PARITY: Phase 23(a) reversal cache is updated for
        # SELL fills BEFORE the followed-set check so reversal gating
        # works even when copy_scalp doesn't follow the seller wallet
        # (the cache key is per-wallet anyway).
        side = str(payload.get("side", "")).upper()
        if side == "SELL":
            try:
                ts = int(payload.get("ts", 0))
            except (TypeError, ValueError):
                ts = 0
            tok = str(payload.get("token_id", ""))
            if tok and ts > 0:
                self._recent_sell_ts[(wallet, tok)] = ts
            return
        if wallet not in self._followed:
            return
        if side != "BUY":
            return
        market_id = str(payload.get("market_id", ""))
        token_id = str(payload.get("token_id", ""))
        if not market_id or not token_id:
            return
        if market_id in self._market_blocked:
            return

        try:
            fill_ts = int(payload.get("ts", 0))
        except (TypeError, ValueError):
            return
        cache = self._latest_bid_imbalance.get(token_id)
        if self._cfg.require_imbalance:
            if cache is None:
                return
            if (fill_ts - cache.ts) > self._cfg.imbalance_max_age_s:
                return

        try:
            src_price = Decimal(str(payload.get("price", 0)))
            src_size_shares = Decimal(str(payload.get("size", 0)))
        except (TypeError, ValueError):
            return
        if src_price <= 0 or src_size_shares <= 0:
            return
        if (
            self._cfg.max_buy_price is not None
            and src_price >= self._cfg.max_buy_price
        ):
            return

        # ── 2026-05-09 PARITY GATES (mirrored from copy_trade) ────────
        # Phase 23(c) — wallet variance gate
        if self._cfg.wallet_avg_pnl_floor_pct is not None:
            avg_pnl = self._wallet_avg_pnl.get(wallet)
            if (
                avg_pnl is not None
                and avg_pnl < self._cfg.wallet_avg_pnl_floor_pct
            ):
                self.intents_rejected_low_quality += 1
                logger.info(
                    "%s: rejecting intent — wallet=%s avg_pnl=%.2f%% "
                    "below floor %.2f%%",
                    self.name, wallet, float(avg_pnl) * 100,
                    float(self._cfg.wallet_avg_pnl_floor_pct) * 100,
                )
                return

        # Phase 23(a) — wallet reversal gate
        if self._cfg.reversal_lookback_s is not None:
            last_sell = self._recent_sell_ts.get((wallet, token_id), 0)
            if last_sell > 0:
                age = fill_ts - last_sell
                if 0 <= age <= self._cfg.reversal_lookback_s:
                    self.intents_rejected_reversal += 1
                    logger.info(
                        "%s: rejecting intent — wallet=%s sold token=%s "
                        "%ds ago (within %ds reversal window)",
                        self.name, wallet, token_id, age,
                        self._cfg.reversal_lookback_s,
                    )
                    return

        # Phase 29(a) — penny-token trap protection
        if (
            self._cfg.penny_max_price is not None
            and src_price < self._cfg.penny_max_price
        ):
            eod = payload.get("end_date_iso")
            if eod:
                try:
                    from datetime import datetime as _dt
                    import time as _time
                    eod_str = str(eod).replace("Z", "+00:00")
                    eod_ts = int(_dt.fromisoformat(eod_str).timestamp())
                    secs_left = eod_ts - int(_time.time())
                    if secs_left < self._cfg.penny_min_time_to_resolution_s:
                        self.intents_rejected_penny_trap += 1
                        logger.info(
                            "%s: rejecting penny-token intent — "
                            "src=%.4f < penny_max=%.2f, bar resolves "
                            "in %ds (< penny floor %ds). wallet=%s",
                            self.name, float(src_price),
                            float(self._cfg.penny_max_price),
                            secs_left,
                            self._cfg.penny_min_time_to_resolution_s,
                            wallet,
                        )
                        return
                except (ValueError, TypeError):
                    pass

        # 2026-05-09 — absolute market-duration floor (Phase 23(b)
        # follow-up). Catches the gap Phase 23(b) misses: short-bar
        # entries at any price level. v53 pos 22498 fired here.
        if self._cfg.market_duration_floor_s is not None:
            eod = payload.get("end_date_iso")
            if eod:
                try:
                    from datetime import datetime as _dt
                    import time as _time
                    eod_str = str(eod).replace("Z", "+00:00")
                    eod_ts = int(_dt.fromisoformat(eod_str).timestamp())
                    secs_left = eod_ts - int(_time.time())
                    if secs_left < self._cfg.market_duration_floor_s:
                        self.intents_rejected_short_market += 1
                        logger.info(
                            "%s: rejecting intent — short-market gate: "
                            "bar resolves in %ds (< absolute floor %ds) "
                            "at price=%.4f. wallet=%s",
                            self.name, secs_left,
                            self._cfg.market_duration_floor_s,
                            float(src_price), wallet,
                        )
                        return
                except (ValueError, TypeError):
                    pass

        # Phase 23(b) — bar-resolution time-floor (price-conditional)
        if (
            self._cfg.min_time_to_resolution_s is not None
            and src_price >= self._cfg.time_to_resolution_min_price
        ):
            eod = payload.get("end_date_iso")
            if eod:
                try:
                    from datetime import datetime as _dt
                    import time as _time
                    eod_str = str(eod).replace("Z", "+00:00")
                    eod_ts = int(_dt.fromisoformat(eod_str).timestamp())
                    secs_left = eod_ts - int(_time.time())
                    if secs_left < self._cfg.min_time_to_resolution_s:
                        self.intents_rejected_near_resolution += 1
                        logger.info(
                            "%s: rejecting intent — bar resolves in "
                            "%ds (< floor %ds) at price=%.4f "
                            "(>=%.2f). wallet=%s",
                            self.name, secs_left,
                            self._cfg.min_time_to_resolution_s,
                            float(src_price),
                            float(self._cfg.time_to_resolution_min_price),
                            wallet,
                        )
                        return
                except (ValueError, TypeError):
                    pass

        # Phase 21 — pre-trade slippage gate
        if (
            self._best_ask_getter is not None
            and self._cfg.source_slippage_cap_pct is not None
        ):
            try:
                ask = self._best_ask_getter(token_id)
            except Exception:
                ask = None
            if ask is not None and ask > 0:
                slip = (Decimal(str(ask)) - src_price) / src_price
                if slip > self._cfg.source_slippage_cap_pct:
                    self.intents_rejected_slippage += 1
                    logger.info(
                        "%s: rejecting intent — best_ask=%.4f vs "
                        "source=%.4f (slip=%.2f%% > cap %.2f%%) "
                        "wallet=%s token=%s",
                        self.name, float(ask), float(src_price),
                        float(slip) * 100,
                        float(self._cfg.source_slippage_cap_pct) * 100,
                        wallet, token_id,
                    )
                    return
        # ── End parity gates ──────────────────────────────────────────

        src_size_usd = src_price * src_size_shares
        proposed = src_size_usd * self._cfg.proportion

        raw_floor = max(
            self._cfg.min_size_usd, self._cfg.min_shares * src_price
        )
        floor_usd = raw_floor * (Decimal("1") + self._cfg.floor_pad_pct)

        hard_ceiling = (
            self._cfg.max_position_usd_hard
            if self._cfg.max_position_usd_hard is not None
            else self._cfg.max_position_usd * Decimal("1.5")
        )
        if floor_usd > hard_ceiling:
            return

        size_usd = min(proposed, self._cfg.max_position_usd)
        if size_usd < floor_usd:
            logger.info(
                "copy_scalp: upsizing intent to V2 floor "
                "(proposed=$%.4f → floor=$%.4f, price=$%.4f, "
                "soft_cap=$%s, hard=$%s)",
                size_usd, floor_usd, src_price,
                self._cfg.max_position_usd, hard_ceiling,
            )
            size_usd = floor_usd

        marketable_price = min(
            src_price * (Decimal("1") + self._cfg.buy_price_uplift_pct),
            Decimal("0.99"),
        )

        intent = BuyIntent(
            intent_id=str(uuid.uuid4()),
            strategy=self.name,
            market_id=market_id,
            token_id=token_id,
            side=IntentSide.BUY,
            size_usd=size_usd,
            limit_price=marketable_price,
            source_wallet=wallet,
            source_size_usd=src_size_usd,
            source=IntentSource.COPY_TRADE,
            created_at=float(fill_ts),
            end_date_iso=payload.get("end_date_iso"),
            exit_config=for_strategy(self.name),
        )

        # 2026-05-10 Phase 32 P3 — RiskAllocator gate (BaseStrategy helper).
        if not self._allocator_approves_intent(
            market_id=str(market_id),
            token_id=str(token_id),
            size_usd=float(size_usd),
            marketable_price=float(marketable_price),
            extra={
                "wallet_address": wallet,
                "wallet_paper_fills_count": int(
                    payload.get("wallet_paper_fills_count", 0)
                ),
            },
        ):
            return

        await self._bus.publish(EVT_BUY_INTENT, intent)
        self.intents_emitted += 1
