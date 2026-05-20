"""Copy-trade strategy — primary income engine.

Fires `EVT_BUY_INTENT` only when ALL THREE confirmations agree:

  1. Source wallet is in the followed (top-decile) set
     (from `EVT_WALLET_RANK_CHANGED`).
  2. The token has had a recent BID-side imbalance signal (within
     `imbalance_max_age_s` seconds).
  3. The market's last context decision is OK (not BLOCK).

Sizing: `min(source_size_usd * proportion, max_position_usd)`.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

_SHORT_WINDOW_SLUG_RE = re.compile(r"-updown-(5m|15m)-\d+$")
_HOURLY_SLUG_RE = re.compile(r"-up-or-down-[a-z]+-\d+-\d{4}-\d+(am|pm)-et$")


def _exit_profile_for_slug(slug: str, default: str) -> str:
    """Map an event slug to the appropriate exit-profile name.

      btc-updown-15m-{ts}                 → 'scalp_15m'  (12 min max_hold)
      btc-updown-5m-{ts}                  → 'scalp_15m'  (12 min — no 5m profile)
      bitcoin-up-or-down-april-30-2026-7am-et  → 'scalp_1h' (50 min max_hold)

    Anything else (long-tail markets, politics, etc.) falls back to the
    caller's default — typically 'copy_trade' with a 24h max_hold.
    """
    if not slug:
        return default
    if _SHORT_WINDOW_SLUG_RE.search(slug):
        return "scalp_15m"
    if _HOURLY_SLUG_RE.search(slug):
        return "scalp_1h"
    return default

from poly_terminal.agents.orderbook_intel.imbalance import ImbalanceSignal
from poly_terminal.agents.risk.intent import BuyIntent
from poly_terminal.agents.strategy.allocator import (
    LedgerSnapshot,
    RiskAllocator,
)
from poly_terminal.agents.strategy.base import BaseStrategy
from poly_terminal.agents.strategy.exit_config import for_strategy
from poly_terminal.agents.strategy.framework import StrategySignal
from poly_terminal.bus.event_bus import EventBus
from poly_terminal.shared.enums import BotMode
from poly_terminal.bus.events import (
    EVT_BOOK_IMBALANCE,
    EVT_BUY_INTENT,
    EVT_CONTEXT_BLOCK,
    EVT_CONTEXT_OK,
    EVT_WALLET_FILL,
    EVT_WALLET_RANK_CHANGED,
)
from poly_terminal.shared.enums import IntentSide, IntentSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CopyTradeConfig:
    proportion: Decimal = Decimal("0.30")          # 30% of source size
    max_position_usd: Decimal = Decimal("10")
    imbalance_max_age_s: int = 60
    # Polymarket V2 enforces TWO floors on every order:
    #   - 5 shares (resting / non-marketable orders)
    #   - $1 USD notional (marketable BUYs)
    # We upsize copy-trade intents to whichever floor binds.
    min_size_usd: Decimal = Decimal("1.00")
    min_shares: Decimal = Decimal("5")
    # Float-drift pad: Polymarket recomputes notional as
    # `shares * price` server-side; an exact-$1 intent often lands as
    # $0.9999 after py-clob-client's float math, tripping the
    # marketable-min rejection. Pad the upsized floor by 5% so the
    # server's number lands safely above $1 (and well above 5 shares
    # × price). Caught by the very first $2-cap canary at price ≈ $0.20.
    floor_pad_pct: Decimal = Decimal("0.05")
    # Soft-elasticity ceiling. When the V2 floor would exceed
    # `max_position_usd`, normally we drop the intent — but at small
    # caps that means an entire price band ($0.40–$0.60 at $2 cap)
    # is unreachable. `max_position_usd_hard` lets the upsize
    # opportunistically use up to this hard ceiling. Set to None (or
    # equal to `max_position_usd`) to disable elasticity. Default
    # 1.5× the soft cap → at $2 cap, hard ceiling becomes $3, which
    # extends the tradeable price band from $0.40 to $0.60.
    max_position_usd_hard: Decimal | None = None
    # Hard cap on position size in shares (independent of USD cap).
    # When set, size_usd is clamped to max_shares * src_price after all
    # other sizing. V2 floor still takes priority (can't go below it).
    max_shares: int | None = None
    # Marketable BUY price uplift. WAS 5% — historical workaround for
    # stale `src_price` causing resting BUYs to never cross the spread.
    # 2026-05-02: ExecutionAgent now re-quotes against current best_ask
    # before submit (live_orders.LiveOrderClient.get_best_ask) and
    # clamps to `intent.limit_price * (1 + BUY_MAX_SLIP_PCT)`. Keeping
    # uplift here would double-pad: strategy bumps +5%, execution
    # bumps further to best_ask. Set to 0 to defer all marketability
    # decisions to the execution layer where current orderbook state
    # is known. Operators can re-enable for paper-only soaks where
    # ExecutionAgent's re-quote doesn't run.
    buy_price_uplift_pct: Decimal = Decimal("0")
    # High-price filter. Diagnostic on 2892 PAPER closes (PRE-2026-05-02
    # phantom-position bug):
    #   p<$0.20 → 42.7% win, +$0.31 avg PnL  (positive edge)
    #   p<$0.40 → 61.5% win, +$0.22 avg PnL  (positive edge)
    #   p<$0.60 → 52.5% win, +$0.05 avg PnL  (marginal)
    #   p<$0.80 → 66.9% win, +$0.15 avg PnL  (positive edge)
    #   p≥$0.80 → 41.7% win, **-$0.19 avg PnL  (negative edge)**
    # 2026-05-02 retune: relaxed from $0.75 to $0.85 since the prior
    # PnL stats came from phantom round-trips (no real fills behind
    # them — see commit notes for the optimistic-position bug fix).
    # Re-measure once real fills accumulate post-fix; further relax
    # to $0.90 or remove if real-data ROI in the $0.75-$0.85 band
    # is positive. None disables the gate entirely.
    max_buy_price: Decimal | None = Decimal("0.85")
    # Imbalance-gate behavior. Pre-2026-05-02 the strategy required a
    # cached BID imbalance for the wallet's traded token within
    # `imbalance_max_age_s`; without it, the intent was silently
    # dropped. But the market WS only subscribes to tokens Discovery
    # watches (BTC/ETH crypto-bar binaries) — the followed whales
    # trade across hundreds of OTHER tokens (politics, sports, news
    # events) for which we never have orderbook coverage, so the
    # imbalance gate dropped ~80% of intents.
    # Default fail-open: trust the wallet's signal as-is, treat
    # imbalance only as a boost confirmation when present. Set to
    # True to restore the strict gate (useful for non-copy strategies
    # where wallet identity isn't the primary signal).
    require_imbalance: bool = False
    # 2026-05-08 PHASE 21 — pre-trade slippage gate.
    # When a `best_ask_getter` callable is wired and the current
    # best_ask is more than this percentage above the source wallet's
    # fill price, the strategy abandons the copy. Late-copy on a
    # moving market produces fills well above source's entry, putting
    # the position into immediate adverse selection (negative
    # unrealized on entry, biased toward SL exits). Set to None to
    # disable. Fails open when the getter returns None or raises —
    # missing orderbook never blocks the wallet signal.
    source_slippage_cap_pct: Decimal | None = Decimal("0.05")
    # 2026-05-08 PHASE 23(a) — wallet reversal gate.
    # Track per-(wallet, token) recent SELL fills. When a BUY signal
    # arrives but the SAME wallet sold the SAME token within
    # `reversal_lookback_s`, abandon the copy — they're reversing
    # (catching a falling knife), not entering. Caught the v42 root
    # cause: wallet bought $0.59 then within 26s started SELLing as
    # the bar moved against them; we processed the BUY only and
    # entered $0.45 mid-collapse. Set to None to disable.
    reversal_lookback_s: int | None = 60
    # 2026-05-08 PHASE 23(b) — bar-resolution time-floor.
    # Reject when `(end_date_iso - now) < min_time_to_resolution_s`
    # AND `src_price >= time_to_resolution_min_price`. Short-window
    # crypto-bar binaries near resolution are a trap zone — price can
    # collapse from $0.45 to $0.06 in 70s (v42). Cheap out-of-the-
    # money tickets stay allowed (they can still flip up). Set to
    # None to disable.
    #
    # 2026-05-09 — bumped default 300 → 600 after v52 pos 22496
    # entered with bar_to_close=264s. The original 300s floor
    # was too tight given fill-latency between intent emit and
    # chain settlement (intent passes the gate at intent time, but
    # by chain settlement the bar may have counted down below the
    # floor). 600s leaves 5+ minutes of post-fill buffer.
    min_time_to_resolution_s: int | None = 600
    time_to_resolution_min_price: Decimal = Decimal("0.30")
    # 2026-05-09 — absolute market-duration floor (Phase 23(b) follow-up).
    # Reject ANY intent regardless of src_price when
    # `(end_date_iso - now) < market_duration_floor_s`. Catches the
    # gap Phase 23(b) misses: mid-price entries ($0.05-$0.29) that
    # bypass the 0.30 price floor.
    #
    # v50/v51/v52 lost -$9.05 on three near-resolution binaries
    # (Ethereum/XRP 5-15min bars). 2026-05-09 backtest of v50-v55
    # showed the ORIGINAL 1800s default also blocked the lone winner
    # (pos 22491, bar=741s, +$0.85 TP). Mirroring Phase 23(b)'s 600s
    # threshold preserves winners while still catching the trap
    # cases (pos 22492 at bar=2s, 22494 at 110s, 22496 at 264s,
    # 22498 at 344s — all < 600s). Set to None to disable.
    market_duration_floor_s: int | None = 600
    # 2026-05-08 PHASE 23(c) — wallet-quality variance gate.
    # When a wallet's rolling avg PnL per dollar (provided externally
    # via `set_wallet_avg_pnl`, typically by a periodic re-audit
    # task) is below this floor, demote the wallet — no copies fire.
    # Default 0% (no negative-expectancy wallets). Wallets without
    # data are KEPT (fail-open) so the auditor can backfill at its
    # own pace. Set to None to disable the gate entirely.
    wallet_avg_pnl_floor_pct: Decimal | None = Decimal("0")
    # 2026-05-08 PHASE 29(a) — penny-token trap protection.
    # Phase 23(b) only catches the high-priced near-resolution case
    # (≥ time_to_resolution_min_price = 0.30). v46 trapped at $0.01
    # entries on tokens crashing to $0 within 2 min — no exit
    # liquidity at any price level. These are the wallet's lottery-
    # ticket trades: rare 100x pumps offset by 99% wipeouts. At our
    # smaller position size we eat the wipeouts without enough wins.
    # Phase 29(a): block penny entries near resolution.
    # `penny_max_price`: src_price strictly below this is "penny"
    # `penny_min_time_to_resolution_s`: penny entries need at least
    #     this much remaining bar life to be allowed.
    # Set penny_max_price to None to disable.
    penny_max_price: Decimal | None = Decimal("0.05")
    penny_min_time_to_resolution_s: int = 1200  # 20 min


@dataclass
class _ImbalanceCache:
    side: str
    ts: int


class CopyTradeStrategy(BaseStrategy):
    name = "copy_trade"

    def __init__(
        self,
        bus: EventBus,
        cfg: CopyTradeConfig | None = None,
        best_ask_getter: Callable[[str], float | None] | None = None,
        # 2026-05-09 Phase 32 P3 — RiskAllocator gate (pass-through to BaseStrategy).
        allocator: RiskAllocator | None = None,
        mode_getter: Callable[[], BotMode] | None = None,
        ledger_snapshot_getter: Callable[[], LedgerSnapshot] | None = None,
    ) -> None:
        super().__init__(
            bus,
            allocator=allocator,
            mode_getter=mode_getter,
            ledger_snapshot_getter=ledger_snapshot_getter,
        )
        self._cfg = cfg or CopyTradeConfig()
        self._followed: set[str] = set()
        self._latest_bid_imbalance: dict[str, _ImbalanceCache] = {}
        self._market_blocked: set[str] = set()
        # 2026-05-08 PHASE 21 — optional pre-trade slippage gate.
        # If wired (typically `live_client.get_best_ask`), every
        # copy-eligible fill checks current ask vs source price and
        # rejects intents where the market has moved beyond
        # `cfg.source_slippage_cap_pct` since the source filled.
        # None = disabled (backward compat, paper-only tests).
        self._best_ask_getter = best_ask_getter
        # 2026-05-08 PHASE 23(a) — per-(wallet, token) recent SELL
        # cache. Updated every time we see EVT_WALLET_FILL with
        # side=SELL on a followed wallet. Used to reject BUYs when
        # the same wallet is reversing on the same token within
        # `cfg.reversal_lookback_s`.
        self._recent_sell_ts: dict[tuple[str, str], int] = {}
        # 2026-05-08 PHASE 23(c) — per-wallet rolling avg PnL.
        # Keyed by lowercase wallet address. Populated externally by
        # the periodic re-audit task (boot + every N minutes).
        # Wallets not in the dict default to allow (fail-open).
        self._wallet_avg_pnl: dict[str, Decimal] = {}
        # Counters exposed alongside `intents_emitted` for
        # /api/strategies and operator dashboards.
        self.intents_rejected_slippage: int = 0
        self.intents_rejected_reversal: int = 0
        self.intents_rejected_near_resolution: int = 0
        self.intents_rejected_low_quality: int = 0
        self.intents_rejected_penny_trap: int = 0  # Phase 29(a)
        # 2026-05-09 — absolute market-duration floor (Phase 23(b) follow-up)
        self.intents_rejected_short_market: int = 0
        # 2026-05-09 Phase 32 P3 — RiskAllocator rejections counter is
        # now defined in BaseStrategy.__init__

    def set_wallet_avg_pnl(
        self, wallet: str, avg_pct: Decimal
    ) -> None:
        """Phase 23(c) external setter — auditor calls this with the
        wallet's rolling avg PnL per dollar (e.g., last-20 closed
        pairs). Negative values trigger the variance gate when
        `cfg.wallet_avg_pnl_floor_pct` is set."""
        self._wallet_avg_pnl[wallet.lower()] = avg_pct

    async def _subscribe(self) -> None:
        self._bus.subscribe(EVT_WALLET_RANK_CHANGED, self._on_rank)
        self._bus.subscribe(EVT_BOOK_IMBALANCE, self._on_imbalance)
        self._bus.subscribe(EVT_CONTEXT_OK, self._on_ctx_ok)
        self._bus.subscribe(EVT_CONTEXT_BLOCK, self._on_ctx_block)
        self._bus.subscribe(EVT_WALLET_FILL, self._on_wallet_fill)

    # ── State updates ─────────────────────────────────────────────────

    async def _on_rank(self, _e: str, payload: Any) -> None:
        if isinstance(payload, dict) and "followed" in payload:
            self._followed = {w.lower() for w in payload["followed"]}

    async def _on_imbalance(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, ImbalanceSignal):
            return
        # Only cache BID-side (buy pressure) signals.
        if payload.side != "BID":
            self._latest_bid_imbalance.pop(payload.token_id, None)
            return
        self._latest_bid_imbalance[payload.token_id] = _ImbalanceCache(
            side=payload.side, ts=int(payload.ts)
        )

    async def _on_ctx_ok(self, _e: str, payload: Any) -> None:
        if isinstance(payload, dict):
            self._market_blocked.discard(str(payload.get("market_id", "")))

    async def _on_ctx_block(self, _e: str, payload: Any) -> None:
        if isinstance(payload, dict):
            self._market_blocked.add(str(payload.get("market_id", "")))

    # ── Trigger ───────────────────────────────────────────────────────

    async def _on_wallet_fill(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        wallet = str(payload.get("wallet", "")).lower()
        if wallet not in self._followed:
            return
        side = str(payload.get("side", "")).upper()
        token_id = str(payload.get("token_id", ""))
        # 2026-05-08 PHASE 23(a) — track wallet's recent SELL fills
        # so a subsequent BUY on the same token within the lookback
        # window can be flagged as a reversal. Done BEFORE the
        # `side != BUY` early-return so SELL events still update the
        # cache. Requires a token_id; market_id may be empty on
        # SELLs we don't otherwise care about, so we don't gate the
        # cache on it.
        if side == "SELL" and token_id:
            try:
                _ts = int(payload.get("ts", 0))
            except (TypeError, ValueError):
                _ts = 0
            if _ts > 0:
                prev = self._recent_sell_ts.get((wallet, token_id), 0)
                if _ts > prev:
                    self._recent_sell_ts[(wallet, token_id)] = _ts
        if side != "BUY":
            return
        market_id = str(payload.get("market_id", ""))
        if not market_id or not token_id:
            return
        if market_id in self._market_blocked:
            return

        # Orderbook confirmation — recent BID imbalance for this token.
        # Default fail-open since 2026-05-02: the market WS only
        # subscribes to Discovery's tokens (BTC/ETH bars), so wallets
        # trading politics / sports / news produce intents whose
        # imbalance cache will be empty 80%+ of the time. Trust the
        # wallet identity as primary signal; imbalance is just a boost.
        # Set cfg.require_imbalance=True to restore the hard gate.
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

        # Sizing.
        try:
            src_price = Decimal(str(payload.get("price", 0)))
            src_size_shares = Decimal(str(payload.get("size", 0)))
        except (TypeError, ValueError):
            return
        if src_price <= 0 or src_size_shares <= 0:
            return
        # High-price filter: drop intents at or above max_buy_price.
        # PAPER history shows p≥$0.80 has 41.7% win rate and -$0.19
        # avg PnL — capped upside / full downside.
        if (
            self._cfg.max_buy_price is not None
            and src_price >= self._cfg.max_buy_price
        ):
            return

        # 2026-05-08 PHASE 23(c) — wallet-quality variance gate.
        # Wallets with negative rolling avg PnL get demoted. Fail-open
        # for wallets without recorded data (auditor backfills async).
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

        # 2026-05-08 PHASE 23(a) — wallet reversal gate.
        # If the wallet sold this same token within the lookback
        # window, they're reversing — abandon the copy.
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

        # 2026-05-08 PHASE 29(a) — penny-token trap protection.
        # Phase 23(b) only catches high-priced near-resolution entries
        # (≥ time_to_resolution_min_price). v46 trapped at $0.01 on
        # tokens that crashed to $0 within 2 min with no exit
        # liquidity. Block penny-priced entries that don't have
        # enough bar life remaining.
        if (
            self._cfg.penny_max_price is not None
            and src_price < self._cfg.penny_max_price
        ):
            eod = payload.get("end_date_iso")
            if eod:
                try:
                    from datetime import datetime as _dt
                    eod_str = str(eod).replace("Z", "+00:00")
                    eod_ts = int(_dt.fromisoformat(eod_str).timestamp())
                    now_ts = int(time.time())
                    secs_left = eod_ts - now_ts
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
                    pass  # malformed end_date — fail open

        # 2026-05-09 — absolute market-duration floor (Phase 23(b) follow-up).
        # Catches the gap Phase 23(b) misses: cheap entries that bypass
        # the 0.30 price floor, and high-price intents that pass at
        # intent time but settle close to the wire. v52 pos 22496 had
        # bar_to_close=264s at fill (Phase 23(b) would have caught it
        # at the new 600s floor too, but this gate is the structural
        # backstop). Fail open on missing end_date_iso.
        if self._cfg.market_duration_floor_s is not None:
            eod = payload.get("end_date_iso")
            if eod:
                try:
                    from datetime import datetime as _dt
                    eod_str = str(eod).replace("Z", "+00:00")
                    eod_ts = int(_dt.fromisoformat(eod_str).timestamp())
                    now_ts = int(time.time())
                    secs_left = eod_ts - now_ts
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
                    pass  # malformed end_date — fail open

        # 2026-05-08 PHASE 23(b) — bar-resolution time-floor.
        # Reject high-price entries near bar close. Cheap lottery
        # tickets (< time_to_resolution_min_price) are still allowed
        # since they can flip up. Missing/malformed end_date_iso →
        # fail open.
        if (
            self._cfg.min_time_to_resolution_s is not None
            and src_price >= self._cfg.time_to_resolution_min_price
        ):
            eod = payload.get("end_date_iso")
            if eod:
                try:
                    from datetime import datetime as _dt
                    eod_str = str(eod).replace("Z", "+00:00")
                    eod_ts = int(_dt.fromisoformat(eod_str).timestamp())
                    now_ts = int(time.time())
                    secs_left = eod_ts - now_ts
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
                    pass  # malformed end_date — fail open

        # 2026-05-08 PHASE 21 — pre-trade slippage gate.
        # Drop the copy when the current best_ask has moved more than
        # `source_slippage_cap_pct` above source's fill price. Late-
        # copy on a fast market lands the position above the source's
        # entry, immediately negative on unrealized, biased toward SL.
        # Fails open: missing getter, None return, or any exception
        # falls through to the normal flow — wallet identity is the
        # primary edge; orderbook unavailability never blocks the
        # signal. Negative slip (ask BELOW source) is benign and
        # treated as a buying opportunity, not a rejection trigger.
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
        src_size_usd = src_price * src_size_shares
        proposed = src_size_usd * self._cfg.proportion

        # Enforce V2 floors: whichever is binding (5 shares × price OR
        # $1 marketable notional). Pad by `floor_pad_pct` so server-
        # side float math doesn't drift the notional under $1.
        raw_floor = max(
            self._cfg.min_size_usd, self._cfg.min_shares * src_price
        )
        floor_usd = raw_floor * (Decimal("1") + self._cfg.floor_pad_pct)

        # Soft-elasticity ceiling: when the floor exceeds the soft cap,
        # allow the upsize up to `max_position_usd_hard` (defaults to
        # 1.5× the soft cap). Drop only if even the hard ceiling can't
        # absorb the floor — better than letting the order spam
        # live_orders with rejections.
        hard_ceiling = (
            self._cfg.max_position_usd_hard
            if self._cfg.max_position_usd_hard is not None
            else self._cfg.max_position_usd * Decimal("1.5")
        )
        if floor_usd > hard_ceiling:
            return

        size_usd = min(proposed, self._cfg.max_position_usd)
        if size_usd < floor_usd:
            # Upsize to the padded floor. May exceed `max_position_usd`
            # (the soft cap) but stays at or below `max_position_usd_hard`.
            logger.info(
                "copy_trade: upsizing intent to V2 floor "
                "(proposed=$%.4f → floor=$%.4f, price=$%.4f, "
                "min_shares=%s, soft_cap=$%s, hard=$%s)",
                size_usd, floor_usd, src_price, self._cfg.min_shares,
                self._cfg.max_position_usd, hard_ceiling,
            )
            size_usd = floor_usd

        # Shares cap: clamp to max_shares * src_price, but never below V2 floor.
        if self._cfg.max_shares is not None:
            shares_cap_usd = Decimal(str(self._cfg.max_shares)) * src_price
            size_usd = min(size_usd, max(shares_cap_usd, floor_usd))

        # Window-aware ExitConfig: when copying a whale into a short-window
        # crypto market (15m / 5m / 1h bar), inherit the matching scalp_*
        # exit profile (12-50min max_hold) instead of the default copy_trade
        # 24h. Otherwise the bar resolves before the time-stop fires and
        # the position-cap saturates.
        slug = str(payload.get("slug", ""))
        exit_strategy = _exit_profile_for_slug(slug, default=self.name)

        # Apply marketability uplift to limit_price so the BUY crosses
        # the spread instead of resting at the followed wallet's stale
        # fill price. Clamping to Polymarket's binary outcome ceiling
        # is done in live_orders._build_signed (max 0.99) — we don't
        # need to clamp here, but cap at 0.99 anyway so the paper-mode
        # math (which doesn't go through live_orders) stays sane.
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
            exit_config=for_strategy(exit_strategy),
        )

        # 2026-05-09 Phase 32 P3 — RiskAllocator gate (BaseStrategy helper).
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
