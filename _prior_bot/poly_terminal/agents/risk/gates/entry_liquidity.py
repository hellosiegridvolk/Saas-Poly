"""Pre-submit liquidity gate.

Fetches best bid + best ask from the live orderbook before every BUY.
Rejects when:
  - empty book (no bids OR no asks)
  - spread too wide (best_ask - best_bid > max_spread)
  - estimated entry slippage too high (best_ask - intent.limit_price > intent.limit_price * max_slippage_pct)

All Reject codes are structured so monitor/AutoTuner can attribute
rejections to a specific cause (not the generic FAK 'no_match').

Async because it does a network round-trip per intent (~50-200ms via
SDK get_price). Wired BEFORE per_trade_size in the pipeline so cheap
sanity checks (mode, blacklist, dedupe) still short-circuit first.

SELL intents pass through unchanged — only entry-side BUYs need the
fillability check.

2026-05-05 ship: per deep-research-report (14) §entry-gate.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Awaitable, Callable

from poly_terminal.agents.risk.intent import BuyIntent
from poly_terminal.shared.enums import IntentSide
from poly_terminal.shared.typed_reject import Reject

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntryLiquidityConfig:
    max_spread: Decimal = Decimal("0.05")
    max_entry_slippage_pct: Decimal = Decimal("0.03")
    assumed_exit_discount_pct: Decimal = Decimal("0.04")

    @classmethod
    def from_env(cls) -> "EntryLiquidityConfig":
        return cls(
            max_spread=Decimal(
                os.environ.get("ENTRY_LIQUIDITY_MAX_SPREAD", "0.05")
            ),
            max_entry_slippage_pct=Decimal(
                os.environ.get("ENTRY_LIQUIDITY_MAX_SLIPPAGE_PCT", "0.03")
            ),
            assumed_exit_discount_pct=Decimal(
                os.environ.get("ENTRY_LIQUIDITY_EXIT_DISCOUNT_PCT", "0.04")
            ),
        )


class EntryLiquidityGate:
    def __init__(
        self,
        cfg: EntryLiquidityConfig,
        best_ask_fn: Callable[[str], Awaitable[float | None]],
        best_bid_fn: Callable[[str], Awaitable[float | None]],
    ) -> None:
        self._cfg = cfg
        self._ask = best_ask_fn
        self._bid = best_bid_fn

    async def __call__(self, intent: BuyIntent) -> Reject | None:
        # SELLs don't entry-fill on the ask side. Pass through.
        if intent.side != IntentSide.BUY:
            return None
        if not intent.token_id:
            return None  # let other gates handle malformed intents
        try:
            best_ask = await self._ask(intent.token_id)
            best_bid = await self._bid(intent.token_id)
        except Exception:
            logger.warning(
                "entry_liquidity: orderbook fetch raised for token %s",
                intent.token_id, exc_info=True,
            )
            return Reject(
                code="entry_liquidity_fetch_failed",
                detail=f"orderbook fetch raised for token={intent.token_id}",
            )
        if best_ask is None or best_bid is None:
            return Reject(
                code="entry_liquidity_fetch_failed",
                detail=(
                    f"orderbook fetch returned None "
                    f"(ask={best_ask}, bid={best_bid})"
                ),
            )
        if best_ask <= 0 or best_bid <= 0:
            return Reject(
                code="entry_liquidity_empty_book",
                detail=f"non-positive prices ask={best_ask} bid={best_bid}",
            )
        ask = Decimal(str(best_ask))
        bid = Decimal(str(best_bid))
        spread = ask - bid
        if spread > self._cfg.max_spread:
            logger.info(
                "entry_liquidity: REJECT spread_too_wide intent=%s token=%s "
                "bid=%s ask=%s spread=%s max=%s",
                intent.intent_id, intent.token_id, bid, ask, spread,
                self._cfg.max_spread,
            )
            return Reject(
                code="entry_liquidity_spread_too_wide",
                detail=f"bid={bid} ask={ask} spread={spread} > {self._cfg.max_spread}",
            )
        # Slippage = best_ask - intent.limit_price (when ask > limit, market
        # has moved against us). Compare against intent.limit * max_pct.
        slippage = ask - intent.limit_price
        max_slip = intent.limit_price * self._cfg.max_entry_slippage_pct
        if slippage > max_slip:
            logger.info(
                "entry_liquidity: REJECT slippage_too_high intent=%s "
                "limit=%s ask=%s slippage=%s max=%s",
                intent.intent_id, intent.limit_price, ask, slippage, max_slip,
            )
            return Reject(
                code="entry_liquidity_slippage_too_high",
                detail=(
                    f"limit={intent.limit_price} ask={ask} "
                    f"slippage={slippage} > {max_slip}"
                ),
            )
        return None
