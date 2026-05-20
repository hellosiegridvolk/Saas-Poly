"""PriceSurgeDetector — Option B latency fix (2026-05-20).

When a whale BUYs a large position the ask price drops immediately on
the CLOB WebSocket (the order takes liquidity off the book). This
detector watches `best_bid_ask` events for a pre-subscribed set of
"tracked tokens" and emits EVT_WALLET_FILL with wallet="price_impact"
when the ask price drops >= `surge_threshold_pct` from its recent
baseline in a single tick.

Lag: 0–500ms (CLOB WS delivery) vs 2–5s (on-chain) or 30s (Data API).

Trade-off vs Option A:
  + Faster (no Polygon block time, no Gamma lookup)
  - No wallet attribution (wallet="price_impact")
  - More false positives (any large order, not just followed wallets)
  - Requires tracked tokens to already be subscribed in market_ws

Integration:
  • PolygonLogSubscriber (Option A) calls `watch_token(token_id)` when
    it fires a fill, so the detector picks up that token for future
    cycles on the same market.
  • At startup, main.py pre-subscribes all token_ids from wallet_history
    for the followed wallets (so any market they've traded before is
    watched from the first tick).
  • copy_scalp_active subscribes to EVT_WALLET_FILL and accepts
    wallet="price_impact" in its followed set (add the sentinel to
    WALLET_COPY_SCALP_ACTIVE_OVERRIDE is NOT needed — the detector
    passes market_id and token_id directly, so the wallet filter in
    copy_scalp is bypassed via the strategy that subscribes to
    EVT_PRICE_SURGE instead).

EVT_PRICE_SURGE payload (subset of EVT_WALLET_FILL format):
  token_id, market_id, price (current ask), price_drop_pct, ts
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from decimal import Decimal
from typing import Any

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_BEST_BID_ASK, EVT_PRICE_SURGE

logger = logging.getLogger(__name__)

# How far back to look when computing the "baseline" ask price.
_BASELINE_WINDOW_S = 30

# Minimum ask price to act on (ignore sub-cent noise).
_MIN_ASK = Decimal("0.02")


class PriceSurgeDetector:
    """Watches EVT_BEST_BID_ASK for tracked tokens.

    When best_ask drops >= threshold_pct% in a single tick (relative to
    the recent baseline), emits EVT_PRICE_SURGE with token/market context.

    Call `watch_token(token_id, market_id)` to add a token to the watch
    set. The market_ws subscription is the caller's responsibility.
    """

    def __init__(
        self,
        bus: EventBus,
        *,
        surge_threshold_pct: float = 3.0,
        cooldown_s: float = 10.0,
    ) -> None:
        self._bus = bus
        self._threshold = surge_threshold_pct / 100.0
        self._cooldown_s = cooldown_s
        # token_id → market_id
        self._watched: dict[str, str] = {}
        # token_id → deque of (ts, ask_price) for baseline
        self._history: dict[str, deque[tuple[float, Decimal]]] = {}
        # token_id → last surge emit ts (for cooldown)
        self._last_surge: dict[str, float] = {}
        self._stats = {"surges_emitted": 0, "tokens_watched": 0}

    def watch_token(self, token_id: str, market_id: str) -> None:
        if token_id not in self._watched:
            self._watched[token_id] = market_id
            self._history[token_id] = deque()
            self._stats["tokens_watched"] += 1
            logger.debug(
                "price_surge_detector: watching token %s… market=%s…",
                token_id[:12], market_id[:12],
            )

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    async def start(self) -> None:
        self._bus.subscribe(EVT_BEST_BID_ASK, self._on_best_bid_ask)

    async def _on_best_bid_ask(self, _e: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        token_id = str(payload.get("token_id", ""))
        if token_id not in self._watched:
            return

        try:
            best_ask = Decimal(str(payload.get("best_ask", "0")))
        except Exception:
            return
        if best_ask < _MIN_ASK:
            return

        now = float(payload.get("ts", time.time()))
        hist = self._history[token_id]

        # Prune old entries outside the baseline window.
        cutoff = now - _BASELINE_WINDOW_S
        while hist and hist[0][0] < cutoff:
            hist.popleft()

        if hist:
            # Baseline = median of recent asks (robust to single outliers).
            asks = sorted(h[1] for h in hist)
            baseline = asks[len(asks) // 2]
            if baseline > _MIN_ASK:
                drop_pct = float((baseline - best_ask) / baseline)
                if drop_pct >= self._threshold:
                    # Cooldown gate — don't spam on sustained low ask.
                    last = self._last_surge.get(token_id, 0.0)
                    if now - last >= self._cooldown_s:
                        self._last_surge[token_id] = now
                        market_id = self._watched[token_id]
                        await self._emit_surge(
                            token_id, market_id, best_ask, drop_pct, now
                        )

        hist.append((now, best_ask))

    async def _emit_surge(
        self,
        token_id: str,
        market_id: str,
        ask: Decimal,
        drop_pct: float,
        ts: float,
    ) -> None:
        self._stats["surges_emitted"] += 1
        logger.info(
            "price_surge_detector: ask drop %.1f%% on token %s… "
            "(ask=%.4f) → EVT_PRICE_SURGE",
            drop_pct * 100, token_id[:12], float(ask),
        )
        await self._bus.publish(
            EVT_PRICE_SURGE,
            {
                "token_id": token_id,
                "market_id": market_id,
                "price": float(ask),
                "price_drop_pct": drop_pct,
                "ts": int(ts),
                "_source": "price_surge",
            },
        )


__all__ = ["PriceSurgeDetector"]
