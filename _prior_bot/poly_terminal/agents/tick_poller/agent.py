"""REST-based TickPoller — synthesizes EVT_MARKET_TICK when WS feed is silent.

2026-05-05 — Polymarket's `/ws/market` endpoint stopped delivering
`price_change` / `last_trade_price` events around 06:00 today. The
bot's ExitAgent depends on EVT_MARKET_TICK to evaluate TP/SL/time-stop
inside `decision_engine.evaluate`; without ticks every position rides
to bar resolution and TP/SL never fires. Histogram of WS event activity
shows ~5,500/hr → 0/hr in 6 hours.

This agent fills the gap by REST-polling Polymarket's `/price` endpoint
for each open-position token and synthesizing EVT_MARKET_TICK events
with side="POLL" (so downstream consumers can distinguish poll-derived
ticks from real WS ticks if needed).

Design choices:
  - Poll `/price?side=BUY` because all bot positions are BUYs and TP/SL
    is evaluated against the price we'd receive on close — the best_bid.
  - Dedupe on unchanged price: skip publishing when the polled price
    equals the last published price for that token. Avoids inflating
    `adverse_tick_count` from a stuck book.
  - Per-call timeout: bound the SDK call so a stuck HTTP doesn't wedge
    the loop (mirrors the shadow_price_fn timeout patch from earlier).
  - Concurrent within-cycle, sequential between cycles: poll all tokens
    in parallel (capped by semaphore) so cycle latency stays bounded
    even as open-position count grows.
  - Off by default — gated on `TICK_POLLER_ENABLED=true` env var so the
    legacy WS-only deployment keeps its existing behavior.

Stats counters (exposed for monitor):
  - cycles, polls_attempted, polls_404, polls_failed, polls_ok
  - ticks_published, ticks_deduped (price unchanged from last)
  - tokens_at_last_cycle (current open-position count)
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Awaitable, Callable

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_MARKET_TICK

logger = logging.getLogger(__name__)


# Function signatures the poller needs from its collaborators.
# Keep them as simple callables so the agent can be unit-tested with
# fakes without pulling in the full bot stack.

PriceFetcher = Callable[[str], Any]
"""Sync callable: token_id -> float | None (best_bid). Wrapped in
asyncio.to_thread by the agent. Mirrors the bot's
`live_client.get_best_bid` shape."""

OpenTokensFetcher = Callable[[], "Awaitable[list[str]]"]
"""Async callable: -> list[str] of open-position token_ids."""


class TickPoller:
    """REST-driven EVT_MARKET_TICK synthesizer.

    Owns one poll loop. Construct with collaborators + interval, call
    `await poller.run(shutdown)` from the bot's task list.
    """

    def __init__(
        self,
        bus: EventBus,
        get_best_bid: PriceFetcher,
        get_open_tokens: OpenTokensFetcher,
        *,
        get_best_ask: PriceFetcher | None = None,
        get_last_trade_price: PriceFetcher | None = None,
        poll_interval_s: float = 5.0,
        per_call_timeout_s: float = 3.0,
        max_concurrent_polls: int = 5,
    ) -> None:
        self._bus = bus
        self._get_best_bid = get_best_bid
        # Price-source fallback chain (2026-05-05):
        #   1. last_trade_price — canonical "fair" price used by
        #      Polymarket's WS `last_trade_price` event. Best because
        #      our own fill produces a last_trade event matching the
        #      fill price → no spurious SL fire on first tick.
        #   2. midpoint (bid + ask)/2 — when no recent trade exists
        #      (e.g. brand-new bar). Avoids the -spread% bias of
        #      best_bid alone.
        #   3. best_bid — legacy single-side behavior. Kept for tests
        #      and for the "no fancy fetchers wired" deployment path.
        self._get_best_ask = get_best_ask
        self._get_last_trade_price = get_last_trade_price
        self._get_open_tokens = get_open_tokens
        # Floor at 0.05s — prevents an accidental `0` from spinning the
        # event loop, but loose enough that tests can run at 20-50ms
        # cadence without artificially slowing the suite. Production
        # callers should pass ≥1s anyway (env default is 5s).
        self._poll_interval_s = max(0.05, float(poll_interval_s))
        self._per_call_timeout_s = max(0.05, float(per_call_timeout_s))
        self._sem = asyncio.Semaphore(max(1, int(max_concurrent_polls)))
        # token_id → last published price (str of Decimal). Used to
        # dedupe repeat polls. Reset is allowed at any time — the
        # next changed-price poll will publish again.
        self._last_published: dict[str, str] = {}
        self.stats: dict[str, int] = {
            "cycles": 0,
            "polls_attempted": 0,
            "polls_ok": 0,
            "polls_404": 0,
            "polls_failed": 0,
            "polls_timeout": 0,
            "ticks_published": 0,
            "ticks_deduped": 0,
            "tokens_at_last_cycle": 0,
        }

    async def _poll_one(self, token_id: str, ts_s: int) -> None:
        """Single token poll → maybe-publish path. Captures all errors
        locally so a single bad token can't kill the cycle.

        Price source preference (set in __init__ docstring):
          1. last_trade_price (if wired)
          2. midpoint of bid+ask (if both wired)
          3. best_bid (legacy fallback)
        """
        self.stats["polls_attempted"] += 1
        price: float | None = None
        try:
            # Tier 1: last_trade_price
            if self._get_last_trade_price is not None:
                price = await asyncio.wait_for(
                    asyncio.to_thread(self._get_last_trade_price, token_id),
                    timeout=self._per_call_timeout_s,
                )
            # Tier 2: midpoint (used when no last trade OR tier 1 not wired)
            if price is None and self._get_best_ask is not None:
                bid_task = asyncio.wait_for(
                    asyncio.to_thread(self._get_best_bid, token_id),
                    timeout=self._per_call_timeout_s,
                )
                ask_task = asyncio.wait_for(
                    asyncio.to_thread(self._get_best_ask, token_id),
                    timeout=self._per_call_timeout_s,
                )
                bid, ask = await asyncio.gather(bid_task, ask_task)
                if bid is not None and ask is not None:
                    price = (float(bid) + float(ask)) / 2.0
            # Tier 3: best_bid alone (only if no other source wired)
            if (
                price is None
                and self._get_best_ask is None
                and self._get_last_trade_price is None
            ):
                price = await asyncio.wait_for(
                    asyncio.to_thread(self._get_best_bid, token_id),
                    timeout=self._per_call_timeout_s,
                )
        except asyncio.TimeoutError:
            self.stats["polls_timeout"] += 1
            return
        except Exception:
            # /price 404 (resolved market) bubbles up as PolyApiException
            # from the bot's get_best_bid wrapper which catches it and
            # returns None. Anything reaching here is a code-side bug.
            logger.exception(
                "tick_poller: poll raised for token %s", token_id,
            )
            self.stats["polls_failed"] += 1
            return
        if price is None:
            # Either 404 (resolved) or empty book — no tick to publish.
            self.stats["polls_404"] += 1
            return
        self.stats["polls_ok"] += 1
        # Dedupe: skip publish when price equals the last we sent.
        # str(Decimal) is the canonical key — float comparison is
        # untrustworthy at tick-level (0.49000000001 != 0.49).
        try:
            price_dec = Decimal(str(price))
        except Exception:
            self.stats["polls_failed"] += 1
            return
        price_str = str(price_dec)
        if self._last_published.get(token_id) == price_str:
            self.stats["ticks_deduped"] += 1
            return
        self._last_published[token_id] = price_str
        await self._bus.publish(
            EVT_MARKET_TICK,
            {
                "token_id": token_id,
                "price": price_dec,
                "size": Decimal("0"),  # poll doesn't report a trade size
                "side": "POLL",         # distinguishable from BUY/SELL/TRADE
                "ts": ts_s,
            },
        )
        self.stats["ticks_published"] += 1

    async def _poll_cycle(self) -> None:
        """Run one cycle: fetch open tokens, poll each in parallel."""
        try:
            tokens = await self._get_open_tokens()
        except Exception:
            logger.exception("tick_poller: get_open_tokens failed")
            return
        self.stats["tokens_at_last_cycle"] = len(tokens)
        if not tokens:
            return

        import time
        # 2026-05-05: ts MUST be seconds (not ms). PositionState.entry_ts
        # is set from `int(time.time())` in the execution agent, so the
        # decision_engine's `(now_ts - entry_ts) >= max_hold_seconds`
        # check requires the same unit. Mismatch → time-stop fires
        # immediately on every poll (we observed avg hold = 0.1 min
        # before this fix).
        ts_s = int(time.time())

        async def _bounded(t: str) -> None:
            async with self._sem:
                await self._poll_one(t, ts_s)

        await asyncio.gather(*(_bounded(t) for t in tokens), return_exceptions=True)

    async def run(self, shutdown: asyncio.Event) -> None:
        """Driver loop — call once per process. Returns when shutdown
        is set."""
        logger.info(
            "tick_poller: starting (interval=%.1fs timeout=%.1fs concurrent=%d)",
            self._poll_interval_s, self._per_call_timeout_s,
            self._sem._value,  # type: ignore[attr-defined]
        )
        while not shutdown.is_set():
            try:
                await self._poll_cycle()
            except Exception:
                logger.exception("tick_poller: cycle failed")
            self.stats["cycles"] += 1
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=self._poll_interval_s,
                )
                return  # shutdown set
            except asyncio.TimeoutError:
                continue
        logger.info("tick_poller: stopped (cycles=%d)", self.stats["cycles"])

    def reset_dedupe_for_token(self, token_id: str) -> None:
        """Clear the last-published price for a token. Useful after
        position close to free memory; not strictly required."""
        self._last_published.pop(token_id, None)
