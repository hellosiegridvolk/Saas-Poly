"""Market WebSocket — message dispatcher + subscription manager.

Split into pure-logic units so they're individually testable:

  - `build_subscribe_payload` — wire format constant
  - `SubscriptionManager` — token add/remove queue + currently-subscribed set
  - `MarketDispatcher` — raw JSON → typed bus events

The thin connect/run loop is in `MarketWebSocket` (also in this module).
That part is exercised in integration tests; unit tests pin the logic.
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_BEST_BID_ASK,
    EVT_BOOK_SNAPSHOT,
    EVT_MARKET_TICK,
    EVT_MARKET_TICK_SIZE,
)
from poly_terminal.data.clob.orderbook import parse_clob_book

logger = logging.getLogger(__name__)


def build_subscribe_payload(
    token_ids: list[str], *, best_bid_ask: bool = True
) -> dict[str, object]:
    """Subscribe payload for the Polymarket market WS.

    `best_bid_ask=True` (deep-research-23 item #4) sets
    `custom_feature_enabled` so the server emits `best_bid_ask`
    events alongside `book` / `price_change` / `last_trade_price`.
    Best-bid-ask is the strictly richer signal for SELL evaluation —
    we'd close at best_bid, not last_trade_price. Default True; tests
    that pin the legacy payload pass False.
    """
    payload: dict[str, object] = {
        "assets_ids": list(token_ids),
        "type": "Market",
    }
    if best_bid_ask:
        payload["custom_feature_enabled"] = True
    return payload


def build_unsubscribe_payload(token_ids: list[str]) -> dict[str, object]:
    return {"assets_ids": list(token_ids), "type": "Unsubscribe"}


class SubscriptionManager:
    """Manages the subscribed set + add/remove queues.

    Independent of the network so unit tests can exercise it without a
    real socket.
    """

    def __init__(self) -> None:
        self.subscribed: set[str] = set()
        self._pending_sub: set[str] = set()
        self._pending_unsub: set[str] = set()

    def mark_subscribed(self, token_ids: list[str]) -> None:
        """Forcibly mark a set of tokens as already subscribed.

        Used after a clean reconnect when we've re-sent the subscribe.
        """
        self.subscribed.update(token_ids)

    def subscribe(self, token_ids: list[str]) -> None:
        new = set(token_ids) - self.subscribed - self._pending_sub
        self._pending_sub.update(new)

    def unsubscribe(self, token_ids: list[str]) -> None:
        existing = set(token_ids) & self.subscribed
        self._pending_unsub.update(existing)

    def pending_subscribe(self) -> set[str]:
        return set(self._pending_sub)

    def pending_unsubscribe(self) -> set[str]:
        return set(self._pending_unsub)

    def drain_pending_subscribe(self) -> set[str]:
        out = set(self._pending_sub)
        self._pending_sub.clear()
        self.subscribed.update(out)
        return out

    def drain_pending_unsubscribe(self) -> set[str]:
        out = set(self._pending_unsub)
        self._pending_unsub.clear()
        self.subscribed.difference_update(out)
        return out


class MarketDispatcher:
    """Parses raw WS messages and publishes typed events on the bus."""

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self.dropped_count = 0

    async def handle(self, raw: str | bytes) -> None:
        try:
            data: Any = json.loads(raw)
        except (ValueError, TypeError):
            self.dropped_count += 1
            logger.debug("ws.market dropped malformed message")
            return
        # 2026-05-05: Polymarket WS sends events as JSON arrays — even a
        # single price_change comes wrapped as `[{...}]`. Pre-fix, this
        # dispatcher's `isinstance(data, dict)` check silently dropped
        # every batch, so EVT_MARKET_TICK and EVT_BOOK_SNAPSHOT never
        # fired. Symptom: recorder accumulated 0 ticks, bot's exit
        # engine got 0 TP/SL fires, every position rode to bar_end.
        # Empty list (`[]`) is the server ack; treat as no-op.
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    await self._handle_dict(item)
                else:
                    self.dropped_count += 1
            return
        if not isinstance(data, dict):
            self.dropped_count += 1
            return
        await self._handle_dict(data)

    async def _handle_dict(self, data: dict[str, Any]) -> None:
        """Dispatch a single event dict. Extracted from `handle` so the
        list-batch path can reuse it without re-parsing JSON."""
        event_type = data.get("event_type")
        if event_type == "book":
            await self._publish_book(data)
        elif event_type == "price_change":
            await self._publish_tick(data)
        elif event_type == "tick_size_change":
            await self._publish_tick_size(data)
        elif event_type == "last_trade_price":
            await self._publish_last_trade(data)
        elif event_type == "best_bid_ask":
            await self._publish_best_bid_ask(data)
        else:
            self.dropped_count += 1
            logger.debug("ws.market unknown event_type=%r", event_type)

    async def _publish_book(self, data: dict[str, Any]) -> None:
        snap = parse_clob_book(data)
        await self._bus.publish(EVT_BOOK_SNAPSHOT, snap)

    @staticmethod
    def _normalize_ts_to_seconds(raw: Any) -> int:
        """Polymarket sends `timestamp` in milliseconds (13-digit). Bot
        position state stores `entry_ts` in seconds (from
        `int(time.time())`). Decision engine compares `(now_ts - entry_ts)
        >= max_hold_seconds`; with mixed units the diff is ~1.7 trillion
        seconds and the time-stop fires on every tick. Normalize all
        tick `ts` to seconds at this single chokepoint so every
        downstream consumer (decision_engine, profit_taker, strategies,
        recorder) sees consistent units. 2026-05-05 fix.

        Heuristic: ≥10^12 means milliseconds (post-2001 in ms is 13-digit
        territory; in seconds it's 10-digit). Floor to int seconds.
        """
        try:
            v = int(raw or 0)
        except (TypeError, ValueError):
            return 0
        if v >= 1_000_000_000_000:  # 10^12 → must be ms (year 33658+ in s)
            return v // 1000
        return v

    async def _publish_tick(self, data: dict[str, Any]) -> None:
        tick = {
            "token_id": str(data.get("asset_id", "")),
            "price": Decimal(str(data.get("price", "0"))),
            "size": Decimal(str(data.get("size", "0"))),
            "side": str(data.get("side", "")).upper() or None,
            "ts": self._normalize_ts_to_seconds(data.get("timestamp", 0)),
        }
        await self._bus.publish(EVT_MARKET_TICK, tick)

    async def _publish_tick_size(self, data: dict[str, Any]) -> None:
        await self._bus.publish(
            EVT_MARKET_TICK_SIZE,
            {
                "token_id": str(data.get("asset_id", "")),
                "tick_size": Decimal(str(data.get("tick_size", "0"))),
                "ts": self._normalize_ts_to_seconds(data.get("timestamp", 0)),
            },
        )

    async def _publish_last_trade(self, data: dict[str, Any]) -> None:
        # Re-use the tick channel for last-trade-price prints.
        await self._bus.publish(
            EVT_MARKET_TICK,
            {
                "token_id": str(data.get("asset_id", "")),
                "price": Decimal(str(data.get("price", "0"))),
                "size": Decimal(str(data.get("size", "0"))),
                "side": "TRADE",
                "ts": self._normalize_ts_to_seconds(data.get("timestamp", 0)),
            },
        )

    async def _publish_best_bid_ask(self, data: dict[str, Any]) -> None:
        """Polymarket emits `best_bid_ask` when subscribed with
        `custom_feature_enabled: true`. We publish two events:

        1. EVT_BEST_BID_ASK with both bid + ask + size — for any
           consumer that needs the spread (orderbook intel, future
           liquidity gates, etc).
        2. EVT_MARKET_TICK with `price=best_bid` and `side='BBA'` so
           the existing exit-decision pipeline picks it up like any
           other tick. best_bid is the realistic SELL-side exit price
           — strictly more accurate than last_trade_price (which can
           be stale by minutes on illiquid markets).
        """
        token_id = str(data.get("asset_id", ""))
        ts = self._normalize_ts_to_seconds(data.get("timestamp", 0))
        try:
            best_bid = Decimal(str(data.get("best_bid", "0")))
            best_ask = Decimal(str(data.get("best_ask", "0")))
            best_bid_size = Decimal(str(data.get("best_bid_size", "0")))
            best_ask_size = Decimal(str(data.get("best_ask_size", "0")))
        except (TypeError, ValueError, InvalidOperation):
            # InvalidOperation fires when the payload contains a
            # non-numeric string ("not_a_number") — Decimal raises
            # that, not ValueError. Treat all three as malformed.
            self.dropped_count += 1
            logger.debug(
                "ws.market malformed best_bid_ask payload: %s", data
            )
            return
        await self._bus.publish(
            EVT_BEST_BID_ASK,
            {
                "token_id": token_id,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "best_bid_size": best_bid_size,
                "best_ask_size": best_ask_size,
                "ts": ts,
            },
        )
        # Forward best_bid as a market tick so the existing exit
        # decision engine evaluates against the realistic exit price.
        # If best_bid is 0 (no buyers — we'd close at $0), still emit
        # the tick so the recorder/observability layers see it; the
        # decision engine's TP path won't trigger on price=0 and the
        # SL path requires three concurrent conditions, so a stale 0
        # bid is benign for HOLD decisions.
        await self._bus.publish(
            EVT_MARKET_TICK,
            {
                "token_id": token_id,
                "price": best_bid,
                "size": best_bid_size,
                "side": "BBA",
                "ts": ts,
            },
        )


class MarketWebSocket:
    """Connects to the Polymarket public market WebSocket.

    Owns the connect-loop, ping-loop, subscription-flush task, and reconnect
    backoff. Public surface is `run(shutdown)` plus subscribe/unsubscribe.

    Connect strategy is overridable for tests via `connect_factory`; the
    default uses the production `websockets.connect`.
    """

    DEFAULT_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(
        self,
        bus: EventBus,
        url: str = DEFAULT_URL,
        ping_interval_s: int = 10,
        max_queue: int = 1000,
        connect_factory: Any = None,
    ) -> None:
        self.bus = bus
        self.url = url
        self.ping_interval_s = ping_interval_s
        self.max_queue = max_queue
        self.subs = SubscriptionManager()
        self.dispatcher = MarketDispatcher(bus)
        self._shutdown = asyncio.Event()
        self._connect = connect_factory  # async callable returning a ws conn

    def subscribe_tokens(self, token_ids: list[str]) -> None:
        self.subs.subscribe(token_ids)

    def unsubscribe_tokens(self, token_ids: list[str]) -> None:
        self.subs.unsubscribe(token_ids)

    @property
    def subscribed_count(self) -> int:
        return len(self.subs.subscribed)

    def stop(self) -> None:
        self._shutdown.set()

    async def run(self, shutdown: asyncio.Event | None = None) -> None:
        """Maintain the connection until `shutdown` (or self._shutdown) is set."""
        from poly_terminal.data.websocket.reconnector import Backoff

        stop = shutdown or self._shutdown
        backoff = Backoff(initial_s=1.0, max_s=60.0, factor=2.0, jitter=True)
        while not stop.is_set():
            try:
                await self._connect_and_stream(stop)
                backoff.reset()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                delay = backoff.next_delay()
                logger.warning(
                    "ws.market disconnected (%s) — reconnecting in %.1fs", exc, delay
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                    return
                except asyncio.TimeoutError:
                    continue
        logger.info("ws.market stopped")

    async def _connect_and_stream(self, stop: asyncio.Event) -> None:
        connect = self._connect or self._default_connect
        cm = connect(self.url)
        async with cm as ws:
            logger.info("ws.market connected to %s", self.url)
            # Re-subscribe all currently-subscribed + drain pending.
            existing = list(self.subs.subscribed)
            if existing:
                await ws.send(json.dumps(build_subscribe_payload(existing)))
            new_subs = self.subs.drain_pending_subscribe()
            if new_subs:
                await ws.send(json.dumps(build_subscribe_payload(list(new_subs))))
            await asyncio.gather(
                self._recv_loop(ws, stop),
                self._ping_loop(ws, stop),
                self._sub_flush_loop(ws, stop),
            )

    @staticmethod
    def _default_connect(url: str) -> Any:
        import websockets

        return websockets.connect(url, ping_interval=None, ping_timeout=None,
                                   close_timeout=5, max_size=2**22)

    async def _recv_loop(self, ws: Any, stop: asyncio.Event) -> None:
        async for raw in ws:
            if stop.is_set():
                break
            try:
                await self.dispatcher.handle(raw)
            except Exception:
                logger.exception("ws.market dispatcher error")

    async def _ping_loop(self, ws: Any, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await ws.ping()
            except Exception:
                return
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.ping_interval_s)
                return
            except asyncio.TimeoutError:
                continue

    async def _sub_flush_loop(self, ws: Any, stop: asyncio.Event) -> None:
        while not stop.is_set():
            new_subs = self.subs.drain_pending_subscribe()
            if new_subs:
                await ws.send(json.dumps(build_subscribe_payload(list(new_subs))))
            unsubs = self.subs.drain_pending_unsubscribe()
            if unsubs:
                await ws.send(json.dumps(build_unsubscribe_payload(list(unsubs))))
            try:
                await asyncio.wait_for(stop.wait(), timeout=2.0)
                return
            except asyncio.TimeoutError:
                continue
