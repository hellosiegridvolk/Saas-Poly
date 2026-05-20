"""Standalone CLOB orderbook snapshot + tick recorder.

Reuses the existing `MarketWebSocket` machinery (connect, reconnect,
subscribe queueing, dispatch) from `data/websocket/market.py`. The
recorder attaches NEW listeners on:

  - `EVT_BOOK_SNAPSHOT` → full L2 ladder (event_type='book' on the
    Polymarket WS, sent at subscribe and on resync).
  - `EVT_MARKET_TICK`   → individual price-level deltas
    (event_type='price_change' + 'last_trade_price' on the WS).
    Polymarket sends a `book` snapshot ONCE per token at subscribe,
    then streams every state change as a price_change. Without
    capturing these, the dataset has only the initial book per token —
    useless for fill simulation against intraday signals.

Each listener:

  1. Buffers its payload as a dict,
  2. Auto-flushes when the buffer fills (size threshold), AND
  3. Periodically flushes on a timer (interval threshold).

Thresholds guarantee we never sit on data forever during low-volume
periods AND we don't queue unbounded RAM on high-volume bursts.

This module is offline-only research infrastructure; it never calls
into the live trading bot's bus or DB writers.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Awaitable, Callable

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_BOOK_SNAPSHOT, EVT_MARKET_TICK
from poly_terminal.data.clob.orderbook import BookLevel, BookSnapshot
from poly_terminal.data.websocket.market import MarketWebSocket
from poly_terminal.research.clob_recorder.snapshot_repo import SnapshotRepo
from poly_terminal.research.clob_recorder.tick_repo import TickRepo

logger = logging.getLogger(__name__)


def _level_to_dict(level: Any) -> dict[str, float] | None:
    """Normalize a book-level entry to a {price, size} dict.

    Handles `BookLevel` dataclasses, (price, size) tuples, and dicts.
    Returns None when the input isn't shaped like a level so the caller
    can drop it cleanly.
    """
    if isinstance(level, BookLevel):
        return {"price": float(level.price), "size": float(level.size)}
    if isinstance(level, dict):
        try:
            return {"price": float(level["price"]), "size": float(level["size"])}
        except (KeyError, TypeError, ValueError):
            return None
    if isinstance(level, (tuple, list)) and len(level) >= 2:
        try:
            return {"price": float(level[0]), "size": float(level[1])}
        except (TypeError, ValueError):
            return None
    return None


def _normalize_levels(levels: Any) -> list[dict[str, float]]:
    if levels is None:
        return []
    out: list[dict[str, float]] = []
    try:
        iterator = iter(levels)
    except TypeError:
        return out
    for level in iterator:
        norm = _level_to_dict(level)
        if norm is not None:
            out.append(norm)
    return out


def _decimal_to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def tick_to_row(payload: Any) -> dict[str, Any] | None:
    """Convert an EVT_MARKET_TICK payload (dict from MarketDispatcher) to a
    repo row. Returns None for malformed input.

    The MarketDispatcher publishes ticks shaped like::

        {"token_id": "<asset_id>", "price": Decimal, "size": Decimal,
         "side": "BUY" | "SELL" | "TRADE" | None, "ts": int}

    Both `price_change` events (delta on bid/ask side) and
    `last_trade_price` prints (synthetic side="TRADE") flow through the
    same channel — we record both shapes uniformly.
    """
    if not isinstance(payload, dict):
        return None
    token_id = payload.get("token_id") or payload.get("asset_id")
    if not token_id:
        return None
    try:
        ts = int(payload.get("ts") or payload.get("timestamp") or 0)
    except (TypeError, ValueError):
        ts = 0
    side = payload.get("side")
    if side is not None:
        side = str(side)
    return {
        "token_id": str(token_id),
        "ts": ts,
        "price": _decimal_to_float(payload.get("price")),
        "size": _decimal_to_float(payload.get("size")),
        "side": side,
        "source": "clob_ws",
    }


def snapshot_to_row(snap: Any) -> dict[str, Any] | None:
    """Convert a BookSnapshot (or dict-shaped equivalent) to a repo row.

    Returns None if the snapshot is missing a token_id — defensive against
    upstream parser corner cases.
    """
    if isinstance(snap, BookSnapshot):
        token_id = snap.token_id
        bids = _normalize_levels(snap.bids)
        asks = _normalize_levels(snap.asks)
        ts = int(snap.ts)
        best_bid = _decimal_to_float(snap.best_bid())
        best_ask = _decimal_to_float(snap.best_ask())
    elif isinstance(snap, dict):
        token_id = snap.get("token_id") or snap.get("asset_id")
        bids = _normalize_levels(snap.get("bids"))
        asks = _normalize_levels(snap.get("asks"))
        try:
            ts = int(snap.get("ts") or snap.get("timestamp") or 0)
        except (TypeError, ValueError):
            ts = 0
        best_bid = _decimal_to_float(
            snap.get("best_bid") or (bids[0]["price"] if bids else None)
        )
        best_ask = _decimal_to_float(
            snap.get("best_ask") or (asks[0]["price"] if asks else None)
        )
    else:
        return None

    if not token_id:
        return None

    return {
        "token_id": str(token_id),
        "ts": ts,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bids": bids,
        "asks": asks,
        "source": "clob_ws",
    }


class ClobRecorder:
    """Buffer + persist orderbook snapshots from the live CLOB WebSocket.

    Construct with an open `SnapshotRepo`, the WS URL, and a list of token
    ids to subscribe to. Call `await recorder.run(shutdown)` to start; it
    returns when the shutdown event is set, after one final flush.
    """

    def __init__(
        self,
        snapshot_repo: SnapshotRepo,
        market_ws_url: str,
        token_ids: list[str],
        buffer_size: int = 100,
        flush_interval_s: float = 5.0,
        token_refresh_fn: "Callable[[], Awaitable[set[str]]] | None" = None,
        token_refresh_interval_s: float = 300.0,
        tick_repo: TickRepo | None = None,
        tick_buffer_size: int | None = None,
    ) -> None:
        self._repo = snapshot_repo
        self._url = market_ws_url
        self._token_ids = list(token_ids)
        self._buffer_size = max(1, int(buffer_size))
        self._flush_interval_s = max(0.1, float(flush_interval_s))
        # 2026-05-05 token-list refresh: optional async callable that
        # returns the CURRENT desired token set. Recorder periodically
        # diffs it against the active subscriptions and adds/removes.
        # When None, the recorder keeps its initial static list (legacy
        # behavior). When set, useful for crypto bars whose markets
        # spawn/close every 5-60 minutes — without refresh, the static
        # list ages into mostly-dead tokens within a few hours.
        self._token_refresh_fn = token_refresh_fn
        self._token_refresh_interval_s = max(30.0, float(token_refresh_interval_s))
        # 2026-05-05 tick capture: optional companion repo for
        # research_orderbook_ticks (price_change deltas + last_trade
        # prints). When None, EVT_MARKET_TICK events are dropped —
        # legacy snapshot-only behavior.
        self._tick_repo = tick_repo
        # Ticks fire ~10× more often than book snapshots in active
        # markets; allow a separate (typically larger) buffer threshold.
        self._tick_buffer_size = (
            max(1, int(tick_buffer_size))
            if tick_buffer_size is not None
            else max(1, int(buffer_size) * 5)
        )

        self._pending_snapshots: list[dict[str, Any]] = []
        self._pending_ticks: list[dict[str, Any]] = []
        self._buffer_lock = asyncio.Lock()
        self._tick_buffer_lock = asyncio.Lock()
        self._started = False
        self._shutdown = asyncio.Event()
        self._ws: MarketWebSocket | None = None

        self.stats: dict[str, int] = {
            "snapshots_received": 0,
            "snapshots_persisted": 0,
            "buffer_high_water": 0,
            "errors": 0,
            "token_refreshes": 0,
            "tokens_added": 0,
            "tokens_dropped": 0,
            "ticks_received": 0,
            "ticks_persisted": 0,
            "tick_buffer_high_water": 0,
            "ticks_dropped_no_repo": 0,
        }

    @property
    def buffer_depth(self) -> int:
        return len(self._pending_snapshots)

    @property
    def tick_buffer_depth(self) -> int:
        return len(self._pending_ticks)

    async def run(self, shutdown: asyncio.Event | None = None) -> None:
        """Run the recorder until `shutdown` is set.

        Subscribes to all configured tokens, runs the WS recv loop and the
        timer-driven flush loop concurrently, and on exit performs one
        final flush so no in-flight snapshots are lost.
        """
        stop = shutdown or self._shutdown
        if not self._token_ids:
            logger.warning(
                "clob_recorder: no token_ids configured — recorder is a no-op; "
                "set RECORDER_TOKENS or RECORDER_TOKEN_FILE"
            )
            await stop.wait()
            return

        bus = EventBus()  # local bus, isolated from the live bot's bus
        # 2026-05-05 stall fix: the bot's default connect_factory disables
        # websockets-library protocol pings (ping_interval=None) — fine for
        # the bot because it has constant book/tick traffic on its 5-10
        # active tokens. The recorder subscribes to ~220 tokens, most of
        # which are dormant crypto bars. With no traffic AND no protocol
        # ping, Polymarket eventually goes silent and our recv_loop blocks
        # forever on `async for raw in ws` (no exception, no reconnect).
        # Enable protocol-level keepalive so dead connections fail visibly
        # within ping_timeout, triggering reconnect via the existing backoff.
        def _keepalive_connect(url: str) -> Any:
            import websockets
            return websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=2**22,
            )

        ws = MarketWebSocket(
            bus=bus, url=self._url, connect_factory=_keepalive_connect,
        )
        ws.subscribe_tokens(self._token_ids)
        bus.subscribe(EVT_BOOK_SNAPSHOT, self._on_snapshot)
        # Tick subscription only when a tick_repo is wired — keeps the
        # legacy snapshot-only deployment path unchanged.
        if self._tick_repo is not None:
            bus.subscribe(EVT_MARKET_TICK, self._on_tick)
        self._ws = ws
        self._started = True

        logger.info(
            "clob_recorder starting: url=%s tokens=%d buffer=%d "
            "tick_buffer=%d tick_repo=%s interval=%.1fs",
            self._url,
            len(self._token_ids),
            self._buffer_size,
            self._tick_buffer_size,
            "on" if self._tick_repo is not None else "off",
            self._flush_interval_s,
        )

        # Always run the WS + flush loops. Optionally also run the
        # token-refresh loop if a refresh callable was provided.
        coros = [ws.run(stop), self._flush_loop(stop)]
        if self._token_refresh_fn is not None:
            coros.append(self._token_refresh_loop(stop))
            logger.info(
                "clob_recorder: token refresh enabled (interval=%.0fs)",
                self._token_refresh_interval_s,
            )
        try:
            await asyncio.gather(*coros)
        except asyncio.CancelledError:
            logger.info("clob_recorder cancelled — flushing remaining buffers")
            raise
        finally:
            try:
                final = await self._flush_buffer()
                if final:
                    logger.info("clob_recorder final flush persisted %d snapshots", final)
            except Exception:
                logger.exception("clob_recorder final snapshot flush failed")
                self.stats["errors"] += 1
            if self._tick_repo is not None:
                try:
                    final_t = await self._flush_ticks()
                    if final_t:
                        logger.info(
                            "clob_recorder final flush persisted %d ticks", final_t
                        )
                except Exception:
                    logger.exception("clob_recorder final tick flush failed")
                    self.stats["errors"] += 1

    async def _on_snapshot(self, _e: str, payload: Any) -> None:
        """Bus handler: convert payload, append to buffer, maybe flush."""
        row = snapshot_to_row(payload)
        if row is None:
            self.stats["errors"] += 1
            return
        self.stats["snapshots_received"] += 1

        should_flush = False
        async with self._buffer_lock:
            self._pending_snapshots.append(row)
            depth = len(self._pending_snapshots)
            if depth > self.stats["buffer_high_water"]:
                self.stats["buffer_high_water"] = depth
            if depth >= self._buffer_size:
                should_flush = True

        if should_flush:
            try:
                await self._flush_buffer()
            except Exception:
                logger.exception("clob_recorder buffer-trigger flush failed")
                self.stats["errors"] += 1

    async def _on_tick(self, _e: str, payload: Any) -> None:
        """Bus handler for EVT_MARKET_TICK — buffer the delta, maybe flush.

        Only invoked when `tick_repo` is configured. Otherwise the
        subscription is never registered and ticks are dropped at the
        bus level (cheaper than allocating + immediately discarding).
        """
        if self._tick_repo is None:
            self.stats["ticks_dropped_no_repo"] += 1
            return
        row = tick_to_row(payload)
        if row is None:
            self.stats["errors"] += 1
            return
        self.stats["ticks_received"] += 1

        should_flush = False
        async with self._tick_buffer_lock:
            self._pending_ticks.append(row)
            depth = len(self._pending_ticks)
            if depth > self.stats["tick_buffer_high_water"]:
                self.stats["tick_buffer_high_water"] = depth
            if depth >= self._tick_buffer_size:
                should_flush = True

        if should_flush:
            try:
                await self._flush_ticks()
            except Exception:
                logger.exception("clob_recorder tick buffer-trigger flush failed")
                self.stats["errors"] += 1

    async def _token_refresh_loop_inner(self, shutdown: asyncio.Event) -> None:
        """Inner refresh impl — wrapped by _token_refresh_loop with a
        defensive try/except so a fetch error or shape bug never tears
        down the recorder via gather() propagation.
        """
        await self._token_refresh_inner(shutdown)

    async def _token_refresh_loop(self, shutdown: asyncio.Event) -> None:
        """Top-level refresh loop — never lets exceptions propagate to gather."""
        try:
            await self._token_refresh_inner(shutdown)
        except Exception:
            logger.exception(
                "clob_recorder token refresh loop crashed — disabling refresh "
                "for this session; recorder continues with current subs"
            )
            self.stats["errors"] += 1

    async def _token_refresh_inner(self, shutdown: asyncio.Event) -> None:
        """Periodically refresh the WS subscription token set.

        Compares the result of `token_refresh_fn()` against the WS's
        current subscribed tokens and:
          - calls subscribe_tokens() for tokens that just became active
          - calls unsubscribe_tokens() for tokens no longer in the set

        Designed for crypto bars: markets spawn (5/15/60-min bars) and
        close constantly. A static token list goes stale within hours.
        """
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=self._token_refresh_interval_s
                )
                return  # shutdown signalled
            except asyncio.TimeoutError:
                pass
            try:
                fresh = await self._token_refresh_fn()  # type: ignore[misc]
            except Exception:
                logger.exception("clob_recorder token refresh fetch failed")
                self.stats["errors"] += 1
                continue
            if not isinstance(fresh, set):
                fresh = set(fresh) if fresh else set()
            if not fresh:
                logger.warning(
                    "clob_recorder token refresh returned empty set — "
                    "skipping diff to avoid unsubscribing all"
                )
                continue
            ws = self._ws
            if ws is None:
                continue
            current = set(ws.subs.subscribed) | set(ws.subs.pending_subscribe())
            to_add = sorted(fresh - current)
            to_drop = sorted(current - fresh)
            if to_add:
                try:
                    ws.subscribe_tokens(to_add)
                    self.stats["tokens_added"] += len(to_add)
                except Exception:
                    logger.exception(
                        "clob_recorder token refresh: subscribe_tokens failed"
                    )
                    self.stats["errors"] += 1
            if to_drop:
                try:
                    ws.unsubscribe_tokens(to_drop)
                    self.stats["tokens_dropped"] += len(to_drop)
                except Exception:
                    logger.exception(
                        "clob_recorder token refresh: unsubscribe_tokens failed"
                    )
                    self.stats["errors"] += 1
            self.stats["token_refreshes"] += 1
            if to_add or to_drop:
                logger.info(
                    "clob_recorder token refresh: +%d new, -%d closed "
                    "(current set size=%d)",
                    len(to_add), len(to_drop), len(fresh),
                )

    async def _flush_loop(self, shutdown: asyncio.Event) -> None:
        """Periodically flush both buffers until shutdown is set."""
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=self._flush_interval_s
                )
                # shutdown signalled — exit; final flush happens in run()
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._flush_buffer()
            except Exception:
                logger.exception("clob_recorder periodic snapshot flush failed")
                self.stats["errors"] += 1
            if self._tick_repo is not None:
                try:
                    await self._flush_ticks()
                except Exception:
                    logger.exception("clob_recorder periodic tick flush failed")
                    self.stats["errors"] += 1

    async def _flush_buffer(self) -> int:
        """Drain the snapshot buffer and persist via the repo.

        Returns count flushed.
        """
        async with self._buffer_lock:
            if not self._pending_snapshots:
                return 0
            batch = self._pending_snapshots
            self._pending_snapshots = []

        try:
            written = await self._repo.insert_many(batch)
        except Exception:
            # Re-queue so we don't drop on transient DB failures. Cap the
            # re-queue size at the configured buffer to avoid runaway RAM.
            async with self._buffer_lock:
                spare = max(0, self._buffer_size - len(self._pending_snapshots))
                if spare > 0:
                    self._pending_snapshots.extend(batch[:spare])
            self.stats["errors"] += 1
            raise

        self.stats["snapshots_persisted"] += written
        return written

    async def _flush_ticks(self) -> int:
        """Drain the tick buffer and persist via the tick_repo.

        Returns count flushed. No-op when tick_repo is None.
        """
        if self._tick_repo is None:
            return 0
        async with self._tick_buffer_lock:
            if not self._pending_ticks:
                return 0
            batch = self._pending_ticks
            self._pending_ticks = []

        try:
            written = await self._tick_repo.insert_many(batch)
        except Exception:
            # Re-queue with capped size mirroring _flush_buffer's behavior.
            async with self._tick_buffer_lock:
                spare = max(0, self._tick_buffer_size - len(self._pending_ticks))
                if spare > 0:
                    self._pending_ticks.extend(batch[:spare])
            self.stats["errors"] += 1
            raise

        self.stats["ticks_persisted"] += written
        return written
