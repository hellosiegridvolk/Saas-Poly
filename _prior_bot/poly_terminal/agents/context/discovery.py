"""DiscoveryAgent — finds live short-window crypto Up/Down markets.

Calls GammaClient.fetch_event_by_slug for each (asset, window, bar) tuple
on a recurring schedule. Deduplicates by market_id, builds a single
watchlist payload, publishes EVT_WATCHLIST_UPDATED.

Watchlist consumers:
  - MarketWebSocket subscribes to YES/NO token_ids for live book data.
  - ScalpWindowStrategy reads bar_start_ts / bar_end_ts to gate entries.
  - DumpHedgeStrategy uses the YES/NO token pairing.
  - ContextAgent evaluates each market's spread/time-left/orderbook flags.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Protocol

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_WATCHLIST_UPDATED
from poly_terminal.data.gamma.models import GammaEvent
from poly_terminal.data.gamma.slugs import (
    build_1h_slug,
    build_15m_slug,
    build_5m_slug,
)

logger = logging.getLogger(__name__)


class _GammaSource(Protocol):
    async def fetch_event_by_slug(self, slug: str) -> GammaEvent | None: ...


@dataclass(frozen=True)
class DiscoveryConfig:
    assets: tuple[str, ...] = ("btc", "eth")
    windows: tuple[str, ...] = ("15m", "1h")
    interval_s: int = 60
    # When True, also probe prev/next bar so clock-skew or boundary timing
    # doesn't miss the current window.
    probe_neighbors: bool = True


class DiscoveryAgent:
    """Periodic short-window market discovery."""

    def __init__(
        self,
        bus: EventBus,
        gamma: _GammaSource,
        cfg: DiscoveryConfig | None = None,
        now_reader: Callable[[], int] | None = None,
    ) -> None:
        self._bus = bus
        self._gamma = gamma
        self._cfg = cfg or DiscoveryConfig()
        self._now = now_reader or (lambda: int(datetime.now(timezone.utc).timestamp()))
        self.last_market_count = 0

    async def run(self, shutdown: asyncio.Event) -> None:
        """Loop until shutdown — run_once immediately, then every interval_s."""
        while not shutdown.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception("discovery: iteration failed")
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=self._cfg.interval_s
                )
                return
            except asyncio.TimeoutError:
                continue
        logger.info("discovery: stopped")

    async def run_once(self) -> int:
        """One discovery pass — returns number of markets discovered."""
        now_ts = self._now()
        slugs = list(self._build_slugs(now_ts))
        events = await self._fetch_all(slugs)
        markets = self._build_markets(events, now_ts)
        if not markets:
            self.last_market_count = 0
            return 0
        await self._bus.publish(
            EVT_WATCHLIST_UPDATED,
            {"markets": markets, "ts": now_ts},
        )
        self.last_market_count = len(markets)
        return len(markets)

    # ── helpers ───────────────────────────────────────────────────────

    def _build_slugs(self, now_ts: int):
        """Yield (asset, window, slug) tuples for every cell in the matrix."""
        offsets = (-1, 0, 1) if self._cfg.probe_neighbors else (0,)
        et_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        for asset in self._cfg.assets:
            for window in self._cfg.windows:
                if window == "15m":
                    for off in offsets:
                        yield asset, "15m", build_15m_slug(asset, now_ts + off * 900)
                elif window == "5m":
                    for off in offsets:
                        yield asset, "5m", build_5m_slug(asset, now_ts + off * 300)
                elif window == "1h":
                    for off in offsets:
                        ts = now_ts + off * 3600
                        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                        try:
                            yield asset, "1h", build_1h_slug(asset, dt)
                        except ValueError:
                            continue
                else:
                    logger.debug("discovery: unsupported window %r", window)

    async def _fetch_all(
        self, slugs: list[tuple[str, str, str]]
    ) -> list[tuple[str, str, GammaEvent]]:
        """Fan-out fetch (asset, window, slug) → list of resolved (asset, window, event)."""
        async def _one(asset: str, window: str, slug: str):
            event = await self._gamma.fetch_event_by_slug(slug)
            return (asset, window, slug, event)

        results = await asyncio.gather(
            *(_one(a, w, s) for a, w, s in slugs), return_exceptions=True
        )
        out: list[tuple[str, str, GammaEvent]] = []
        for r in results:
            if isinstance(r, BaseException):
                continue
            asset, window, _slug, event = r
            if event is None:
                continue
            out.append((asset, window, event))
        return out

    def _build_markets(
        self,
        events: list[tuple[str, str, GammaEvent]],
        now_ts: int,
    ) -> list[dict[str, object]]:
        """Reduce events → flat list of typed market dicts (deduped)."""
        seen: set[str] = set()
        out: list[dict[str, object]] = []
        for asset, window, event in events:
            if not event.active or event.closed:
                continue
            market = event.first_tradable_market()
            if market is None:
                continue
            if market.condition_id in seen:
                continue
            seen.add(market.condition_id)
            yes = market.yes_token()
            no = market.no_token()
            if yes is None or no is None:
                continue
            bar_start, bar_end = self._bar_bounds(window, now_ts)
            out.append(
                {
                    "asset": asset,
                    "window": window,
                    "market_id": market.condition_id,
                    "slug": market.slug,
                    "token_yes": yes.token_id,
                    "token_no": no.token_id,
                    "bar_start_ts": bar_start,
                    "bar_end_ts": bar_end,
                    "end_date_iso": market.end_date_iso,
                }
            )
        return out

    @staticmethod
    def _bar_bounds(window: str, now_ts: int) -> tuple[int, int]:
        if window == "5m":
            start = (now_ts // 300) * 300
            return start, start + 300
        if window == "15m":
            start = (now_ts // 900) * 900
            return start, start + 900
        if window == "1h":
            start = (now_ts // 3600) * 3600
            return start, start + 3600
        return now_ts, now_ts
