"""WalletActivityPoller — polls Data API /activity per followed wallet.

Polymarket's User WebSocket only streams events for the authenticated
user. To copy-trade arbitrary whales, the bot polls Data API
`/activity?user=<wallet>` for each followed wallet on a tight cadence
and emits EVT_WALLET_FILL for every new TRADE event it sees.

Cadence: 3s default (well below Data API's ~30 req/s budget for 6-60
followed wallets).

Dedupe: per-wallet `seen` set keyed by transactionHash. Bounded by
trimming to the most-recent N hashes per wallet so the set doesn't
grow unbounded.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_WALLET_FILL,
    EVT_WALLET_RANK_CHANGED,
    EVT_WALLET_REDEEM,
)

logger = logging.getLogger(__name__)


class _ActivitySource(Protocol):
    async def fetch_activity(
        self, wallet: str, limit: int = 10
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class PollerConfig:
    interval_s: float = 3.0
    limit_per_wallet: int = 20
    dedupe_capacity: int = 256
    # Concurrency cap so a long-tail Data API response doesn't queue up.
    max_concurrent: int = 8
    # Tracked-only "seed" cadence: every Nth fast tick we also poll the
    # tracked-but-not-followed set so wallet_history can accumulate for
    # candidates that haven't earned into the followed tier yet. 20 → ~60s
    # at the 3s default fast cadence. 0 disables the slow tier.
    slow_tier_ratio: int = 20
    slow_tier_limit: int = 10


@dataclass
class _PerWallet:
    seen: OrderedDict[str, None] = field(default_factory=OrderedDict)


class WalletActivityPoller:
    def __init__(
        self,
        bus: EventBus,
        data_api: _ActivitySource,
        cfg: PollerConfig | None = None,
    ) -> None:
        self._bus = bus
        self._api = data_api
        self._cfg = cfg or PollerConfig()
        self._followed: set[str] = set()
        # Tracked-but-not-followed wallets get a slower cadence so that
        # wallet_history accumulates for cold-start candidates. Without
        # this, only wallets already on the followed set ever earn data,
        # and the unfollowed tier can never accumulate enough closed
        # trades to score above the win-rate floor.
        self._tracked: set[str] = set()
        self._state: dict[str, _PerWallet] = {}
        self._sem: asyncio.Semaphore | None = None
        self._started = False

    @property
    def followed_count(self) -> int:
        return len(self._followed)

    @property
    def tracked_only_count(self) -> int:
        return len(self._tracked - self._followed)

    def set_followed(self, wallets: set[str]) -> None:
        self._followed = {w.lower() for w in wallets}
        # Drop state for wallets neither followed NOR tracked. We keep
        # state for tracked-but-not-followed wallets so the seen-tx
        # dedupe set survives across the fast/slow tier boundary.
        keep = self._followed | self._tracked
        for w in list(self._state.keys()):
            if w not in keep:
                del self._state[w]

    def set_tracked(self, wallets: set[str]) -> None:
        """Update the slow-tier set. Tracked wallets get polled at
        roughly slow_tier_ratio × interval_s cadence so wallet_history
        accumulates for unfollowed candidates."""
        self._tracked = {w.lower() for w in wallets}
        keep = self._followed | self._tracked
        for w in list(self._state.keys()):
            if w not in keep:
                del self._state[w]

    async def start(self) -> None:
        if self._started:
            return
        self._bus.subscribe(EVT_WALLET_RANK_CHANGED, self._on_rank)
        self._started = True

    async def _on_rank(self, _e: str, payload: Any) -> None:
        if isinstance(payload, dict):
            self.set_followed({w for w in payload.get("followed", set())})

    async def run(self, shutdown: asyncio.Event) -> None:
        """Runs once immediately, then every interval_s until shutdown.

        Every `slow_tier_ratio` iterations we also poll the
        tracked-but-not-followed set so cold-start candidates can
        accumulate wallet_history rows toward the win-rate floor.
        """
        if not self._started:
            await self.start()
        self._sem = asyncio.Semaphore(self._cfg.max_concurrent)
        iteration = 0
        ratio = max(0, int(self._cfg.slow_tier_ratio))
        while not shutdown.is_set():
            try:
                await self.poll_once()
                if ratio > 0 and iteration % ratio == 0:
                    await self.poll_tracked_seed()
            except Exception:
                logger.exception("activity_poller: iteration failed")
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=self._cfg.interval_s
                )
                return
            except asyncio.TimeoutError:
                iteration += 1
                continue

    async def poll_once(self) -> int:
        """Fast-tier pass over `_followed`. Returns events emitted."""
        wallets = list(self._followed)
        if not wallets:
            return 0
        return await self._poll_set(wallets, limit=self._cfg.limit_per_wallet)

    async def poll_tracked_seed(self) -> int:
        """Slow-tier pass over tracked-but-not-followed wallets.

        Uses a shorter `slow_tier_limit` per fetch (typically 10 vs 20)
        so the Data API budget stays reasonable when we're seeding
        history for ~50+ unfollowed candidates. Returns events emitted.
        """
        seed = self._tracked - self._followed
        if not seed:
            return 0
        return await self._poll_set(
            list(seed), limit=self._cfg.slow_tier_limit
        )

    async def _poll_set(self, wallets: list[str], *, limit: int) -> int:
        sem = self._sem or asyncio.Semaphore(self._cfg.max_concurrent)

        async def _one(wallet: str) -> int:
            async with sem:
                return await self._poll_wallet(wallet, limit=limit)

        results = await asyncio.gather(
            *(_one(w) for w in wallets), return_exceptions=True
        )
        return sum(r for r in results if isinstance(r, int))

    async def _poll_wallet(self, wallet: str, *, limit: int | None = None) -> int:
        try:
            activity = await self._api.fetch_activity(
                wallet, limit=limit if limit is not None else self._cfg.limit_per_wallet
            )
        except Exception:
            logger.exception("activity_poller: fetch failed for %s", wallet)
            return 0
        emitted = 0
        # Reverse-iterate so older trades are emitted first (chronological).
        for item in reversed(activity):
            t = str(item.get("type", "")).upper()
            if t not in ("TRADE", "REDEEM"):
                continue
            txhash = str(item.get("transactionHash", "")).lower()
            if not txhash:
                continue
            state = self._state.setdefault(wallet, _PerWallet())
            if txhash in state.seen:
                continue
            state.seen[txhash] = None
            if len(state.seen) > self._cfg.dedupe_capacity:
                state.seen.popitem(last=False)
            if t == "TRADE":
                await self._bus.publish(
                    EVT_WALLET_FILL, self._normalize(wallet, item)
                )
            else:  # REDEEM — Polymarket binaries don't sell-out; they redeem
                await self._bus.publish(
                    EVT_WALLET_REDEEM, self._normalize_redeem(wallet, item)
                )
            emitted += 1
        return emitted

    @staticmethod
    def _normalize(wallet: str, item: dict[str, Any]) -> dict[str, Any]:
        slug = str(item.get("slug", item.get("eventSlug", "")))
        return {
            "wallet": wallet,
            "trade_id": str(item.get("transactionHash", "")).lower(),
            "token_id": str(item.get("asset", "")),
            "market_id": str(item.get("conditionId", "")),
            "side": str(item.get("side", "")).upper(),
            "price": float(item.get("price", 0) or 0),
            "size": float(item.get("size", 0) or 0),
            "ts": int(item.get("timestamp", 0)),
            "title": str(item.get("title", "")),
            "slug": slug,
            "end_date_iso": _derive_end_date_iso(slug),
        }

    @staticmethod
    def _normalize_redeem(wallet: str, item: dict[str, Any]) -> dict[str, Any]:
        """REDEEM rows from /activity carry no `asset`/`side` (the contract
        redeems all winning tokens at once), but `conditionId` and `size`
        (USDC payout) are populated. We pass through the conditionId as
        `market_id` so the ingestor can close every open BUY for that
        market on this wallet — winners only; losers never appear.
        """
        return {
            "wallet": wallet,
            "trade_id": str(item.get("transactionHash", "")).lower(),
            "market_id": str(item.get("conditionId", "")),
            "payout_usd": float(item.get("usdcSize", item.get("size", 0)) or 0),
            "ts": int(item.get("timestamp", 0)),
            "title": str(item.get("title", "")),
            "slug": str(item.get("slug", item.get("eventSlug", ""))),
        }


_SHORT_WINDOW_SLUG_RE = re.compile(
    r"-updown-(5m|15m)-(\d{10,})$"
)
_HOURLY_SLUG_RE = re.compile(
    r"-up-or-down-([a-z]+)-(\d{1,2})-(\d{4})-(\d{1,2})(am|pm)-et$"
)
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def _derive_end_date_iso(slug: str) -> str:
    """Compute `end_date_iso` from a short-window crypto slug.

      btc-updown-5m-{unix_ts}                    → ts + 300s
      btc-updown-15m-{unix_ts}                   → ts + 900s
      bitcoin-up-or-down-april-30-2026-7am-et    → 7am ET + 1h

    Returns "" for any other slug shape. Discovery's payload includes
    end_date_iso directly; this only helps the activity-poller path
    where Data API doesn't return it.
    """
    if not slug:
        return ""
    h = _HOURLY_SLUG_RE.search(slug)
    if h:
        from zoneinfo import ZoneInfo

        month_name, day, year, hour, ampm = h.groups()
        month = _MONTHS.get(month_name)
        if month is None:
            return ""
        h12 = int(hour)
        h24 = h12 % 12 + (12 if ampm == "pm" else 0)
        try:
            et = datetime(
                int(year), month, int(day), h24, 0, tzinfo=ZoneInfo("America/New_York")
            )
        except ValueError:
            return ""
        # Bar end = bar_start + 1h.
        end_utc = et.astimezone(timezone.utc) + (
            datetime(2000, 1, 1, 1, 0, tzinfo=timezone.utc)
            - datetime(2000, 1, 1, 0, 0, tzinfo=timezone.utc)
        )
        return end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    m = _SHORT_WINDOW_SLUG_RE.search(slug)
    if not m:
        return ""
    window, ts_str = m.group(1), m.group(2)
    bar_start = int(ts_str)
    seconds = 300 if window == "5m" else 900
    end_dt = datetime.fromtimestamp(bar_start + seconds, tz=timezone.utc)
    return end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
