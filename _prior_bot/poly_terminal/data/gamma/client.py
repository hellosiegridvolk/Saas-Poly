"""Async Gamma API client — typed responses + latency budget.

The base URL is configurable so tests can point at a local fixture server.
For production use the default `https://gamma-api.polymarket.com`.

Every public method is wrapped in `@latency_tracked(budget)` so a slow
upstream cleanly trips the circuit breaker (see ADR + 02 §4).
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from poly_terminal.data.gamma.models import GammaEvent, parse_event
from poly_terminal.data.latency_budget import LatencyBudget, latency_tracked

logger = logging.getLogger(__name__)


class GammaClient:
    """Thin async wrapper over Polymarket Gamma /events + /markets."""

    def __init__(
        self,
        base_url: str = "https://gamma-api.polymarket.com",
        session: aiohttp.ClientSession | None = None,
        budget: LatencyBudget | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = session
        self._owned_session = session is None
        self._budget = budget or LatencyBudget(
            name="gamma", ceiling_ms=1000, window_size=50
        )

    async def __aenter__(self) -> "GammaClient":
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owned_session = True
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owned_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owned_session = True
        return self._session

    async def fetch_event_by_slug(self, slug: str) -> GammaEvent | None:
        """Return the event for `slug`, or None if not active/closed/missing.

        An "event" container can hold multiple markets; the caller usually
        wants `event.first_tradable_market()` for the trading-side view.
        """

        @latency_tracked(self._budget)
        async def _call() -> GammaEvent | None:
            session = self._ensure_session()
            async with session.get(
                f"{self.base_url}/events", params={"slug": slug}
            ) as resp:
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    return None
                items: list[dict[str, Any]] = await resp.json()
            if not items:
                return None
            payload = items[0]
            event = parse_event(payload)
            if not event.active or event.closed:
                return None
            return event

        return await _call()

    async def fetch_events_by_tag(
        self, tag_id: int, limit: int = 20
    ) -> list[GammaEvent]:
        """Tag-id fallback when slug build misses (Bug #4 mitigation)."""

        @latency_tracked(self._budget)
        async def _call() -> list[GammaEvent]:
            session = self._ensure_session()
            params = {
                "tag_id": tag_id,
                "active": "true",
                "closed": "false",
                "limit": limit,
            }
            async with session.get(
                f"{self.base_url}/events", params=params
            ) as resp:
                if resp.status != 200:
                    return []
                items: list[dict[str, Any]] = await resp.json()
            return [parse_event(p) for p in items]

        return await _call()

    @property
    def latency(self) -> LatencyBudget:
        return self._budget
