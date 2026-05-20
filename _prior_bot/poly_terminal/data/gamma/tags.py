"""Discover + cache Polymarket tag IDs.

Used by the Discovery Agent's tag-id fallback path: when slug build returns
404, query `/events?tag_id=<crypto-window-tag>` to find currently-live
short-window events. Persisting tag IDs in code is brittle — Polymarket
re-numbers them — so we discover them at boot and refresh on a TTL.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TagId:
    tag_id: int
    label: str
    slug: str | None = None


class TagCache:
    """Reads `/tags?limit=100`, caches for `ttl_s` seconds."""

    def __init__(
        self,
        base_url: str = "https://gamma-api.polymarket.com",
        session: aiohttp.ClientSession | None = None,
        ttl_s: float = 3600.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = session
        self._owned_session = session is None
        self._ttl_s = ttl_s
        self._tags: list[TagId] = []
        self._loaded_at: float | None = None

    async def close(self) -> None:
        if self._owned_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owned_session = True
        return self._session

    def _cache_fresh(self) -> bool:
        return (
            self._loaded_at is not None
            and (time.monotonic() - self._loaded_at) < self._ttl_s
        )

    async def load(self) -> list[TagId]:
        if self._cache_fresh():
            return list(self._tags)
        session = self._ensure_session()
        try:
            async with session.get(
                f"{self.base_url}/tags", params={"limit": 100}
            ) as resp:
                if resp.status != 200:
                    return []
                items: list[dict[str, Any]] = await resp.json()
        except aiohttp.ClientError as exc:
            logger.warning("tag fetch failed: %s", exc)
            return []
        out: list[TagId] = []
        for item in items:
            try:
                out.append(
                    TagId(
                        tag_id=int(item["id"]),
                        label=str(item.get("label", "")),
                        slug=item.get("slug"),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        self._tags = out
        self._loaded_at = time.monotonic()
        return list(self._tags)

    def find_by_slug(self, slug: str) -> TagId | None:
        for tag in self._tags:
            if tag.slug == slug:
                return tag
        return None

    def find_by_label_substring(self, needle: str) -> TagId | None:
        n = needle.lower()
        for tag in self._tags:
            if n in tag.label.lower():
                return tag
        return None
