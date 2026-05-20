"""Sync wrapper around Polymarket Gamma's `/markets` endpoint.

Phase 32 P3 (2026-05-10) — closes the last endgame_yield wiring debt.
The strategy's `evaluate_market` callable expects a sync function that
returns `GammaMarketMeta | None` for a given `market_id`. This module
provides that, with:

  * urllib-based HTTP (zero new top-level deps; matches the pattern
    used by `CTFBalanceReader` and `live_readiness.gate_usdc_visible`)
  * In-memory TTL cache (positive + negative) so the strategy can be
    triggered on every EVT_CONTEXT_OK without burning Gamma quota
  * Defensive parsing — malformed `clobTokenIds`, missing fields,
    paginated-default responses all return None instead of raising
  * Tests inject a `transport` stub so no real HTTP is touched
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Protocol
from urllib.parse import urlencode

from poly_terminal.agents.strategy.endgame_evaluator import GammaMarketMeta


logger = logging.getLogger(__name__)


_DEFAULT_BASE_URL = "https://gamma-api.polymarket.com"
_DEFAULT_TIMEOUT_S = 8.0
_DEFAULT_CACHE_TTL_S = 300       # 5 min — market metadata changes slowly
_DEFAULT_NEGATIVE_CACHE_TTL_S = 30  # not-yet-indexed lookups retry after 30s


class _Transport(Protocol):
    """Anything that can issue an HTTP GET. Returns (status, body_bytes)."""

    def request(self, url: str) -> tuple[int, bytes]:
        ...


class _UrllibTransport:
    """Production transport — `urllib.request.urlopen` with realistic UA.

    Public RPCs (Gamma included) reject the default python-urllib UA;
    using a Mozilla-like UA matches what `CTFBalanceReader` and
    `live_readiness.gate_usdc_visible` already do for the same reason.
    """

    def __init__(self, *, timeout_s: float) -> None:
        self._timeout_s = timeout_s

    def request(self, url: str) -> tuple[int, bytes]:
        import urllib.request
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": (
                    "Mozilla/5.0 (poly-endgame-yield/1.0) "
                    "PolymarketCanary/1.0"
                ),
            },
        )
        with urllib.request.urlopen(req, timeout=self._timeout_s) as resp:
            return resp.status, resp.read()


@dataclass
class _CacheEntry:
    meta: GammaMarketMeta | None
    expires_at: float


class GammaMetadataFetcher:
    """Sync, cached lookup of Gamma `/markets` metadata.

    Use as the `gamma_metadata_fetcher` callable on
    `EndgameMarketEvaluator`. The bound `.fetch` method has the right
    signature: `(market_id: str) -> GammaMarketMeta | None`.
    """

    def __init__(
        self,
        *,
        base_url: str = _DEFAULT_BASE_URL,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        cache_ttl_s: float = _DEFAULT_CACHE_TTL_S,
        negative_cache_ttl_s: float = _DEFAULT_NEGATIVE_CACHE_TTL_S,
        transport: _Transport | None = None,
        now_fn: Callable[[], float] = time.time,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._cache_ttl_s = cache_ttl_s
        self._negative_cache_ttl_s = negative_cache_ttl_s
        self._transport: _Transport = (
            transport if transport is not None
            else _UrllibTransport(timeout_s=timeout_s)
        )
        self._now = now_fn
        self._cache: dict[str, _CacheEntry] = {}

    def fetch(self, market_id: str) -> GammaMarketMeta | None:
        """Return GammaMarketMeta for `market_id`, or None if not found
        / unparseable / Gamma error. Cached for `cache_ttl_s` (positive
        results) or `negative_cache_ttl_s` (None results)."""
        now = self._now()
        hit = self._cache.get(market_id)
        if hit is not None and hit.expires_at > now:
            return hit.meta
        meta = self._fetch_raw(market_id)
        ttl = (
            self._cache_ttl_s if meta is not None
            else self._negative_cache_ttl_s
        )
        self._cache[market_id] = _CacheEntry(meta=meta, expires_at=now + ttl)
        return meta

    # ── Internal ──────────────────────────────────────────────────

    def _fetch_raw(self, market_id: str) -> GammaMarketMeta | None:
        url = (
            f"{self._base_url}/markets?"
            + urlencode({"condition_ids": market_id})
        )
        try:
            status, body = self._transport.request(url)
        except Exception as exc:
            logger.debug(
                "GammaMetadataFetcher: transport error for %s — %s",
                market_id, exc,
            )
            return None
        if status != 200:
            logger.debug(
                "GammaMetadataFetcher: HTTP %d for %s",
                status, market_id,
            )
            return None
        try:
            payload = json.loads(body)
        except (ValueError, TypeError):
            logger.debug(
                "GammaMetadataFetcher: malformed JSON for %s", market_id,
            )
            return None
        if not isinstance(payload, list) or not payload:
            return None
        # Defensive: confirm the row's conditionId matches what we
        # asked for. Gamma occasionally serves paginated defaults
        # when a filter is silently ignored.
        for row in payload:
            if not isinstance(row, dict):
                continue
            if str(row.get("conditionId", "")).lower() != market_id.lower():
                continue
            return _parse_market_row(market_id, row)
        return None


def _parse_market_row(
    market_id: str, row: dict[str, Any],
) -> GammaMarketMeta | None:
    """Convert a Gamma `/markets` row to GammaMarketMeta. Returns None
    when the row is structurally unusable (e.g., malformed clob ids)."""
    raw_clob = row.get("clobTokenIds")
    yes_token: str | None = None
    no_token: str | None = None
    if isinstance(raw_clob, list):
        ids = [str(v) for v in raw_clob]
    elif isinstance(raw_clob, str):
        try:
            parsed = json.loads(raw_clob)
        except (ValueError, TypeError):
            return None
        if not isinstance(parsed, list):
            return None
        ids = [str(v) for v in parsed]
    else:
        ids = []
    if len(ids) >= 1:
        yes_token = ids[0] or None
    if len(ids) >= 2:
        no_token = ids[1] or None
    if yes_token is None and no_token is None:
        return None

    end_date_iso = row.get("endDate") or row.get("end_date_iso")
    end_date_iso = str(end_date_iso) if end_date_iso else None
    close_unix = _coerce_unix_seconds(
        row.get("endDateUnix") or row.get("end_date_unix"),
        end_date_iso,
    )
    return GammaMarketMeta(
        market_id=market_id,
        end_date_iso=end_date_iso,
        close_time_unix_s=close_unix,
        yes_token_id=yes_token,
        no_token_id=no_token,
    )


def _coerce_unix_seconds(
    raw_unix: Any, end_date_iso: str | None,
) -> int:
    """Best-effort conversion to unix seconds. Falls back to parsing
    `end_date_iso` when no explicit unix field is present. Returns 0
    if neither yields a usable timestamp."""
    if raw_unix is not None:
        try:
            v = int(raw_unix)
            # Gamma sometimes returns ms rather than seconds — normalize.
            if v > 10_000_000_000:
                v //= 1000
            return v
        except (TypeError, ValueError):
            pass
    if end_date_iso:
        try:
            s = end_date_iso.replace("Z", "+00:00")
            return int(datetime.fromisoformat(s).timestamp())
        except (TypeError, ValueError):
            return 0
    return 0


__all__ = [
    "GammaMetadataFetcher",
]
