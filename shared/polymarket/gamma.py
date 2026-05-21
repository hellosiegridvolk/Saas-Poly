"""Gamma API client wrapper.

Encodes the spec quirks from §3.8 and §16:
- ``clobTokenIds`` is a JSON-encoded string inside a JSON field; parse twice.
- All Gamma timestamps are UTC; never trust local TZ.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import httpx

GAMMA_DEFAULT_BASE_URL = "https://gamma-api.polymarket.com"


def parse_clob_token_ids(raw: Any) -> list[str]:
    """Resolve the double-encoded ``clobTokenIds`` field.

    Gamma returns this as a JSON string nested inside a JSON field, so a
    single parse leaves you with a string instead of the expected list.
    Accept already-parsed lists too — the helper is idempotent.

    Raises ValueError on anything that is not a JSON-encoded array of
    strings or a list of strings.
    """
    if raw is None:
        raise ValueError("clobTokenIds is missing")
    if isinstance(raw, list):
        parsed: list[str] = raw
    elif isinstance(raw, str):
        decoded = json.loads(raw)
        if not isinstance(decoded, list):
            raise ValueError(f"clobTokenIds did not decode to a list: {decoded!r}")
        parsed = decoded
    else:
        raise ValueError(f"clobTokenIds is unsupported type {type(raw).__name__}")
    if not all(isinstance(t, str) for t in parsed):
        raise ValueError("clobTokenIds contained non-string entries")
    return parsed


class GammaClient:
    """Async wrapper around the public Gamma REST API.

    All network calls use ``httpx.AsyncClient`` so we never block the event
    loop (spec §3.3). Construct one per process; share across coroutines.
    """

    def __init__(
        self,
        base_url: str = GAMMA_DEFAULT_BASE_URL,
        client: httpx.AsyncClient | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> GammaClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def get_market(self, condition_id: str) -> Mapping[str, Any]:
        """Fetch a single market by condition_id. Caller resolves clobTokenIds."""
        response = await self._client.get(
            f"{self._base_url}/markets", params={"condition_ids": condition_id}
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            raise ValueError(f"market not found: {condition_id}")
        market = payload[0] if isinstance(payload, list) else payload
        assert isinstance(market, dict)
        return market
