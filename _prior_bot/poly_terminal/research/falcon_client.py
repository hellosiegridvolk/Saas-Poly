"""Async httpx client for Falcon / Heisenberg semantic-retrieve API.

Used by the offline research module to pull historical Polymarket market,
trade, and (potentially) orderbook data for backtesting. The bot's live
hot-path NEVER calls this module — it is research-only.

Auth: token comes from `FALCON_TOKEN` env var ONLY. Never written to disk.

Endpoint: POST {BASE}/api/v2/semantic/retrieve/parameterized
Body shape:
    {
        "agent_id": int,
        "params": {...},                       # caller-defined
        "pagination": {"limit": int, "offset": int},
        "formatter_config": {"format_type": "raw"},
    }

Retry policy: 3 attempts on 429/5xx with exponential backoff (1s, 2s, 4s).
Other 4xx errors raise immediately.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, AsyncIterator

import httpx

_BASE_URL = "https://narrative.agent.heisenberg.so"
_ENDPOINT = "/api/v2/semantic/retrieve/parameterized"
_DEFAULT_TIMEOUT_S = 30.0
_MAX_ATTEMPTS = 3
_BACKOFF_SCHEDULE_S = (1.0, 2.0, 4.0)
_CONCURRENCY_CAP = 30


class FalconAuthError(RuntimeError):
    """Raised when the Falcon token is missing, empty, or rejected."""


class FalconAPIError(RuntimeError):
    """Raised on non-auth API failures (4xx other than 401/403, exhausted retries on 5xx/429)."""


class FalconClient:
    """Async client for the Falcon parameterized semantic-retrieve endpoint.

    Use as an async context manager:

        async with FalconClient() as c:
            rows = await c.query(agent_id=574, params={...})

    The client owns one httpx.AsyncClient and a soft concurrency semaphore
    to avoid hammering the upstream when many tasks fan out at once.
    """

    def __init__(self, timeout_s: float | None = None) -> None:
        token = os.environ.get("FALCON_TOKEN", "")
        if not token:
            raise FalconAuthError(
                "FALCON_TOKEN not set in environment — cannot authenticate "
                "to Falcon. Export FALCON_TOKEN=<your_token> and retry."
            )
        self._token = token

        # Resolution priority: env var > arg > default.
        env_timeout = os.environ.get("FALCON_TIMEOUT_S")
        if env_timeout:
            try:
                resolved_timeout = float(env_timeout)
            except ValueError:
                resolved_timeout = timeout_s if timeout_s is not None else _DEFAULT_TIMEOUT_S
        elif timeout_s is not None:
            resolved_timeout = float(timeout_s)
        else:
            resolved_timeout = _DEFAULT_TIMEOUT_S
        self._timeout_s = resolved_timeout

        self._semaphore = asyncio.Semaphore(_CONCURRENCY_CAP)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "FalconClient":
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            timeout=self._timeout_s,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def query(
        self,
        agent_id: int,
        params: dict[str, Any],
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Single-page query. Returns the raw `data` list of dicts.

        Retries on 429/5xx with exponential backoff. Raises FalconAPIError on
        non-recoverable failures, FalconAuthError on 401/403.
        """
        if self._client is None:
            raise RuntimeError(
                "FalconClient not entered — use `async with FalconClient() as c:`"
            )

        body = {
            "agent_id": agent_id,
            "params": params,
            "pagination": {"limit": limit, "offset": offset},
            "formatter_config": {"format_type": "raw"},
        }

        async with self._semaphore:
            response = await self._post_with_retry(body)

        payload = response.json()
        # Falcon returns data under a "data" key; tolerate both shapes for safety.
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return data
            # Some endpoints nest under data.results
            if isinstance(data, dict):
                inner = data.get("results")
                if isinstance(inner, list):
                    return inner
            results = payload.get("results")
            if isinstance(results, list):
                return results
            return []
        if isinstance(payload, list):
            return payload
        return []

    async def query_all(
        self,
        agent_id: int,
        params: dict[str, Any],
        page_size: int = 100,
    ) -> AsyncIterator[dict[str, Any]]:
        """Async iterator that walks pages until an empty page is returned."""
        offset = 0
        while True:
            page = await self.query(agent_id, params, limit=page_size, offset=offset)
            if not page:
                return
            for row in page:
                yield row
            if len(page) < page_size:
                # Short page → end of stream.
                return
            offset += page_size

    async def _post_with_retry(self, body: dict[str, Any]) -> httpx.Response:
        assert self._client is not None
        last_exc: Exception | None = None
        for attempt in range(_MAX_ATTEMPTS):
            try:
                response = await self._client.post(_ENDPOINT, json=body)
            except httpx.RequestError as exc:
                last_exc = exc
                if attempt < _MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_BACKOFF_SCHEDULE_S[attempt])
                    continue
                raise FalconAPIError(
                    f"Falcon transport error after {_MAX_ATTEMPTS} attempts: {exc!r}"
                ) from exc

            status = response.status_code
            if 200 <= status < 300:
                return response
            if status in (401, 403):
                raise FalconAuthError(
                    f"Falcon rejected token (status {status}): {response.text[:200]}"
                )
            if status == 429 or 500 <= status < 600:
                # Retryable.
                if attempt < _MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_BACKOFF_SCHEDULE_S[attempt])
                    continue
                raise FalconAPIError(
                    f"Falcon retryable error {status} exhausted after "
                    f"{_MAX_ATTEMPTS} attempts: {response.text[:200]}"
                )
            # Other 4xx — no retry.
            raise FalconAPIError(
                f"Falcon API error {status}: {response.text[:200]}"
            )

        # Defensive fallthrough; should never be reached.
        raise FalconAPIError(
            f"Falcon request exhausted retries: {last_exc!r}"
        )
