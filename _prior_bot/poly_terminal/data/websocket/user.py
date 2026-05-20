"""User-channel WebSocket — authenticated dispatcher.

Two surfaces:
  - `UserDispatcher` — pure logic; raw msg → bus events. Tested in isolation.
  - `UserWebSocket`  — thin connect/auth/run loop (integration-tested).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from collections import OrderedDict
from typing import Any

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import (
    EVT_ORDER_CANCELLED,
    EVT_ORDER_FILLED,
    EVT_ORDER_SUBMITTED,
    EVT_WALLET_FILL,
)

logger = logging.getLogger(__name__)


def build_l2_auth_headers(
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    funder_address: str = "",
    method: str = "GET",
    path: str = "/auth/api-key",
    timestamp: int | None = None,
) -> dict[str, str]:
    """Build POLY_* L2 auth headers for the WS upgrade request.

    Format matches py-clob-client's `create_level_2_headers`:
      - Secret is urlsafe-base64-decoded before being used as the HMAC key
      - Signature is urlsafe-base64-encoded HMAC-SHA256 of (ts + method + path)
      - Header names use UNDERSCORES (POLY_*), not dashes
    """
    ts = str(timestamp if timestamp is not None else int(time.time()))
    message = ts + method + path
    secret_bytes = base64.urlsafe_b64decode(api_secret)
    sig = hmac.new(secret_bytes, message.encode("utf-8"), hashlib.sha256).digest()
    headers = {
        "POLY_SIGNATURE": base64.urlsafe_b64encode(sig).decode("utf-8"),
        "POLY_TIMESTAMP": ts,
        "POLY_API_KEY": api_key,
        "POLY_PASSPHRASE": api_passphrase,
    }
    if funder_address:
        headers["POLY_ADDRESS"] = funder_address
    return headers


def build_user_subscribe_payload(
    api_key: str, api_secret: str, api_passphrase: str
) -> dict[str, Any]:
    return {
        "auth": {
            "apiKey": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        },
        "markets": [],
        "assets_ids": [],
        "type": "User",
    }


class UserDispatcher:
    """Parses authenticated user-channel events and republishes on the bus.

    `tracked_wallets` is a set of lowercase hex addresses that we mirror
    onto `EVT_WALLET_FILL` for the Wallet Intelligence Agent.
    """

    _DEDUPE_CAPACITY = 2048

    def __init__(
        self,
        bus: EventBus,
        tracked_wallets: set[str],
    ) -> None:
        self._bus = bus
        self._tracked = {w.lower() for w in tracked_wallets}
        self.dropped_count = 0
        # OrderedDict gives us insertion-order LRU eviction.
        self._seen: OrderedDict[str, None] = OrderedDict()

    def update_tracked_wallets(self, wallets: set[str]) -> None:
        self._tracked = {w.lower() for w in wallets}

    async def handle(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            data: Any = json.loads(raw)
        except (ValueError, TypeError):
            self.dropped_count += 1
            return
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    await self._dispatch(item)
        elif isinstance(data, dict):
            await self._dispatch(data)
        else:
            self.dropped_count += 1

    def _seen_already(self, key: str) -> bool:
        if key in self._seen:
            return True
        self._seen[key] = None
        if len(self._seen) > self._DEDUPE_CAPACITY:
            self._seen.popitem(last=False)
        return False

    async def _dispatch(self, event: dict[str, Any]) -> None:
        etype = event.get("event_type") or event.get("type") or ""
        if etype == "order":
            await self._dispatch_order(event)
        elif etype == "trade":
            await self._dispatch_trade(event)
        else:
            self.dropped_count += 1

    async def _dispatch_order(self, event: dict[str, Any]) -> None:
        order_id = str(event.get("id", ""))
        status = str(event.get("status", "")).upper()
        if not order_id or not status:
            self.dropped_count += 1
            return
        dedupe_key = f"order:{order_id}:{status}"
        if self._seen_already(dedupe_key):
            return
        payload = {
            "order_id": order_id,
            "token_id": str(event.get("asset_id", "")),
            "side": str(event.get("side", "")).upper(),
            "price": event.get("price"),
            "size": event.get("size"),
            "filled_size": event.get("filled_size"),
            "status": status,
            "raw": event,
        }
        if status == "LIVE":
            await self._bus.publish(EVT_ORDER_SUBMITTED, payload)
        elif status == "MATCHED":
            await self._bus.publish(EVT_ORDER_FILLED, payload)
        elif status == "CANCELLED":
            await self._bus.publish(EVT_ORDER_CANCELLED, payload)
        elif status == "EXPIRED":
            await self._bus.publish(EVT_ORDER_CANCELLED, payload)
        else:
            self.dropped_count += 1

    async def _dispatch_trade(self, event: dict[str, Any]) -> None:
        wallet = str(event.get("maker_address", "")).lower()
        if not wallet:
            self.dropped_count += 1
            return
        if wallet not in self._tracked:
            return  # not a tracked wallet — silently drop
        trade_id = str(event.get("trade_id", ""))
        if trade_id and self._seen_already(f"trade:{trade_id}"):
            return
        try:
            size = float(str(event.get("size", "0")))
        except (TypeError, ValueError):
            size = 0.0
        try:
            price = float(str(event.get("price", "0")))
        except (TypeError, ValueError):
            price = 0.0
        await self._bus.publish(
            EVT_WALLET_FILL,
            {
                "wallet": wallet,
                "trade_id": trade_id,
                "token_id": str(event.get("asset_id", "")),
                "side": str(event.get("side", "")).upper(),
                "price": price,
                "size": size,
                "ts": int(event.get("timestamp", 0)),
            },
        )


class UserWebSocket:
    """Authenticated User WebSocket — connects, subscribes, drives UserDispatcher.

    Skips connection cleanly when API credentials are empty (so paper-mode
    runs without auth still come up). Operators set the creds before
    promoting to LIVE_DRY/LIVE.

    Connect strategy is overridable for tests via `connect_factory`.
    """

    DEFAULT_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

    def __init__(
        self,
        bus: EventBus,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        tracked_wallets: set[str] | None = None,
        url: str = DEFAULT_URL,
        ping_interval_s: int = 10,
        connect_factory: Any = None,
        funder_address: str = "",
    ) -> None:
        self.bus = bus
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.funder_address = funder_address
        self.url = url
        self.ping_interval_s = ping_interval_s
        self.dispatcher = UserDispatcher(
            bus=bus, tracked_wallets=tracked_wallets or set()
        )
        self._shutdown = asyncio.Event()
        self._connect = connect_factory

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret and self.api_passphrase)

    def update_tracked_wallets(self, wallets: set[str]) -> None:
        self.dispatcher.update_tracked_wallets(wallets)

    def stop(self) -> None:
        self._shutdown.set()

    async def run(self, shutdown: asyncio.Event | None = None) -> None:
        """Maintain the connection until `shutdown` (or self._shutdown) is set.

        If credentials are empty, log once and wait for shutdown without
        connecting. This is the safe path for paper-mode soaks before the
        operator sets POLY_API_*.
        """
        from poly_terminal.data.websocket.reconnector import Backoff

        stop = shutdown or self._shutdown
        if not self.has_credentials:
            logger.info(
                "ws.user: no credentials — skipping connect (paper-only path)"
            )
            await stop.wait()
            return
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
                    "ws.user disconnected (%s) — reconnecting in %.1fs", exc, delay
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                    return
                except asyncio.TimeoutError:
                    continue
        logger.info("ws.user stopped")

    async def _connect_and_stream(self, stop: asyncio.Event) -> None:
        connect = self._connect or self._default_connect
        # Pass L2 auth headers via the WS upgrade request — Polymarket
        # closes the connection seconds after subscribe-only auth.
        auth_headers = build_l2_auth_headers(
            api_key=self.api_key,
            api_secret=self.api_secret,
            api_passphrase=self.api_passphrase,
            funder_address=self.funder_address,
        )
        try:
            cm = connect(self.url, additional_headers=auth_headers)
        except TypeError:
            # Test factories typically don't accept additional_headers.
            cm = connect(self.url)
        async with cm as ws:
            logger.info("ws.user connected to %s", self.url)
            await ws.send(
                json.dumps(
                    build_user_subscribe_payload(
                        self.api_key, self.api_secret, self.api_passphrase
                    )
                )
            )
            await asyncio.gather(
                self._recv_loop(ws, stop),
                self._ping_loop(ws, stop),
            )

    @staticmethod
    def _default_connect(url: str, additional_headers: dict[str, str] | None = None) -> Any:
        import websockets

        kwargs: dict[str, Any] = {
            "ping_interval": None,
            "ping_timeout": None,
            "close_timeout": 5,
            "max_size": 2**22,
        }
        if additional_headers:
            kwargs["additional_headers"] = additional_headers
        return websockets.connect(url, **kwargs)

    async def _recv_loop(self, ws: Any, stop: asyncio.Event) -> None:
        async for raw in ws:
            if stop.is_set():
                break
            try:
                await self.dispatcher.handle(raw)
            except Exception:
                logger.exception("ws.user dispatcher error")

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
