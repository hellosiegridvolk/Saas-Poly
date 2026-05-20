"""PolygonLogSubscriber — real-time OrderFilled events via Polygon WebSocket.

Option A latency fix (2026-05-20):
  Data API /activity median lag: 30s (p90: 196s)
  On-chain log subscriber lag:   2-5s (Polygon block time + WS delivery)

Subscribes to eth_subscribe("logs") on the Polymarket Exchange V2 and
NegRisk V2 contracts. Decodes OrderFilled events and emits EVT_WALLET_FILL
in the same format as WalletActivityPoller so copy_scalp picks them up
transparently.

Exchange contracts (Polygon mainnet, from py_clob_client_v2/config.py):
  Exchange V2:      0xE111180000d2663C0091e4f400237545B87B996B
  NegRisk V2:       0xe2222d279d744050d28e00520010520000310F59

OrderFilled(V2) event:
  topic[0]: 0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee
  topic[1]: orderHash (bytes32, indexed)
  topic[2]: maker     (address, indexed)  ← wallet that placed the order
  topic[3]: taker     (address, indexed)
  data:     side(uint8) | tokenId(uint256) | makerAmountFilled(uint256)
            | takerAmountFilled(uint256) | fee(uint256) | builder(bytes32)
            | metadata(bytes32)
  side: 0 = BUY, 1 = SELL

Token→market_id resolution: Gamma API /markets?clob_token_ids=<id>.
Results are cached indefinitely (token IDs are stable — they map to a
single market for the lifetime of the deployment).

Dedup: per-txhash seen-set (shared with WalletActivityPoller via the
bus — if both fire the same trade, copy_scalp's own dedup in
_on_wallet_fill drops the duplicate because it checks the same trade_id).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aiohttp

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_WALLET_FILL

logger = logging.getLogger(__name__)

# Polymarket Exchange V2 and NegRisk V2 on Polygon mainnet.
_EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B".lower()
_NEG_RISK_V2 = "0xe2222d279d744050d28e00520010520000310F59".lower()

# keccak256("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)")
# Verified live against Polygon mainnet 2026-05-20.
_ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)

# EVM word size in hex chars (32 bytes = 64 hex chars)
_WORD = 64

# Reconnect backoff: [1, 2, 4, 8, 16, 30] seconds, then cap at 30.
_BACKOFF = [1, 2, 4, 8, 16, 30]


def _decode_order_filled(log: dict[str, Any]) -> dict[str, Any] | None:
    """Decode a raw eth_getLogs/eth_subscribe log entry.

    Returns a dict with keys:
      tx_hash, maker, taker, side (0=BUY,1=SELL),
      token_id (decimal str), price (float), size_shares (float),
      block_number (int)
    or None if the log is not a parseable OrderFilled event.
    """
    topics = log.get("topics", [])
    if len(topics) < 4:
        return None
    if topics[0].lower() != _ORDER_FILLED_TOPIC:
        return None

    maker = "0x" + topics[2][-40:].lower()
    taker = "0x" + topics[3][-40:].lower()

    data_hex = log.get("data", "0x")[2:]
    # V2 data layout: side | tokenId | makerAmt | takerAmt | fee | builder | metadata
    # Minimum 5 words (side through fee).
    if len(data_hex) < 5 * _WORD:
        return None

    chunks = [data_hex[i:i + _WORD] for i in range(0, len(data_hex), _WORD)]

    try:
        side = int(chunks[0], 16)          # 0=BUY, 1=SELL
        token_id = int(chunks[1], 16)      # uint256
        maker_amt = int(chunks[2], 16)     # USDC in 1e6
        taker_amt = int(chunks[3], 16)     # shares in 1e6
    except (ValueError, IndexError):
        return None

    if taker_amt == 0:
        return None

    price = maker_amt / taker_amt          # USDC/share (already in same scale)
    size_shares = taker_amt / 1_000_000   # human shares

    try:
        block_number = int(log.get("blockNumber", "0x0"), 16)
    except (ValueError, TypeError):
        block_number = 0

    return {
        "tx_hash": str(log.get("transactionHash", "")).lower(),
        "maker": maker,
        "taker": taker,
        "side": side,
        "token_id": str(token_id),
        "price": price,
        "size_shares": size_shares,
        "block_number": block_number,
    }


class _TokenMarketCache:
    """Async cache: token_id (decimal str) → conditionId (market_id).

    Resolves via Gamma API /markets?clob_token_ids=<id>.
    Cache is write-once — token IDs never change markets.
    """

    def __init__(self, gamma_url: str, session: aiohttp.ClientSession) -> None:
        self._base = gamma_url.rstrip("/")
        self._session = session
        self._cache: dict[str, str] = {}
        self._inflight: dict[str, asyncio.Future[str]] = {}

    async def get_market_id(self, token_id: str) -> str | None:
        if token_id in self._cache:
            return self._cache[token_id]

        # Coalesce concurrent requests for the same token.
        if token_id in self._inflight:
            try:
                return await asyncio.shield(self._inflight[token_id])
            except Exception:
                return None

        fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()
        self._inflight[token_id] = fut
        try:
            url = f"{self._base}/markets"
            params = {"clob_token_ids": token_id, "limit": "1"}
            async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status != 200:
                    fut.set_result("")
                    return None
                items = await resp.json()
            if not items:
                fut.set_result("")
                return None
            market_id = str(items[0].get("conditionId", "")).lower()
            self._cache[token_id] = market_id
            fut.set_result(market_id)
            return market_id or None
        except Exception as exc:
            logger.debug("token_market_cache: lookup failed for %s: %s", token_id, exc)
            if not fut.done():
                fut.set_exception(exc)
            return None
        finally:
            self._inflight.pop(token_id, None)


class PolygonLogSubscriber:
    """Real-time Polymarket OrderFilled events via Polygon WebSocket.

    Emits EVT_WALLET_FILL for BUY orders from followed wallets,
    in the same payload format as WalletActivityPoller so downstream
    copy_scalp strategy requires no changes.

    Call `set_followed(wallets)` to update the followed set at runtime.
    Call `run(shutdown)` as an asyncio task.
    """

    def __init__(
        self,
        bus: EventBus,
        *,
        rpc_ws_url: str = "wss://polygon.publicnode.com",
        gamma_url: str = "https://gamma-api.polymarket.com",
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._bus = bus
        self._rpc_ws_url = rpc_ws_url
        self._gamma_url = gamma_url
        self._external_session = session is not None
        self._session: aiohttp.ClientSession = session or aiohttp.ClientSession()
        self._followed: set[str] = set()
        self._seen: set[str] = set()          # txhash dedup (bounded)
        self._seen_order: list[str] = []      # insertion order for bounded evict
        self._market_cache = _TokenMarketCache(gamma_url, self._session)
        self._sub_id: str | None = None
        self._stats = {"events_received": 0, "fills_emitted": 0, "reconnects": 0}

    def set_followed(self, wallets: set[str]) -> None:
        self._followed = {w.lower() for w in wallets}

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    async def run(self, shutdown: asyncio.Event) -> None:
        attempt = 0
        while not shutdown.is_set():
            try:
                await self._connect_and_stream(shutdown)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if shutdown.is_set():
                    return
                delay = _BACKOFF[min(attempt, len(_BACKOFF) - 1)]
                logger.warning(
                    "polygon_log_subscriber: disconnected (%s), reconnect in %ds",
                    type(exc).__name__, delay,
                )
                self._stats["reconnects"] += 1
                self._sub_id = None
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=delay)
                    return
                except asyncio.TimeoutError:
                    attempt += 1
                    continue
        if not self._external_session:
            await self._session.close()

    async def _connect_and_stream(self, shutdown: asyncio.Event) -> None:
        import websockets

        logger.info("polygon_log_subscriber: connecting to %s", self._rpc_ws_url)
        async with websockets.connect(
            self._rpc_ws_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            # Subscribe to logs from both exchange contracts.
            sub_payload = json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [_EXCHANGE_V2, _NEG_RISK_V2],
                        "topics": [_ORDER_FILLED_TOPIC],
                    },
                ],
            })
            await ws.send(sub_payload)
            # First message is the subscription confirmation.
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            resp = json.loads(raw)
            self._sub_id = resp.get("result")
            logger.info(
                "polygon_log_subscriber: subscribed (sub_id=%s, "
                "watching %d followed wallets)",
                self._sub_id, len(self._followed),
            )

            async def _recv_loop() -> None:
                async for message in ws:
                    self._stats["events_received"] += 1
                    await self._handle_message(message)

            recv_task = asyncio.create_task(_recv_loop())
            shutdown_task = asyncio.create_task(shutdown.wait())
            try:
                done, pending = await asyncio.wait(
                    [recv_task, shutdown_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                if shutdown_task in done:
                    return
                # recv_task completed → connection closed or exception
                if recv_task in done and not recv_task.cancelled():
                    exc = recv_task.exception()
                    if exc:
                        raise exc
            finally:
                recv_task.cancel()
                shutdown_task.cancel()

    async def _handle_message(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return

        # Subscription notifications arrive as:
        # {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x...","result":{log}}}
        if msg.get("method") != "eth_subscription":
            return
        params = msg.get("params", {})
        if not isinstance(params, dict):
            return
        log = params.get("result")
        if not isinstance(log, dict):
            return

        decoded = _decode_order_filled(log)
        if decoded is None:
            return

        # Only care about BUY side from followed wallets.
        if decoded["side"] != 0:
            return
        maker = decoded["maker"]
        if maker not in self._followed:
            return

        # Dedup by tx hash (bounded at 2048 entries).
        tx = decoded["tx_hash"]
        if tx in self._seen:
            return
        self._seen.add(tx)
        self._seen_order.append(tx)
        if len(self._seen_order) > 2048:
            evict = self._seen_order.pop(0)
            self._seen.discard(evict)

        # Resolve token_id → conditionId asynchronously.
        token_id = decoded["token_id"]
        market_id = await self._market_cache.get_market_id(token_id)
        if not market_id:
            logger.debug(
                "polygon_log_subscriber: no market_id for token %s…, skipping",
                token_id[:16],
            )
            return

        now_ts = int(time.time())
        payload = {
            "wallet": maker,
            "trade_id": tx,
            "token_id": token_id,
            "market_id": market_id,
            "side": "BUY",
            "price": decoded["price"],
            "size": decoded["size_shares"],
            "ts": now_ts,
            "title": "",
            "slug": "",
            "end_date_iso": "",
            "_source": "polygon_log",   # diagnostic tag, ignored by consumers
        }
        await self._bus.publish(EVT_WALLET_FILL, payload)
        self._stats["fills_emitted"] += 1
        logger.info(
            "polygon_log_subscriber: EVT_WALLET_FILL emitted — "
            "wallet=%s token=%s… price=%.4f size=%.4f tx=%s…",
            maker, token_id[:12], decoded["price"], decoded["size_shares"], tx[:12],
        )


__all__ = ["PolygonLogSubscriber"]
