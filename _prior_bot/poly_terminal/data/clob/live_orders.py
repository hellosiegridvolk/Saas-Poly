"""Async wrapper around py-clob-client-v2 for order signing + submission.

py-clob-client-v2 is the official Python SDK for Polymarket's V2 CTF
Exchange (post-2026-04-28 migration). It is sync; we wrap calls in
`asyncio.to_thread` so the ExecutionAgent's async event handlers
don't block the bus loop on network I/O. The same ClobClient instance
handles both LIVE_DRY (sign only) and LIVE (sign + post).

Mode contract:
- sign_only(...) → returns the signed order. Used by LIVE_DRY to
  exercise the EIP-712 signature path with no money at risk.
- sign_and_submit(...) → signs and POSTs. Used by LIVE.

Failures bubble up as exceptions so the caller can mark the
live_orders row 'rejected' and log appropriately.

V2 migration notes:
- The V2 SDK ships V2 contract addresses (`exchange_v2`,
  `neg_risk_exchange_v2`) and pUSD as the trading collateral
  (`0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`). No monkey-patch
  needed.
- `OrderArgsV2` adds `expiration`, `builder_code`, `metadata` on top
  of V1's `OrderArgs`. Defaults (0 / zero-bytes32 / zero-bytes32) are
  safe for our standard GTC flow.
- `OrderType.GTC` is a plain `str`, so json.dumps no longer trips on
  enum serialization (the bug that killed the first canary).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# 2026-05-15 PHASE 41.7 — operator-approved hardening of the boot
# cancel-all. Polymarket's CLOB can return HTTP 425 "service not
# ready" when the bot calls /cancel-all immediately on start, before
# the venue is warm. Pre-hardening this swallowed the error and
# returned 0 — harmless in PAPER (no real GTC orders), but in LIVE it
# would skip clearing orphan GTC orders that hold real collateral.
# Retry the 425 transient a bounded number of times with exponential
# backoff; any OTHER error keeps the original fail-soft behavior (no
# retry, no masking of non-transient failures).
_CANCEL_ALL_MAX_ATTEMPTS = 4
_CANCEL_ALL_BASE_DELAY_S = 1.0


@dataclass
class LiveOrderResult:
    signed_order_json: str           # serialized SignedOrder for audit
    submitted: bool                  # True iff post_order was attempted
    response: dict[str, Any] | None  # Polymarket POST /order response (LIVE only)


class LiveOrderClient:
    """Holds a fully-authenticated V2 ClobClient (L1 + L2)."""

    def __init__(
        self,
        host: str,
        private_key: str,
        funder_address: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        chain_id: int = 137,
        signature_type: int = 1,
    ) -> None:
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds

        self._client = ClobClient(
            host=host,
            chain_id=chain_id,
            key=private_key,
            signature_type=signature_type,
            funder=funder_address,
            creds=ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            ),
        )

    async def sign_only(
        self, *, token_id: str, price: float, size: float, side: str
    ) -> LiveOrderResult:
        """LIVE_DRY: build + sign an order without submitting it."""
        signed = await asyncio.to_thread(self._build_signed, token_id, price, size, side)
        return LiveOrderResult(
            signed_order_json=_serialize_signed_order(signed),
            submitted=False,
            response=None,
        )

    async def sign_and_submit(
        self,
        *,
        token_id: str,
        price: float,
        size: float,
        side: str,
        order_type: str = "GTC",
    ) -> LiveOrderResult:
        """LIVE: build + sign + POST. Raises on submission failure.

        order_type is a STRING ("GTC" / "FOK" / "GTD" / "FAK"). In
        py-clob-client-v2 OrderType members ARE plain strs so this is
        no longer a serialization landmine, but we keep the str
        contract for forward-compat with any future encoder change.
        """
        signed = await asyncio.to_thread(self._build_signed, token_id, price, size, side)
        response = await asyncio.to_thread(self._client.post_order, signed, order_type)
        return LiveOrderResult(
            signed_order_json=_serialize_signed_order(signed),
            submitted=True,
            response=response if isinstance(response, dict) else {"raw": str(response)},
        )

    def get_best_ask(self, token_id: str) -> float | None:
        """Best (lowest) SELL offer on the book — i.e. the price a
        marketable BUY would actually fill at. Returns None if the
        book is empty or fetch fails (caller treats as "no liquidity,
        skip"). Sync — wrap in to_thread on the caller side.

        py-clob-client-v2's `get_price` returns a dict shaped like
        `{"price": "0.55"}` (NOT a bare string — a 2026-05-02 LIVE
        canary spent 40 minutes hitting `float(dict)` TypeErrors and
        falling back to the original limit on every BUY).
        """
        return self._extract_price(token_id, "SELL", "get_best_ask")

    def get_best_bid(self, token_id: str) -> float | None:
        """Best (highest) BUY offer on the book — i.e. the price a
        marketable SELL would actually fill at. Used by the shadow-
        execution fallback in ExitAgent.bar_watcher when a position
        closes without ticks: instead of falling back to entry_price
        (which produces $0 realized PnL on every close — see
        2026-05-04 audit of the LIVE_DRY simulation gap), query the
        live orderbook for the price we'd actually receive.
        """
        return self._extract_price(token_id, "BUY", "get_best_bid")

    async def cancel_all_orders(self) -> int:
        """Cancel ALL of the bot's open orders on Polymarket. Returns
        the number reportedly cancelled (0 on error / nothing to
        cancel).

        2026-05-08 PHASE 29(b) — added so the operator can purge
        orphan GTC orders at boot. v46 saw `patient SELL GTC submit
        failed` 3× because Polymarket V2 reserves share collateral
        on resting GTC orders — a previous patient GTC that didn't
        cleanly cancel was holding 233.82M of 302M available shares,
        blocking new submissions. Calling this at boot reclaims the
        collateral.

        Defensive: any SDK error returns 0 instead of raising. The
        caller decides whether absence-of-cancel is fatal.
        """
        response: Any = None
        for attempt in range(1, _CANCEL_ALL_MAX_ATTEMPTS + 1):
            try:
                response = await asyncio.to_thread(self._client.cancel_all)
                break
            except Exception as exc:  # noqa: BLE001 — SDK raises bare Exception
                msg = str(exc).lower()
                transient = "425" in msg or "service not ready" in msg
                if not transient:
                    # Non-transient: preserve original fail-soft
                    # behavior — do NOT retry, do NOT mask it.
                    logger.warning(
                        "live_orders: cancel_all_orders raised — orphan "
                        "GTC orders may still be holding collateral",
                        exc_info=True,
                    )
                    return 0
                if attempt >= _CANCEL_ALL_MAX_ATTEMPTS:
                    # Persistent 425 after all retries — fail loud.
                    # Same return contract: caller treats 0 as
                    # "couldn't cancel" and warns about collateral.
                    logger.warning(
                        "live_orders: cancel_all still HTTP 425 "
                        "'service not ready' after %d attempts — orphan "
                        "GTC orders may still be holding collateral",
                        _CANCEL_ALL_MAX_ATTEMPTS,
                        exc_info=True,
                    )
                    return 0
                delay = _CANCEL_ALL_BASE_DELAY_S * (2 ** (attempt - 1))
                logger.warning(
                    "live_orders: cancel_all HTTP 425 'service not "
                    "ready' (attempt %d/%d) — retrying in %.1fs",
                    attempt,
                    _CANCEL_ALL_MAX_ATTEMPTS,
                    delay,
                )
                await asyncio.sleep(delay)
        # py-clob-client-v2 returns the number of cancelled orders, or
        # a list, depending on version. Accept either.
        if isinstance(response, int):
            return response
        if isinstance(response, list):
            return len(response)
        if isinstance(response, dict):
            cancelled = response.get("cancelled") or response.get("count")
            if isinstance(cancelled, int):
                return cancelled
            if isinstance(cancelled, list):
                return len(cancelled)
        return 0

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a resting GTC order on the CLOB.

        2026-05-08 PHASE 28 — added for "patient SELL" mode that
        places a GTC order at a scalp price, polls for fill, and
        cancels it after the wait window expires. Returns True if
        the SDK accepted the cancel; False on any error (callers
        should fall through to the FAK fast-path).

        py-clob-client-v2's `cancel_order` returns whatever the API
        responds; callers don't need to inspect — non-exception is
        treated as success.
        """
        if not order_id:
            return False
        try:
            await asyncio.to_thread(self._client.cancel_order, order_id)
            return True
        except Exception:
            logger.warning(
                "live_orders: cancel_order failed for %s",
                order_id, exc_info=True,
            )
            return False

    async def get_order_status(self, order_id: str) -> dict | None:
        """Fetch a single order's current state from the CLOB.

        2026-05-08 PHASE 28 — used by the patient-SELL polling loop
        to decide whether the GTC order has filled. Returns the SDK
        response dict (typically containing `status`, `filled_qty`,
        `avg_fill_price`) or None on any error / not-found. Caller
        treats None as "still waiting" rather than "definitely not
        filled" so a transient API blip doesn't trigger premature
        cancel.
        """
        if not order_id:
            return None
        try:
            response = await asyncio.to_thread(
                self._client.get_order, order_id,
            )
        except Exception:
            logger.warning(
                "live_orders: get_order_status failed for %s",
                order_id, exc_info=True,
            )
            return None
        if response is None or not isinstance(response, dict):
            return None
        return response

    def get_last_trade_price(self, token_id: str) -> float | None:
        """Most recent traded price for this token — the canonical
        "fair" price that the WS `last_trade_price` event publishes.
        Best source for TP/SL evaluation tick prices: it tracks
        actual market consensus rather than the bid/ask which can
        diverge by the full spread on illiquid markets.

        Returns None on 404 (no trades yet) or any SDK error so the
        caller can fall back to mid or skip the tick. Sync — wrap in
        asyncio.to_thread on the caller side. 2026-05-05 — added for
        the REST TickPoller fallback.
        """
        try:
            response = self._client.get_last_trade_price(token_id)
        except Exception:
            logger.warning(
                "live_orders: get_last_trade_price failed for token %s",
                token_id, exc_info=True,
            )
            return None
        if response is None:
            return None
        # SDK returns a dict shaped like {"price": "0.55"} — same shape
        # as get_price. Parse defensively.
        if isinstance(response, dict):
            price_str = response.get("price") or response.get("Price")
            if not price_str:
                return None
            try:
                return float(price_str)
            except (TypeError, ValueError):
                return None
        try:
            return float(response)
        except (TypeError, ValueError):
            return None

    def _extract_price(
        self, token_id: str, side: str, log_label: str
    ) -> float | None:
        """Shared dict-vs-string parser for SDK get_price responses."""
        try:
            response = self._client.get_price(token_id, side)
            if response is None:
                return None
            if isinstance(response, dict):
                price_str = response.get("price") or response.get("Price")
                if not price_str:
                    return None
                return float(price_str)
            return float(response)
        except Exception:
            logger.warning(
                "live_orders: %s failed for token %s",
                log_label, token_id, exc_info=True,
            )
            return None

    def _build_signed(
        self, token_id: str, price: float, size: float, side: str
    ) -> Any:
        from py_clob_client_v2.clob_types import OrderArgsV2
        from py_clob_client_v2.order_builder.helpers import (
            decimal_places, round_down, round_normal,
        )

        # Polymarket binary outcomes clamp price to [0.01, 0.99].
        raw_price = max(0.01, min(0.99, float(price)))

        # Fetch the market's tick size and pre-round our price to it.
        # The V2 SDK does this internally via `round_normal(price,
        # round_config.price)` — without doing it ourselves, the SDK
        # can shave the price (e.g. 0.0945 → 0.09 for tick=0.01) and
        # shrink maker_amount below the $1 marketable-BUY floor even
        # though our own pre-submit math said we were over.
        # Failed real-world example: price=0.0945, shares=11 →
        # our maker=$1.04, but SDK rounds to 0.09 → maker=$0.99 →
        # server rejects with "min size: $1".
        # get_tick_size is cached by the SDK after the first call.
        try:
            tick_str = self._client.get_tick_size(token_id)
            tick_dp = decimal_places(float(tick_str))
        except Exception:
            # Defensive fallback. 2dp covers 0.01-tick markets which
            # are by far the most common; sub-cent ticks remain a
            # tail risk we'd rather miss than trade incorrectly.
            tick_dp = 2

        clamped_price = round_normal(raw_price, tick_dp)
        clamped_price = max(0.01, min(0.99, clamped_price))

        # Side-aware shares precision (2026-05-02 fix):
        #
        # V2's marketable BUY enforces maker_amount ≤ 2 decimals
        # ("the market buy orders maker amount supports a max accuracy
        # of 2 decimals"). maker = price × shares, so to keep maker
        # ≤ 2dp we need shares precision = max(0, 2 - price_dp).
        # Earlier the code used s_dp=2 for both sides, which produced
        # 4dp maker (e.g. 11.11 × 0.55 = 6.1105) and the V2 server
        # rejected every marketable BUY that wasn't already integer-
        # share-aligned.
        #
        # Sub-cent markets (tick_dp ≥ 3) CANNOT satisfy this even with
        # integer shares: integer × 3dp = 3dp maker. The marketable
        # BUY path is unreachable for those tokens — skip with a
        # specific error so the operator sees these are unsupported
        # rather than a generic "rejected". SELL exits on existing
        # inventory still work because SELLs don't have the
        # marketable-maker constraint.
        #
        # SELLs rest on the book and don't have the marketable maker
        # constraint — V2's RoundConfig allows 2dp shares. Keep
        # s_dp=2 for SELLs so we don't over-round inventory we're
        # trying to liquidate.
        if side.upper() == "BUY":
            if tick_dp > 2:
                logger.warning(
                    "live_orders: cannot place marketable BUY on "
                    "sub-cent market (tick_dp=%d, token=%s) — V2's "
                    "2dp marketable maker constraint is unreachable; "
                    "skipping order",
                    tick_dp, token_id,
                )
                raise ValueError(
                    f"sub-cent tick_dp={tick_dp} not supported for "
                    f"marketable BUYs"
                )
            s_dp = max(0, 2 - tick_dp)
        else:
            s_dp = 2
        step = 10.0 ** -s_dp if s_dp > 0 else 1.0
        rounded_size = round_down(float(size), s_dp)

        # V2 marketable BUY: maker_amount must be ≥ $1. Closed-form
        # upsize — find the smallest s_dp-aligned share count whose
        # maker clears $1, then take max with our requested size.
        # O(1) instead of the old bump-loop which couldn't recover
        # when raw size was far below the floor.
        if side.upper() == "BUY":
            scale = 10 ** s_dp
            needed_size = math.ceil(scale / clamped_price) / scale
            if rounded_size < needed_size:
                rounded_size = needed_size
            if rounded_size * clamped_price < 1.00 - 1e-9:
                # Should be unreachable post-ceil but stay defensive.
                logger.warning(
                    "live_orders: maker still below $1 after upsize "
                    "(price=%s size=%s); skipping order",
                    clamped_price, rounded_size,
                )
                raise ValueError(
                    f"unreachable maker_min at price {clamped_price}"
                )

        if rounded_size < 5:
            logger.warning(
                "live_orders: rounded shares %.4f below V2 5-share "
                "min after precision-fit (price=%s, tick_dp=%d, "
                "raw=%s); skipping order",
                rounded_size, clamped_price, tick_dp, size,
            )
            raise ValueError(
                f"shares {rounded_size} below V2 min 5 after rounding"
            )

        args = OrderArgsV2(
            token_id=token_id,
            price=clamped_price,
            size=rounded_size,
            side=side,
        )
        return self._client.create_order(args)


def _serialize_signed_order(signed: Any) -> str:
    """Best-effort JSON dump of a py-clob-client SignedOrder.

    The SignedOrder shape varies by version; we try `.dict()`, `__dict__`,
    then fall back to repr. Whatever lands in the column is for human
    audit, not parsing.
    """
    try:
        if hasattr(signed, "dict"):
            return json.dumps(signed.dict(), default=str, sort_keys=True)
        if hasattr(signed, "__dict__"):
            return json.dumps(signed.__dict__, default=str, sort_keys=True)
    except Exception:
        logger.exception("live_orders: failed to serialize SignedOrder; using repr")
    return json.dumps({"repr": repr(signed)})
