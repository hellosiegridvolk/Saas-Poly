"""One-shot LIVE-execution canary.

2026-05-05 — exercises the full LIVE order path (sign + POST + cancel)
against Polymarket without taking inventory or fills. Validates:

  - L1 EIP-712 signing (private_key)
  - L2 auth header generation (api_key/secret/passphrase)
  - py-clob-client-v2 `post_order`
  - py-clob-client-v2 `cancel_order`
  - SDK price/size rounding + V2 floor enforcement

Strategy: place a GTC BUY limit FAR below the current best bid (defaults
to $0.01) for a tiny size (defaults to 105 shares = $1.05 maker amount,
just over Polymarket's $1 V2 minimum). The order rests on the book and
will NOT fill unless the market crashes to $0.01 within the cancel
window — a multi-sigma event we'd want to know about anyway.

After post_order succeeds, the script immediately calls cancel_order on
the returned order id. If cancel fails the canary emits a warning so
the user can clean up via the dashboard.

Hard safety guards:
  - Refuses to submit if maker_amount > MAX_NOTIONAL_USD
  - Refuses to submit if best_bid is below 5× the canary price
    (the price gap must be wide enough to make a fill implausible)
  - Refuses to submit if any of POLY_PRIVATE_KEY / POLY_PROXY_ADDRESS
    are unset

Usage:

    poly-live-canary                            # auto-pick token
    poly-live-canary --token <token_id>         # explicit token
    poly-live-canary --price 0.01 --shares 105  # override defaults
    poly-live-canary --dry-run                  # build+sign, don't POST

Environment:
    DB_PATH         sqlite path (default: exports/state.db)
    LOG_LEVEL       DEBUG|INFO|WARNING|ERROR (default INFO)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

logger = logging.getLogger("poly_terminal.scripts.live_canary")

# Hard caps — refuse any combination that exceeds these.
MAX_NOTIONAL_USD: float = 2.0     # absolute ceiling on USDC at risk
MIN_PRICE_GAP_FACTOR: float = 5.0  # require best_bid >= price × this


def _setup_logging() -> None:
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _read_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        out[k.strip()] = v.split("#")[0].strip()
    return out


def _pick_token_from_db(db_path: str) -> str | None:
    """Default token = the most recently opened bot position. We know
    its market is active because the bot just traded into it."""
    try:
        c = sqlite3.connect(db_path)
        row = c.execute(
            "SELECT token_id FROM positions "
            "WHERE closed_ts IS NULL AND entry_intent_id NOT LIKE 'imported%' "
            "ORDER BY entry_ts DESC LIMIT 1"
        ).fetchone()
        c.close()
        return str(row[0]) if row else None
    except Exception:
        logger.exception("could not read DB for default token")
        return None


def _fetch_best_bid(client, token_id: str) -> float | None:
    """Use the SDK's get_price (BUY side). Returns best_bid or None."""
    try:
        resp = client.get_price(token_id, "BUY")
    except Exception as e:
        logger.warning("best_bid fetch raised: %s", type(e).__name__)
        return None
    if resp is None:
        return None
    if isinstance(resp, dict):
        v = resp.get("price") or resp.get("Price")
        try:
            return float(v) if v else None
        except (TypeError, ValueError):
            return None
    try:
        return float(resp)
    except (TypeError, ValueError):
        return None


def _summarize_response(resp: object) -> str:
    if not isinstance(resp, dict):
        return repr(resp)
    parts = []
    for k in ("success", "errorMsg", "orderID", "status", "transactionsHashes"):
        if k in resp:
            parts.append(f"{k}={resp[k]!r}")
    return " ".join(parts) or repr(resp)


async def _run_canary(args: argparse.Namespace) -> int:
    env_path = Path(args.env_file).expanduser().resolve()
    env = _read_env(env_path)

    private_key = env.get("POLY_PRIVATE_KEY", "")
    funder = env.get("POLY_PROXY_ADDRESS", "")
    api_key = env.get("POLY_API_KEY", "")
    api_secret = env.get("POLY_API_SECRET", "")
    api_passphrase = env.get("POLY_API_PASSPHRASE", "")
    host = env.get("CLOB_API_URL", "https://clob.polymarket.com")
    if not private_key or not funder:
        logger.error("POLY_PRIVATE_KEY / POLY_PROXY_ADDRESS missing in %s", env_path)
        return 2
    if not api_key or not api_secret or not api_passphrase:
        logger.error("L2 creds incomplete; run poly-sync-l2-creds first")
        return 2

    token_id = args.token or _pick_token_from_db(args.db)
    if not token_id:
        logger.error("no token to canary against — pass --token or have an open position")
        return 2

    price = float(args.price)
    shares = float(args.shares)
    notional = price * shares
    if notional > MAX_NOTIONAL_USD:
        logger.error(
            "notional $%.4f > hard cap $%.2f — refusing", notional, MAX_NOTIONAL_USD,
        )
        return 3

    # Build the SDK client with the SAME creds the bot uses.
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds, OrderPayload

    client = ClobClient(
        host=host, chain_id=137, key=private_key, signature_type=1,
        funder=funder,
        creds=ApiCreds(
            api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase,
        ),
    )

    # Quick L2 sanity check before risking anything.
    try:
        keys_resp = client.get_api_keys()
        listed = keys_resp.get("apiKeys", []) if isinstance(keys_resp, dict) else []
        if api_key not in listed:
            logger.error(
                "L2 sanity FAILED: api_key %s not in server keys %s",
                api_key, listed,
            )
            return 4
        logger.info("L2 sanity OK (key listed server-side)")
    except Exception:
        logger.exception("L2 sanity check raised — refusing to canary")
        return 4

    # Best-bid gap check. We require best_bid >= price × MIN_PRICE_GAP_FACTOR
    # so a fill within the cancel window is implausible.
    best_bid = _fetch_best_bid(client, token_id)
    if best_bid is None:
        logger.error("best_bid unavailable for %s — refusing", token_id)
        return 5
    logger.info(
        "best_bid=%.4f canary_price=%.4f gap_factor=%.1fx (need ≥%.1fx)",
        best_bid, price, best_bid / price if price > 0 else 0, MIN_PRICE_GAP_FACTOR,
    )
    if best_bid < price * MIN_PRICE_GAP_FACTOR:
        logger.error(
            "price gap too narrow (best_bid=%.4f vs canary=%.4f); refusing",
            best_bid, price,
        )
        return 5

    # Use the bot's own LiveOrderClient so we exercise its build_signed
    # path (clamping, rounding, V2 floor) — not just the raw SDK.
    from poly_terminal.data.clob.live_orders import LiveOrderClient

    bot_client = LiveOrderClient(
        host=host,
        private_key=private_key,
        funder_address=funder,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )

    print()
    print("=" * 70)
    print(f"  CANARY PLAN")
    print("=" * 70)
    print(f"  token:          {token_id}")
    print(f"  side:           BUY")
    print(f"  price (limit):  ${price:.4f}")
    print(f"  shares:         {shares:.0f}")
    print(f"  notional:       ${notional:.4f}")
    print(f"  best_bid (now): ${best_bid:.4f}")
    print(f"  fill risk:      market would need to crash {best_bid/price:.0f}x in <2s")
    print(f"  order_type:     GTC (rests on book; immediately cancelled)")
    print("=" * 70)
    print()

    if args.dry_run:
        logger.info("[dry-run] signing only, NOT submitting")
        result = await bot_client.sign_only(
            token_id=token_id, price=price, size=shares, side="BUY",
        )
        print(f"signed_order: {result.signed_order_json[:200]}...")
        print()
        print("✓ dry-run complete — no real order placed.")
        return 0

    # ─── REAL canary: sign + POST + cancel ───────────────────────────
    print(">>> firing canary in 2 seconds (Ctrl-C to abort)... <<<")
    try:
        await asyncio.sleep(2)
    except KeyboardInterrupt:
        print("aborted")
        return 1

    t0 = time.monotonic()
    try:
        result = await bot_client.sign_and_submit(
            token_id=token_id, price=price, size=shares,
            side="BUY", order_type="GTC",
        )
    except Exception as e:
        logger.exception("post_order RAISED: %s", type(e).__name__)
        return 6
    post_ms = (time.monotonic() - t0) * 1000.0

    print(f"\n[+{post_ms:.0f}ms] post_order returned:")
    print(f"  {_summarize_response(result.response)}")

    order_id = None
    if isinstance(result.response, dict):
        order_id = (
            result.response.get("orderID")
            or result.response.get("order_id")
            or result.response.get("orderId")
        )

    if not order_id:
        logger.error(
            "post_order did NOT return an order id (response: %r) — "
            "you may need to clean up manually",
            result.response,
        )
        return 7

    print(f"  order_id: {order_id}")
    print(f"\n[+{post_ms:.0f}ms] cancelling order...")

    t1 = time.monotonic()
    try:
        cancel_resp = client.cancel_order(OrderPayload(orderID=order_id))
    except Exception as e:
        logger.exception("cancel_order RAISED — order may still be on book")
        print(f"\n  ⚠  cancel FAILED ({type(e).__name__}: {e})")
        print(f"  ⚠  order_id={order_id} may still be live at price ${price:.4f}")
        print(f"  ⚠  manual cleanup: open Polymarket dashboard and cancel")
        return 8
    cancel_ms = (time.monotonic() - t1) * 1000.0

    print(f"\n[+{cancel_ms:.0f}ms] cancel_order returned:")
    print(f"  {_summarize_response(cancel_resp)}")

    print()
    print("=" * 70)
    print(f"  CANARY RESULTS")
    print("=" * 70)
    print(f"  post_order latency:   {post_ms:>7.0f} ms")
    print(f"  cancel_order latency: {cancel_ms:>7.0f} ms")
    print(f"  order_id:             {order_id}")
    print(f"  status:               ✓ exercised end-to-end without errors")
    print("=" * 70)
    return 0


async def _amain() -> int:
    _setup_logging()
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0] if __doc__ else None,
    )
    parser.add_argument("--env-file", type=str, default=".env")
    parser.add_argument("--db", type=str,
                        default=os.environ.get("DB_PATH", "exports/state.db"))
    parser.add_argument("--token", type=str, default=None,
                        help="explicit token_id (default: most recent open position)")
    parser.add_argument("--price", type=float, default=0.01,
                        help="canary limit price (default: 0.01)")
    parser.add_argument("--shares", type=float, default=105.0,
                        help="shares (default: 105 → $1.05 at $0.01)")
    parser.add_argument("--dry-run", action="store_true",
                        help="sign only, don't POST")
    args = parser.parse_args()
    return await _run_canary(args)


def main() -> int:
    return asyncio.run(_amain())


if __name__ == "__main__":
    sys.exit(main())
