"""Standalone CLI: probe Falcon for backtest data viability.

Runs three due-diligence tests against the configured Falcon agents and
emits a clear PASS/FAIL line for each. The most important is Test 3
(historical orderbook): if Falcon does not expose L2 depth, execution-grade
backtesting via Falcon is NOT viable and the explicit FAIL message tells
the operator to build the Polymarket WS recorder instead.

Exit codes:
    0 → all three tests passed
    1 → at least one test failed (or errored)
    2 → FALCON_TOKEN missing
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

from poly_terminal.research.falcon_client import (
    FalconAPIError,
    FalconAuthError,
    FalconClient,
)


def _agent_id_from_env(name: str, default: int | None = None) -> int | None:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


async def _test_markets(client: FalconClient) -> tuple[bool, str]:
    agent_id = _agent_id_from_env("FALCON_MARKETS_AGENT_ID", default=574)
    if agent_id is None:
        return False, "Test 1 markets: FAIL — agent_id unresolved"
    # 2026-05-04 fix: Falcon's params must be string-typed (per the
    # docs sample: `"closed": "True"`, `"min_volume": "100"`). Sending
    # native bool/int triggers a 400 INVALID_INPUT from the SQL
    # post-processor. Try the documented shape first, fall back to
    # the bool form on error so we still surface useful diagnostics.
    params_attempts: list[dict[str, Any]] = [
        {"closed": "True", "min_volume": "100"},
        {"closed": True},  # legacy/native, in case the API was updated
    ]
    last_err: str | None = None
    rows: list[dict[str, Any]] = []
    for params in params_attempts:
        try:
            rows = await client.query(agent_id=agent_id, params=params, limit=5)
            last_err = None
            break
        except (FalconAPIError, FalconAuthError) as exc:
            last_err = f"{exc} (params={params})"
            continue
    if last_err is not None and not rows:
        return False, f"Test 1 markets: FAIL — {last_err}"

    if not rows:
        return False, "Test 1 markets: FAIL — empty response"

    sample = rows[0]
    has_slug = bool(sample.get("market_slug") or sample.get("slug"))
    has_question = bool(sample.get("question"))
    has_closed = "closed" in sample
    if has_slug and has_question and has_closed:
        return True, (
            "Test 1 markets: PASS — slug + question + closed present "
            f"(n={len(rows)})"
        )
    missing = [
        n
        for n, v in (("market_slug", has_slug), ("question", has_question), ("closed", has_closed))
        if not v
    ]
    return False, f"Test 1 markets: FAIL — missing fields {missing}"


async def _test_trades(client: FalconClient) -> tuple[bool, str]:
    agent_id = _agent_id_from_env("FALCON_TRADES_AGENT_ID")
    if agent_id is None:
        return False, (
            "Test 2 trades: FAIL — FALCON_TRADES_AGENT_ID env var not set; "
            "look up the agent id at https://narrative.agent.heisenberg.so"
        )
    try:
        rows = await client.query(agent_id=agent_id, params={}, limit=5)
    except (FalconAPIError, FalconAuthError) as exc:
        return False, f"Test 2 trades: FAIL — {exc}"

    if not rows:
        return False, "Test 2 trades: FAIL — empty response"

    sample = rows[0]
    has_id = "id" in sample or "trade_id" in sample
    has_price = "price" in sample
    has_size = "size" in sample or "shares" in sample
    has_ts = "ts" in sample or "timestamp" in sample
    if has_id and has_price and has_size and has_ts:
        return True, (
            "Test 2 trades: PASS — id + price + size + ts present "
            f"(n={len(rows)})"
        )
    missing = [
        n
        for n, v in (("id", has_id), ("price", has_price), ("size", has_size), ("ts", has_ts))
        if not v
    ]
    return False, f"Test 2 trades: FAIL — missing fields {missing}"


async def _test_orderbook(client: FalconClient) -> tuple[bool, str]:
    agent_id = _agent_id_from_env("FALCON_ORDERBOOK_AGENT_ID")
    if agent_id is None:
        return False, (
            "Test 3 orderbook: FAIL — FALCON_ORDERBOOK_AGENT_ID env var not set; "
            "look up the agent id at https://narrative.agent.heisenberg.so"
        )
    # Falcon agent 572 (orderbook) requires token_id + a time range.
    # Pull a real token_id from a closed market via agent 574 to avoid
    # hardcoding a literal in the probe. Per Falcon docs, params should
    # be string-typed.
    markets_agent = _agent_id_from_env("FALCON_MARKETS_AGENT_ID", default=574) or 574
    try:
        seed = await client.query(
            agent_id=markets_agent,
            params={"closed": "True", "min_volume": "100"},
            limit=3,
        )
    except (FalconAPIError, FalconAuthError) as exc:
        return False, f"Test 3 orderbook: FAIL — could not seed token_id ({exc})"
    token_id: str | None = None
    market_end_ts: int | None = None
    for m in seed:
        # Falcon market rows expose `side_a_token_id` / `side_b_token_id`
        # (binary outcomes). Also handle `clob_token_ids` (list) for
        # forward-compat.
        for key in ("side_a_token_id", "side_b_token_id", "clob_token_ids", "token_ids", "tokens"):
            v = m.get(key)
            if isinstance(v, list) and v:
                token_id = str(v[0])
                break
            if isinstance(v, str) and v:
                token_id = v.split(",")[0].strip()
                break
        if token_id:
            # Pick a timestamp inside the market's life window: 1h before end_date.
            from datetime import datetime
            end = m.get("end_date") or m.get("end_ts")
            if isinstance(end, str):
                try:
                    market_end_ts = int(datetime.fromisoformat(end.replace("Z","+00:00")).timestamp())
                except (ValueError, TypeError):
                    market_end_ts = None
            elif isinstance(end, (int, float)):
                market_end_ts = int(end)
            break
    if not token_id:
        return False, (
            "Test 3 orderbook: FAIL — could not extract token_id from "
            "any closed-market row to seed the orderbook query"
        )
    if market_end_ts is None:
        # Fall back: 7 days ago.
        import time as _time
        market_end_ts = int(_time.time()) - 7 * 86400
    start_ts = market_end_ts - 3600   # 1h before end
    end_ts = market_end_ts            # at end
    # Try the documented shape; fall back through common variations
    # if the first attempt errors with INVALID_INPUT.
    params_attempts = [
        # 2026-05-04: API error revealed required params are `start_time`/`end_time`
        {"token_id": token_id, "start_time": str(start_ts), "end_time": str(end_ts)},
        {"token_id": token_id, "start_ts": str(start_ts), "end_ts": str(end_ts)},
        {"token_id": token_id, "from_ts": str(start_ts), "to_ts": str(end_ts)},
        {"token_id": token_id, "timestamp": str(market_end_ts)},
    ]
    last_err: str | None = None
    rows: list[dict[str, Any]] = []
    for params in params_attempts:
        try:
            rows = await client.query(agent_id=agent_id, params=params, limit=5)
            last_err = None
            break
        except (FalconAPIError, FalconAuthError) as exc:
            last_err = f"{exc} (params keys={list(params.keys())})"
            continue
    if last_err is not None and not rows:
        return False, f"Test 3 orderbook: FAIL — {last_err}"

    if not rows:
        return False, "Test 3 orderbook: FAIL — empty response"

    sample = rows[0]
    has_bids_ladder = isinstance(sample.get("bids"), list)
    has_asks_ladder = isinstance(sample.get("asks"), list)
    if has_bids_ladder and has_asks_ladder:
        return True, (
            "Test 3 orderbook: PASS — historical L2 ladders present "
            f"(n={len(rows)})"
        )

    # Soft snapshot? best_bid/best_ask without depth.
    has_best = "best_bid" in sample or "best_ask" in sample
    if has_best:
        return False, (
            "Test 3 orderbook: FAIL — Falcon does not appear to expose "
            "historical L2 orderbook depth — execution-grade backtesting "
            "via Falcon is NOT viable; build out the official Polymarket "
            "WS recorder instead."
        )

    return False, (
        "Test 3 orderbook: FAIL — neither L2 ladders nor best_bid/best_ask "
        "fields present in response — agent shape unknown."
    )


async def _run() -> int:
    if not os.environ.get("FALCON_TOKEN"):
        print("FALCON_TOKEN not set — cannot probe Falcon.", file=sys.stderr)
        return 2

    try:
        async with FalconClient() as client:
            results: list[tuple[bool, str]] = [
                await _test_markets(client),
                await _test_trades(client),
                await _test_orderbook(client),
            ]
    except FalconAuthError as exc:
        print(f"FALCON_TOKEN rejected: {exc}", file=sys.stderr)
        return 2

    all_passed = True
    for passed, msg in results:
        print(msg)
        all_passed = all_passed and passed

    return 0 if all_passed else 1


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    sys.exit(main())
