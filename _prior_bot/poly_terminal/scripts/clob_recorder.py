"""Standalone CLI: record Polymarket CLOB orderbook snapshots into SQLite.

Connects to the public market WebSocket, subscribes to a configurable set
of token_ids, and persists every book snapshot into the
`research_orderbook_snapshots` table for downstream backtesting and
fill-simulation.

This binary is OFFLINE research infrastructure. It opens its own
`Database` handle (which auto-applies pending migrations including 0012),
its own `EventBus`, and its own `MarketWebSocket`. It does NOT touch the
live trading bot's process, bus, or in-memory state.

Usage:

    export RECORDER_TOKENS="<comma-separated token ids>"
    poly-clob-recorder

Or with a file:

    export RECORDER_TOKEN_FILE=/path/to/tokens.txt
    poly-clob-recorder

Environment:
    CLOB_WS_URL              ws endpoint (defaults to the public CLOB ws)
    DB_PATH                  sqlite path (default: exports/state.db)
    RECORDER_TOKENS          comma-separated token_ids
    RECORDER_TOKEN_FILE      newline-delimited token_id file (alternative)
    RECORDER_BUFFER_SIZE     auto-flush threshold (default 100)
    RECORDER_FLUSH_INTERVAL_S timer flush interval (default 5.0)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

import re
import sqlite3

from poly_terminal.persistence.db import Database
from poly_terminal.research.clob_recorder.recorder import ClobRecorder
from poly_terminal.research.clob_recorder.snapshot_repo import SnapshotRepo
from poly_terminal.research.clob_recorder.tick_repo import TickRepo

logger = logging.getLogger("poly_terminal.scripts.clob_recorder")

_DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_DEFAULT_DB_PATH = "exports/state.db"


def _resolve_ws_url() -> str:
    """Pick the WS URL from env, falling back to the live CLOB endpoint.

    The bot's `Settings.clob_ws_url` is the base host (no `/ws/market`
    suffix). If `CLOB_WS_URL` is set, we honor it as-is and append the
    market subpath only when the user gave us the bare host.
    """
    raw = os.environ.get("CLOB_WS_URL", "").strip()
    if not raw:
        return _DEFAULT_WS_URL
    if raw.endswith("/ws/market") or "/ws/" in raw:
        return raw
    return raw.rstrip("/") + "/ws/market"


def _read_token_file(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        return []
    out: list[str] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


def _resolve_tokens() -> list[str]:
    """Pull token ids from RECORDER_TOKENS, then RECORDER_TOKEN_FILE."""
    raw = os.environ.get("RECORDER_TOKENS", "").strip()
    tokens: list[str] = []
    if raw:
        tokens.extend(t.strip() for t in raw.split(",") if t.strip())

    file_path = os.environ.get("RECORDER_TOKEN_FILE", "").strip()
    if file_path:
        tokens.extend(_read_token_file(file_path))

    # De-dup while preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for t in tokens:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


def _resolve_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("invalid %s=%r — using default %d", name, raw, default)
        return default


def _resolve_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("invalid %s=%r — using default %.2f", name, raw, default)
        return default


def _setup_logging() -> None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


_CRYPTO_BAR_PATTERNS = [
    re.compile(r"-updown-(5m|15m|1h)-\d+", re.I),
    re.compile(r"-up-or-down-", re.I),
    re.compile(r"^(btc|eth|sol|bitcoin|ethereum|solana|xrp|dogecoin)-", re.I),
]


def _build_falcon_refresher(
    db_path: str,
) -> "Callable[[], Awaitable[set[str]]] | None":
    """If FALCON_TOKEN is set, return an async callable that fetches the
    CURRENT desired token set: every active crypto bar from Falcon agent
    574 + every open bot-position token from the live DB (so the
    recorder always covers what the bot is actively trading).

    Returns None when FALCON_TOKEN unset — refresh disabled, recorder
    keeps its static initial list.
    """
    from typing import Awaitable, Callable  # noqa: F401 — narrow return type
    if not os.environ.get("FALCON_TOKEN", "").strip():
        return None
    from poly_terminal.research.falcon_client import FalconClient

    async def _refresh() -> set[str]:
        tokens: set[str] = set()
        # Bot-managed open-position tokens — always include so the
        # recorder captures real bot activity.
        try:
            conn = sqlite3.connect(db_path)
            for row in conn.execute(
                "SELECT DISTINCT token_id FROM positions "
                "WHERE closed_ts IS NULL AND token_id != ''"
            ):
                if row[0]:
                    tokens.add(str(row[0]))
            conn.close()
        except Exception:
            logger.warning("falcon_refresh: DB read failed", exc_info=True)
        # Falcon: paginate active markets, filter to crypto-bar slugs.
        agent_id = _resolve_int_env("FALCON_MARKETS_AGENT_ID", 574)
        try:
            async with FalconClient() as client:
                offset = 0
                while offset < 2000:
                    rows = await client.query(
                        agent_id=agent_id,
                        params={"closed": "False", "min_volume": "10"},
                        limit=200, offset=offset,
                    )
                    if not rows:
                        break
                    for m in rows:
                        slug = str(m.get("slug", ""))
                        if not any(p.search(slug) for p in _CRYPTO_BAR_PATTERNS):
                            continue
                        for k in ("side_a_token_id", "side_b_token_id"):
                            tid = m.get(k)
                            if isinstance(tid, str) and tid:
                                tokens.add(tid)
                    offset += 200
        except Exception:
            logger.warning("falcon_refresh: Falcon fetch failed", exc_info=True)
        return tokens

    return _refresh


async def _main() -> int:
    _setup_logging()

    tokens = _resolve_tokens()
    if not tokens:
        logger.error(
            "no tokens configured — set RECORDER_TOKENS=<csv> or "
            "RECORDER_TOKEN_FILE=<path> and try again"
        )
        return 2

    ws_url = _resolve_ws_url()
    buffer_size = _resolve_int_env("RECORDER_BUFFER_SIZE", 100)
    flush_interval_s = _resolve_float_env("RECORDER_FLUSH_INTERVAL_S", 5.0)
    db_path = os.environ.get("DB_PATH", "").strip() or _DEFAULT_DB_PATH

    db = Database(db_path)
    applied = await db.initialize()
    logger.info(
        "clob_recorder db=%s migrations_applied=%d tokens=%d url=%s",
        db_path,
        applied,
        len(tokens),
        ws_url,
    )

    repo = SnapshotRepo(db)
    tick_repo = TickRepo(db)
    refresh_fn = _build_falcon_refresher(db_path)
    refresh_interval_s = _resolve_float_env("RECORDER_REFRESH_INTERVAL_S", 300.0)
    tick_buffer_size = _resolve_int_env("RECORDER_TICK_BUFFER_SIZE", buffer_size * 5)
    recorder = ClobRecorder(
        snapshot_repo=repo,
        market_ws_url=ws_url,
        token_ids=tokens,
        buffer_size=buffer_size,
        flush_interval_s=flush_interval_s,
        token_refresh_fn=refresh_fn,
        token_refresh_interval_s=refresh_interval_s,
        tick_repo=tick_repo,
        tick_buffer_size=tick_buffer_size,
    )
    if refresh_fn is None:
        logger.info(
            "clob_recorder token refresh disabled (FALCON_TOKEN unset) — "
            "using static token list"
        )

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(signum: int) -> None:
        logger.info("clob_recorder received signal %s — shutting down", signum)
        shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, int(sig))
        except (NotImplementedError, RuntimeError):
            # Not all platforms (e.g. Windows) support signal handlers
            # via add_signal_handler; fall back to the default behavior.
            pass

    try:
        await recorder.run(shutdown)
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("clob_recorder crashed")
        return 1

    logger.info(
        "clob_recorder stopped — snapshots: received=%d persisted=%d "
        "buf_hw=%d  ticks: received=%d persisted=%d buf_hw=%d  errors=%d",
        recorder.stats["snapshots_received"],
        recorder.stats["snapshots_persisted"],
        recorder.stats["buffer_high_water"],
        recorder.stats["ticks_received"],
        recorder.stats["ticks_persisted"],
        recorder.stats["tick_buffer_high_water"],
        recorder.stats["errors"],
    )
    return 0


def main() -> int:
    return asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(main())
