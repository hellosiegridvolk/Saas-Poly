"""Live-readiness validator — pre-canary checklist.

2026-05-05 — implements the deep-research checklist for "is the bot
ready to fire a tiny live canary?". Validates every gate that CAN be
checked from outside the bot process, and explicitly flags the gates
that REQUIRE additional infrastructure (e.g. CLOSE_ONLY mode +
canary controller) as GAP.

Output is a formatted table per gate plus a single-line verdict:
  GREEN   — every checkable gate passes; safe to run canary
  YELLOW  — gates pass but implementation gaps remain (CLOSE_ONLY mode etc)
  RED     — at least one hard fail; do NOT canary

Usage:

    poly-live-readiness                    # human-readable table
    poly-live-readiness --json             # machine-readable
    poly-live-readiness --strict           # treat YELLOW as RED (no
                                            # canary until all gaps closed)

Environment:
    DB_PATH           sqlite path (default: exports/state.db)
    POLY_PRIVATE_KEY  required for L2 verification
    POLY_PROXY_ADDRESS required
    POLY_API_*        from .env
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Status enum for each gate.
PASS = "PASS"
FAIL = "FAIL"
GAP = "GAP"      # check requires infra not built yet
WARN = "WARN"    # advisory only


@dataclass
class GateResult:
    gate: str
    status: str       # PASS|FAIL|GAP|WARN
    detail: str
    blocking: bool = True   # FAIL on a blocking gate → RED verdict

    @property
    def is_fail(self) -> bool:
        return self.status == FAIL

    @property
    def is_gap(self) -> bool:
        return self.status == GAP


def _read_env_file(path: Path) -> dict[str, str]:
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


# ── Gate implementations ─────────────────────────────────────────────


def gate_bot_mode_close_only(env: dict[str, str]) -> GateResult:
    """Two valid canary-launch modes:
      - CLOSE_ONLY: pre-canary preflight verification (no BUYs flow at
        all; the bot just exercises auth/balance/WS reads).
      - LIVE: actual canary launch. The CanaryControllerAgent flips
        the runtime mode override to CLOSE_ONLY after the first real
        LIVE fill, so the LIVE window is bounded to one position.

    Anything else (READ_ONLY, PAPER, LIVE_DRY) is wrong for a canary
    operation. 2026-05-05 — relaxed from CLOSE_ONLY-only to either
    after CanaryControllerAgent landed.
    """
    from poly_terminal.shared.enums import BotMode
    has_close_only = "CLOSE_ONLY" in [m.name for m in BotMode]
    if not has_close_only:
        return GateResult(
            "BOT_MODE=CLOSE_ONLY|LIVE", GAP,
            "BotMode enum lacks CLOSE_ONLY — needs new mode + gate-pipeline "
            "wiring to allow SELL but block BUY. ~1-2h work.",
        )
    current = env.get("BOT_MODE", "")
    if current in ("CLOSE_ONLY", "LIVE"):
        return GateResult(
            "BOT_MODE=CLOSE_ONLY|LIVE", PASS,
            f"set in env: {current}",
        )
    return GateResult(
        "BOT_MODE=CLOSE_ONLY|LIVE", FAIL,
        f"env BOT_MODE={current!r} — canary needs CLOSE_ONLY (preflight) "
        "or LIVE (actual canary; controller auto-flips to CLOSE_ONLY)",
    )


def gate_paper_mode_false(env: dict[str, str]) -> GateResult:
    pm = env.get("PAPER_MODE", "true").lower()
    if pm == "false":
        return GateResult("PAPER_MODE=false", PASS, "PAPER_MODE=false")
    return GateResult(
        "PAPER_MODE=false", FAIL,
        f"PAPER_MODE={pm!r} (live signing requires PAPER_MODE=false)",
    )


def gate_kill_switch_controllable(env: dict[str, str]) -> GateResult:
    """The kill-switch is file-based at exports/paused.flag.
    Controllable = the path is writeable and the mechanism is wired."""
    flag_path = Path(env.get("KILL_SWITCH_FLAG_PATH", "exports/paused.flag"))
    parent = flag_path.parent
    if not parent.exists():
        return GateResult(
            "kill_switch_controllable", FAIL,
            f"flag dir missing: {parent}",
        )
    if not os.access(parent, os.W_OK):
        return GateResult(
            "kill_switch_controllable", FAIL,
            f"flag dir not writeable: {parent}",
        )
    currently_paused = flag_path.exists()
    return GateResult(
        "kill_switch_controllable", PASS,
        f"flag={flag_path} writeable; currently_paused={currently_paused}",
        blocking=False,
    )


def gate_safety_safe(env: dict[str, str]) -> GateResult:
    """Safety=SAFE = kill switch NOT currently engaged (paused.flag absent)."""
    flag_path = Path(env.get("KILL_SWITCH_FLAG_PATH", "exports/paused.flag"))
    if flag_path.exists():
        return GateResult(
            "Safety=SAFE", FAIL,
            f"kill switch ENGAGED ({flag_path} present); rm to clear",
        )
    return GateResult("Safety=SAFE", PASS, "no paused.flag present")


def gate_l2_auth_works(env: dict[str, str]) -> GateResult:
    """Verify L2 creds against authed Polymarket endpoint."""
    host = env.get("CLOB_API_URL", "https://clob.polymarket.com")
    private_key = env.get("POLY_PRIVATE_KEY", "")
    funder = env.get("POLY_PROXY_ADDRESS", "")
    api_key = env.get("POLY_API_KEY", "")
    api_secret = env.get("POLY_API_SECRET", "")
    api_passphrase = env.get("POLY_API_PASSPHRASE", "")
    if not (private_key and funder and api_key and api_secret and api_passphrase):
        return GateResult("Auth=true", FAIL, "L1/L2 creds incomplete in env")
    try:
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds
        client = ClobClient(
            host=host, chain_id=137, key=private_key,
            signature_type=1, funder=funder,
            creds=ApiCreds(
                api_key=api_key, api_secret=api_secret,
                api_passphrase=api_passphrase,
            ),
        )
        resp = client.get_api_keys()
    except Exception as e:
        return GateResult(
            "Auth=true", FAIL,
            f"get_api_keys raised: {type(e).__name__}: {e}",
        )
    if not isinstance(resp, dict) or api_key not in resp.get("apiKeys", []):
        return GateResult(
            "Auth=true", FAIL,
            f"api_key {api_key} not in server-listed keys: {resp}",
        )
    return GateResult("Auth=true", PASS, f"L2 verified, key={api_key[:13]}...")


def gate_market_ws_connectable(env: dict[str, str]) -> GateResult:
    """Verify market WS connect can be established (don't subscribe).
    GAP: actual WS state requires the running bot's introspection."""
    base = env.get("CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com")
    return GateResult(
        "Market WS=connected", WARN,
        f"endpoint={base}/ws/market — actual connection state requires "
        "bot process introspection (not exposed via /api yet)",
        blocking=False,
    )


def gate_user_ws_connectable(env: dict[str, str]) -> GateResult:
    base = env.get("CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com")
    return GateResult(
        "User WS=connected", WARN,
        f"endpoint={base}/ws/user — actual connection state requires "
        "bot process introspection",
        blocking=False,
    )


def _fetch_usdc_balance(env: dict[str, str]) -> tuple[float, str]:
    """Read USDC balance via Polygon RPC. Returns (balance_usd, source_detail).

    Raises RuntimeError if all candidate RPC endpoints fail.

    Polymarket V2 uses pUSD on Polygon mainnet at
    0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB.

    Tries POLYGON_RPC_URL_PRIMARY then POLYGON_RPC_URL_FALLBACK then a
    small set of well-known public endpoints. Public RPCs block bare
    urllib without a User-Agent, so we send a realistic one.
    """
    funder = env.get("POLY_PROXY_ADDRESS", "")
    if not funder:
        raise RuntimeError("POLY_PROXY_ADDRESS unset")
    candidates: list[str] = []
    for k in ("POLYGON_RPC_URL_PRIMARY", "POLYGON_RPC_URL_FALLBACK"):
        v = env.get(k, "").strip()
        if v:
            candidates.append(v)
    candidates.extend([
        "https://polygon.drpc.org",
        "https://polygon.publicnode.com",
    ])

    import urllib.request
    addr_clean = funder.removeprefix("0x").rjust(64, "0")
    data = "0x70a08231" + addr_clean  # balanceOf(address)
    body = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{
            "to": "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb",
            "data": data,
        }, "latest"],
    }).encode()

    last_err: str | None = None
    for rpc in candidates:
        try:
            req = urllib.request.Request(
                rpc, data=body,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": (
                        "Mozilla/5.0 (poly-live-readiness/1.0) "
                        "PolymarketCanary/1.0"
                    ),
                },
            )
            with urllib.request.urlopen(req, timeout=8) as resp:
                j = json.loads(resp.read())
            if "result" not in j:
                last_err = f"{rpc}: RPC error: {j.get('error', j)}"
                continue
            balance_wei = int(j["result"], 16)
            return balance_wei / 1e6, f"rpc={rpc}"
        except Exception as e:
            last_err = f"{rpc}: {type(e).__name__}: {e}"
            continue
    raise RuntimeError(
        f"all RPC endpoints failed; last_err={last_err}"
    )


def gate_usdc_visible(env: dict[str, str]) -> GateResult:
    """USDC funder balance must be at least $5 (legacy minimum).

    See `gate_live_funder_floor` for the LIVE-mode-specific floor
    (default $20). Both gates use `_fetch_usdc_balance` for parity.
    """
    if not env.get("POLY_PROXY_ADDRESS", ""):
        return GateResult("USDC visible", FAIL, "POLY_PROXY_ADDRESS unset")
    try:
        balance, source = _fetch_usdc_balance(env)
    except RuntimeError as exc:
        return GateResult("USDC visible", FAIL, str(exc))
    if balance < 5.0:
        return GateResult(
            "USDC visible", FAIL,
            f"balance ${balance:.2f} < $5 minimum ({source})",
        )
    return GateResult(
        "USDC visible", PASS,
        f"balance ${balance:.2f} ({source})",
    )


# Default LIVE funder floor — see Phase 2 plan §6. $20 covers a single
# $5 canary plus a $5 hard cap and a ~$10 buffer for fees + chain race
# headroom over 4-6 cycles. Override via LIVE_FUNDER_FLOOR_USD.
_DEFAULT_LIVE_FUNDER_FLOOR_USD = 20.0


def gate_live_funder_floor(env: dict[str, str]) -> GateResult:
    """Refuse to start in BOT_MODE=LIVE if USDC funder is under-collateralized.

    Per v50-v55 post-mortem §6: the bot was halted with $4.27 USDC. A
    LIVE re-arm under that balance would have $5 BUYs rejected at order
    submit, but the failure surface is unpredictable (could leave stuck
    state). This gate is the deterministic stop.

    Skips entirely for non-LIVE modes (PAPER/READ_ONLY/LIVE_DRY) so
    operators can iterate freely without funding pressure.

    Threshold: env var `LIVE_FUNDER_FLOOR_USD` (default $20). Invalid
    values fall back to the default rather than open the gate.
    """
    mode = env.get("BOT_MODE", "").upper()
    if mode != "LIVE":
        return GateResult(
            "LIVE funder floor",
            PASS,
            f"skipped — BOT_MODE={mode or 'unset'} (gate only enforces under LIVE)",
        )

    raw = env.get("LIVE_FUNDER_FLOOR_USD", "").strip()
    try:
        floor = float(raw) if raw else _DEFAULT_LIVE_FUNDER_FLOOR_USD
    except ValueError:
        floor = _DEFAULT_LIVE_FUNDER_FLOOR_USD

    try:
        balance, source = _fetch_usdc_balance(env)
    except Exception as exc:  # noqa: BLE001 — refuse to LIVE on any read error
        return GateResult(
            "LIVE funder floor",
            FAIL,
            f"refusing to LIVE: cannot read USDC balance ({exc})",
        )

    if balance + 1e-9 < floor:
        return GateResult(
            "LIVE funder floor",
            FAIL,
            (
                f"refusing to LIVE: balance ${balance:.2f} < "
                f"floor ${floor:.2f} ({source}); top up funder before re-arm"
            ),
        )
    return GateResult(
        "LIVE funder floor",
        PASS,
        f"balance ${balance:.2f} >= floor ${floor:.2f} ({source})",
    )


def gate_db_integrity(env: dict[str, str]) -> GateResult:
    db_path = env.get("DB_PATH", "exports/state.db")
    if not Path(db_path).exists():
        return GateResult("DB integrity=ok", FAIL, f"db not found: {db_path}")
    try:
        c = sqlite3.connect(db_path)
        result = c.execute("PRAGMA integrity_check").fetchone()
        c.close()
    except Exception as e:
        return GateResult(
            "DB integrity=ok", FAIL,
            f"PRAGMA integrity_check raised: {type(e).__name__}: {e}",
        )
    if result and result[0] == "ok":
        return GateResult("DB integrity=ok", PASS, "PRAGMA integrity_check=ok")
    return GateResult(
        "DB integrity=ok", FAIL,
        f"integrity_check returned: {result}",
    )


def gate_live_open_orders_zero(env: dict[str, str]) -> GateResult:
    """Real LIVE open orders (not LIVE_DRY signed-only)."""
    db_path = env.get("DB_PATH", "exports/state.db")
    try:
        c = sqlite3.connect(db_path)
        n = c.execute(
            "SELECT COUNT(*) FROM live_orders "
            "WHERE mode='LIVE' AND status IN ('signed','live','submitted')"
        ).fetchone()[0]
        c.close()
    except Exception as e:
        return GateResult(
            "live_open_orders=0", FAIL,
            f"query raised: {type(e).__name__}: {e}",
        )
    if n == 0:
        return GateResult("live_open_orders=0", PASS, "0 live open orders")
    return GateResult(
        "live_open_orders=0", FAIL,
        f"{n} live open orders — investigate before canary",
    )


def gate_live_open_positions_zero(env: dict[str, str]) -> GateResult:
    """LIVE-mode positions (excluding LIVE_DRY simulation positions)."""
    db_path = env.get("DB_PATH", "exports/state.db")
    try:
        c = sqlite3.connect(db_path)
        n = c.execute(
            "SELECT COUNT(*) FROM positions p "
            "JOIN live_orders lo ON lo.intent_id = p.entry_intent_id "
            "WHERE p.closed_ts IS NULL AND lo.mode = 'LIVE'"
        ).fetchone()[0]
        # Imported on-chain positions are NOT bot LIVE positions but are
        # in the table — separately reportable but not blocking.
        n_imp = c.execute(
            "SELECT COUNT(*) FROM positions "
            "WHERE closed_ts IS NULL AND entry_intent_id LIKE 'imported%'"
        ).fetchone()[0]
        c.close()
    except Exception as e:
        return GateResult(
            "live_open_positions=0", FAIL,
            f"query raised: {type(e).__name__}: {e}",
        )
    if n == 0:
        return GateResult(
            "live_open_positions=0", PASS,
            f"0 LIVE-mode positions ({n_imp} imported on-chain — informational)",
        )
    return GateResult(
        "live_open_positions=0", FAIL,
        f"{n} LIVE-mode positions open — close before canary",
    )


def gate_live_fills_baseline(env: dict[str, str]) -> GateResult:
    """Record current LIVE fill count as baseline (for canary delta)."""
    db_path = env.get("DB_PATH", "exports/state.db")
    try:
        c = sqlite3.connect(db_path)
        n = c.execute(
            "SELECT COUNT(*) FROM live_orders "
            "WHERE mode='LIVE' AND filled_qty > 0"
        ).fetchone()[0]
        c.close()
    except Exception as e:
        return GateResult(
            "live_fills baseline", FAIL,
            f"query raised: {type(e).__name__}: {e}",
        )
    return GateResult(
        "live_fills baseline", PASS,
        f"recorded baseline: {n} live fills (canary should add 1)",
        blocking=False,
    )


def gate_tick_poller_enabled(env: dict[str, str]) -> GateResult:
    val = env.get("TICK_POLLER_ENABLED", "").strip().lower()
    if val in ("true", "1", "yes"):
        return GateResult("TickPoller_ENABLED=true", PASS, ".env=true")
    return GateResult(
        "TickPoller_ENABLED=true", FAIL,
        f".env={val!r} (must be true while WS bursty)",
    )


def gate_copy_bot_2_disabled(env: dict[str, str]) -> GateResult:
    """The deep-research checklist mentions copy_bot_2=DISABLED. No
    such config currently exists in the codebase. GAP."""
    if "copy_bot_2" in env or "COPY_BOT_2" in env:
        return GateResult(
            "copy_bot_2=DISABLED", FAIL,
            "copy_bot_2 env var present but unknown to bot",
        )
    return GateResult(
        "copy_bot_2=DISABLED", GAP,
        "copy_bot_2 not implemented in codebase — no-op",
        blocking=False,
    )


def gate_canary_caps(env: dict[str, str]) -> GateResult:
    """max_live_positions=1, max_live_exposure_usd=3 from checklist.
    Map to existing MAX_OPEN_POSITIONS, MAX_POSITION_USD."""
    max_pos = env.get("MAX_OPEN_POSITIONS", "")
    max_usd = env.get("MAX_POSITION_USD", "")
    issues = []
    if max_pos != "1":
        issues.append(f"MAX_OPEN_POSITIONS={max_pos!r} (need 1 for canary)")
    if max_usd not in ("3", "3.0", "3.00"):
        issues.append(f"MAX_POSITION_USD={max_usd!r} (need 3 for $3 canary)")
    if issues:
        return GateResult(
            "canary caps (max_pos=1, max_usd=$3)", FAIL,
            "; ".join(issues),
        )
    return GateResult(
        "canary caps (max_pos=1, max_usd=$3)", PASS,
        "MAX_OPEN_POSITIONS=1, MAX_POSITION_USD=3",
    )


def gate_canary_controller(env: dict[str, str]) -> GateResult:
    """CanaryControllerAgent (added 2026-05-05) auto-flips bot mode
    from LIVE → CLOSE_ONLY on first real LIVE fill. Verify the
    module imports + the wiring exists in main.py. The agent is only
    constructed at runtime when bot_mode == LIVE; checking module
    importability is the strongest static check we can do here."""
    try:
        from poly_terminal.agents.canary_controller.agent import (  # noqa: F401
            CanaryControllerAgent,
        )
    except ImportError as e:
        return GateResult(
            "canary controller active", FAIL,
            f"CanaryControllerAgent import failed: {e}",
        )
    # Confirm the wiring exists in main.py (literal pattern check —
    # doesn't fully prove it'll fire at runtime, but catches removal).
    try:
        from pathlib import Path
        main_src = Path(__file__).resolve().parents[1] / "main.py"
        text = main_src.read_text(encoding="utf-8")
        if "CanaryControllerAgent" not in text:
            return GateResult(
                "canary controller active", FAIL,
                "main.py does not reference CanaryControllerAgent",
            )
        if "_mode_override" not in text:
            return GateResult(
                "canary controller active", FAIL,
                "main.py lacks _mode_override layer",
            )
    except Exception as e:
        return GateResult(
            "canary controller active", FAIL,
            f"main.py inspection raised: {e}",
        )
    return GateResult(
        "canary controller active", PASS,
        "module imports + main.py wiring present (constructed at runtime "
        "when bot_mode==LIVE)",
    )


def gate_llm_strategies_paper(env: dict[str, str]) -> GateResult:
    """LLM/research strategies should run in PAPER mode. The codebase
    doesn't expose per-strategy mode overrides today — all strategies
    inherit BOT_MODE. GAP."""
    return GateResult(
        "LLM/research strategies=PAPER", GAP,
        "per-strategy mode override not implemented; all strategies "
        "currently inherit BOT_MODE",
        blocking=False,
    )


def gate_balance_no_errors(env: dict[str, str]) -> GateResult:
    """balance_stale=false / balance_error=false — currently the bot's
    monitor /api/status surfaces these but there's no standalone check."""
    return GateResult(
        "balance freshness", WARN,
        "monitor exposes balance_stale via /api/status but no standalone "
        "check; covered indirectly by gate_usdc_visible passing",
        blocking=False,
    )


def gate_positions_no_errors(env: dict[str, str]) -> GateResult:
    """positions_error=false — same pattern as balance."""
    return GateResult(
        "positions sync ok", WARN,
        "monitor exposes positions_error via /api/status; covered "
        "indirectly by db_integrity + open_positions queries",
        blocking=False,
    )


# ── Runner ───────────────────────────────────────────────────────────


GATES = [
    gate_db_integrity,
    gate_l2_auth_works,
    gate_kill_switch_controllable,
    gate_safety_safe,
    gate_paper_mode_false,
    gate_bot_mode_close_only,
    gate_usdc_visible,
    gate_live_funder_floor,
    gate_live_open_orders_zero,
    gate_live_open_positions_zero,
    gate_live_fills_baseline,
    gate_market_ws_connectable,
    gate_user_ws_connectable,
    gate_balance_no_errors,
    gate_positions_no_errors,
    gate_tick_poller_enabled,
    gate_copy_bot_2_disabled,
    gate_canary_caps,
    gate_canary_controller,
    gate_llm_strategies_paper,
]


def _verdict(results: list[GateResult], strict: bool = False) -> str:
    has_fail = any(r.is_fail and r.blocking for r in results)
    has_gap = any(r.is_gap and r.blocking for r in results)
    if has_fail:
        return "RED"
    if has_gap or (strict and any(r.is_gap for r in results)):
        return "YELLOW"
    return "GREEN"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0] if __doc__ else None,
    )
    parser.add_argument("--env-file", type=str, default=".env")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--strict", action="store_true",
                        help="Treat YELLOW (gaps) as blocking")
    args = parser.parse_args()

    env = dict(os.environ)
    file_env = _read_env_file(Path(args.env_file).expanduser().resolve())
    # CLI env wins; .env fills gaps
    for k, v in file_env.items():
        env.setdefault(k, v)

    results: list[GateResult] = []
    for gate in GATES:
        try:
            results.append(gate(env))
        except Exception as e:
            results.append(GateResult(
                gate.__name__, FAIL,
                f"check raised: {type(e).__name__}: {e}",
            ))

    verdict = _verdict(results, strict=args.strict)

    if args.json:
        print(json.dumps({
            "verdict": verdict,
            "ts": int(time.time()),
            "gates": [
                {"gate": r.gate, "status": r.status, "detail": r.detail,
                 "blocking": r.blocking}
                for r in results
            ],
        }, indent=2))
    else:
        print()
        print(f"{'gate':38s}  {'status':6s}  detail")
        print("-" * 100)
        for r in results:
            mark = {"PASS": "✓", "FAIL": "✗", "GAP": "○", "WARN": "!"}[r.status]
            print(f"  {mark} {r.gate:36s}  {r.status:6s}  {r.detail}")
        print("-" * 100)
        n_pass = sum(1 for r in results if r.status == PASS)
        n_fail = sum(1 for r in results if r.status == FAIL)
        n_gap = sum(1 for r in results if r.status == GAP)
        n_warn = sum(1 for r in results if r.status == WARN)
        print(
            f"  totals: PASS={n_pass}  FAIL={n_fail}  "
            f"GAP={n_gap}  WARN={n_warn}"
        )
        verdict_color = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}[verdict]
        print(f"\n  VERDICT: {verdict_color} {verdict}")
        if verdict == "RED":
            print("  → Blocking failures present. Do NOT run live canary.")
        elif verdict == "YELLOW":
            print("  → No fails, but GAP gates need infra work before canary.")
            print("    Each GAP describes the missing piece + estimated effort.")
        else:
            print("  → All checkable gates pass. Safe to proceed with canary plan.")

    if verdict == "RED":
        return 2
    if verdict == "YELLOW" and args.strict:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
