"""Daily ops check — runs every health/integrity/budget query and prints
a one-screen status report. Maps 1:1 to docs/06_DEPLOYMENT_CHECKLIST.md §C.

Exit codes:
  0  all checks pass
  1  one or more checks failed (caller should investigate; system stays up)
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from poly_terminal.config.fingerprint import compute_fingerprint
from poly_terminal.config.settings import Settings
from poly_terminal.persistence.db import Database
from poly_terminal.persistence.repositories.fills import FillsRepo
from poly_terminal.persistence.repositories.gate_metrics import GateMetricsRepo
from poly_terminal.persistence.repositories.wallets import WalletsRepo


def _ok(label: str, ok: bool, detail: str = "") -> dict[str, object]:
    return {"check": label, "ok": ok, "detail": detail}


async def run_checks(db: Database) -> tuple[bool, list[dict[str, object]]]:
    results: list[dict[str, object]] = []

    # 1. Integrity check.
    try:
        integrity = await db.integrity_check()
        results.append(_ok("db_integrity", integrity == "ok", integrity))
    except Exception as exc:
        results.append(_ok("db_integrity", False, str(exc)))

    # 2. signal_at / filled_at NULL count.
    async with db.connect() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM paper_fills "
            "WHERE signal_at IS NULL OR filled_at IS NULL"
        )
        row = await cur.fetchone()
        nulls = int(row[0]) if row else 0
    results.append(_ok("paper_fills_no_nulls", nulls == 0, f"nulls={nulls}"))

    # 3. Recent fill activity (within last 24h).
    cutoff = int(time.time()) - 86_400
    async with db.connect() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM paper_fills WHERE filled_at > ?",
            (cutoff,),
        )
        row = await cur.fetchone()
        fills_24h = int(row[0]) if row else 0
    results.append(
        _ok(
            "fills_in_last_24h",
            True,  # informational only
            f"{fills_24h} fills",
        )
    )

    # 4. SL hit rate (success-criterion gate from docs/00 §4).
    async with db.connect() as conn:
        cur = await conn.execute(
            """
            SELECT outcome, COUNT(*)
            FROM paper_fills
            WHERE filled_at > ? AND outcome IS NOT NULL
            GROUP BY outcome
            """,
            (cutoff,),
        )
        rows = await cur.fetchall()
    by_outcome = {str(r[0]): int(r[1]) for r in rows}
    total_exits = sum(by_outcome.values())
    sl_count = by_outcome.get("SL", 0)
    sl_rate = sl_count / total_exits if total_exits else 0.0
    results.append(
        _ok(
            "sl_rate_under_30pct",
            sl_rate <= 0.30,
            f"sl={sl_count}/{total_exits} ({sl_rate:.1%})",
        )
    )

    # 5. Gate metrics presence.
    repo = GateMetricsRepo(db)
    rows = await repo.fetch_today()
    gates_with_data = {r["gate_name"] for r in rows}
    results.append(
        _ok(
            "gate_metrics_recorded",
            len(gates_with_data) > 0,
            f"{len(gates_with_data)} gates with rows today",
        )
    )

    # 6. Top-decile wallets refreshed in last 2h.
    wallets_repo = WalletsRepo(db)
    top = await wallets_repo.fetch_top(limit=200)
    fresh_cutoff = int(time.time()) - 2 * 3600
    fresh = sum(1 for s in top if s.last_updated >= fresh_cutoff)
    results.append(
        _ok(
            "wallet_scores_fresh",
            len(top) == 0 or fresh / max(1, len(top)) >= 0.50,
            f"{fresh}/{len(top)} fresh in last 2h",
        )
    )

    # 7. Disk free.
    try:
        stat = os.statvfs(str(db.path.parent))
        free_gb = stat.f_bavail * stat.f_frsize / (1024**3)
        results.append(
            _ok("disk_free_above_10gb", free_gb >= 10.0, f"{free_gb:.1f} GB")
        )
    except Exception as exc:
        results.append(_ok("disk_free_above_10gb", False, str(exc)))

    # 8. Config fingerprint matches startup (passed via env or settings).
    expected = os.environ.get("EXPECTED_FINGERPRINT", "")
    if expected:
        actual = compute_fingerprint(dict(os.environ))
        results.append(
            _ok(
                "config_fingerprint_unchanged",
                actual == expected,
                f"actual={actual[:16]}... expected={expected[:16]}...",
            )
        )

    overall = all(bool(r["ok"]) for r in results)
    return overall, results


def _print_report(overall: bool, results: list[dict[str, object]]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    width = max((len(str(r["check"])) for r in results), default=10)
    print(f"\n=== Poly Terminal Final — Daily Check {now} ===\n")
    for r in results:
        mark = "OK   " if r["ok"] else "FAIL "
        print(f"  {mark} {str(r['check']).ljust(width)}  {r['detail']}")
    print(f"\nOVERALL: {'PASS' if overall else 'FAIL'}\n")


async def _async_main(argv: list[str]) -> int:
    settings = Settings(_env_file=None)
    db = Database(settings.db_path)
    if not Path(settings.db_path).is_file():
        print(f"[daily-check] no DB at {settings.db_path}", file=sys.stderr)
        return 1
    overall, results = await run_checks(db)
    if "--json" in argv:
        print(json.dumps({"overall": overall, "results": results}, indent=2))
    else:
        _print_report(overall, results)
    return 0 if overall else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_async_main(argv or sys.argv[1:]))


if __name__ == "__main__":
    sys.exit(main())
