"""Soak-run performance analyzer with shadow-fallback filter.

2026-05-05 — `_resolve_exit_price` falling back to entry_price (on
get_best_bid 404) records closes with `realized_pnl=0` and
`exit_price = entry_price`. These are NOT real flats — they're
404-fallback artifacts whose true outcome can only be recovered via
post-resolution reconciliation against Gamma. When computing soak
WR/PnL metrics, those rows must be either:

  - excluded entirely (treat as "outcome unknown — wait for
    reconciliation"), or
  - reconciled first via `poly-resolution-reconcile`, after which they
    carry `outcome='TIME_RECONCILED'` and a real PnL.

This script reports four columns per strategy:

  raw          : every closed row, no filter. Inflated by hidden flats.
  reconciled   : rows with outcome='TIME_RECONCILED' carry real PnL.
  excl_unrec   : excludes still-flat 404 artifacts (TIME/TIME_RESTORE
                 with realized_pnl=0 AND entry==exit). Closes the
                 reporting gap on the unreconciled tail.
  decided      : excl_unrec further restricted to W+L only (no zero-PnL
                 rows of any kind) — the "true win rate" cohort.

Usage:

    poly-soak-analysis                          # since process boot
    poly-soak-analysis --since 1777964409       # since unix ts
    poly-soak-analysis --hours 6                # last N hours
    poly-soak-analysis --strategies copy_scalp,copy_trade
    poly-soak-analysis --json                   # machine-readable

Environment:
    DB_PATH   sqlite path (default: exports/state.db)
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass


_DEFAULT_DB_PATH = "exports/state.db"


@dataclass
class StrategyStats:
    strategy: str
    n_closes: int = 0
    n_w: int = 0
    n_l: int = 0
    n_flat: int = 0
    n_reconciled_w: int = 0
    n_reconciled_l: int = 0
    n_unreconciled_flat: int = 0  # the 404-fallback artifacts
    pnl_raw: float = 0.0
    pnl_reconciled_only: float = 0.0


# ── SQL helpers ──────────────────────────────────────────────────────


_BASE_FILTER = (
    "p.closed_ts IS NOT NULL "
    "AND p.entry_intent_id NOT LIKE 'imported%' "
)


def _strategy_filter(strategies: list[str] | None) -> tuple[str, tuple]:
    if not strategies:
        return "", ()
    placeholders = ",".join("?" * len(strategies))
    return f" AND lo.strategy IN ({placeholders})", tuple(strategies)


def _time_filter(since_ts: int | None) -> tuple[str, tuple]:
    if since_ts is None:
        return "", ()
    return " AND p.closed_ts >= ?", (since_ts,)


def _join_clause() -> str:
    return (
        "FROM positions p "
        "JOIN live_orders lo ON lo.intent_id = p.entry_intent_id "
    )


# ── Per-strategy aggregator ─────────────────────────────────────────


def _compute_strategy_stats(
    conn: sqlite3.Connection,
    *,
    since_ts: int | None,
    strategies: list[str] | None,
) -> dict[str, StrategyStats]:
    """Single big query gives us all the buckets we need per strategy.

    Buckets:
      win                 : realized_pnl > 0 (any outcome)
      loss                : realized_pnl < 0 (any outcome)
      flat_unreconciled   : realized_pnl = 0 AND entry==exit AND outcome
                            IN ('TIME','TIME_RESTORE') — 404 artifact
      flat_other          : realized_pnl = 0 AND NOT in the above bucket
      reconciled_win      : outcome='TIME_RECONCILED' AND realized_pnl > 0
      reconciled_loss     : outcome='TIME_RECONCILED' AND realized_pnl < 0
    """
    strat_clause, strat_params = _strategy_filter(strategies)
    time_clause, time_params = _time_filter(since_ts)
    sql = (
        "SELECT lo.strategy, "
        "  COUNT(*) AS n, "
        "  SUM(CASE WHEN p.realized_pnl > 0 THEN 1 ELSE 0 END) AS w, "
        "  SUM(CASE WHEN p.realized_pnl < 0 THEN 1 ELSE 0 END) AS l, "
        "  SUM(CASE WHEN p.realized_pnl = 0 THEN 1 ELSE 0 END) AS flat, "
        "  SUM(CASE "
        "        WHEN p.realized_pnl = 0 "
        "          AND p.exit_price = p.entry_price "
        "          AND p.outcome IN ('TIME','TIME_RESTORE') "
        "        THEN 1 ELSE 0 END) AS flat_unrec, "
        "  SUM(CASE WHEN p.outcome='TIME_RECONCILED' AND p.realized_pnl > 0 "
        "         THEN 1 ELSE 0 END) AS rec_w, "
        "  SUM(CASE WHEN p.outcome='TIME_RECONCILED' AND p.realized_pnl < 0 "
        "         THEN 1 ELSE 0 END) AS rec_l, "
        "  COALESCE(SUM(p.realized_pnl), 0) AS pnl_raw, "
        "  COALESCE(SUM(CASE WHEN p.outcome='TIME_RECONCILED' "
        "                    THEN p.realized_pnl ELSE 0 END), 0) "
        "    AS pnl_recon "
        + _join_clause()
        + "WHERE " + _BASE_FILTER + strat_clause + time_clause
        + " GROUP BY lo.strategy ORDER BY n DESC"
    )
    params = strat_params + time_params
    out: dict[str, StrategyStats] = {}
    for row in conn.execute(sql, params):
        s = StrategyStats(
            strategy=str(row[0]),
            n_closes=int(row[1]),
            n_w=int(row[2] or 0),
            n_l=int(row[3] or 0),
            n_flat=int(row[4] or 0),
            n_unreconciled_flat=int(row[5] or 0),
            n_reconciled_w=int(row[6] or 0),
            n_reconciled_l=int(row[7] or 0),
            pnl_raw=float(row[8] or 0.0),
            pnl_reconciled_only=float(row[9] or 0.0),
        )
        out[s.strategy] = s
    return out


# ── Reporting ────────────────────────────────────────────────────────


def _wr(w: int, l: int) -> float:
    decided = w + l
    return (100.0 * w / decided) if decided else 0.0


def _format_table(stats: dict[str, StrategyStats]) -> str:
    """Multi-column table with the four cohorts side by side."""
    if not stats:
        return "no closed positions match the filter"

    header = (
        f"{'strategy':12s}  {'raw':>22s}  {'excl_unrec':>22s}  "
        f"{'reconciled':>22s}  {'pnl_raw':>10s}  {'pnl_recon':>10s}\n"
    )
    sep = "-" * (12 + 4 + 22 + 4 + 22 + 4 + 22 + 4 + 10 + 4 + 10) + "\n"
    body = ""
    for s in stats.values():
        # raw = every row
        wr_raw = _wr(s.n_w, s.n_l)
        raw_cell = (
            f"n={s.n_closes:4d} W={s.n_w:3d} L={s.n_l:3d} {wr_raw:5.1f}%"
        )
        # excl_unrec — same numerator as raw (W,L based on pnl sign);
        # denominator drops the 404 artifacts. So WR is identical to
        # raw, but the "decided rate" view changes.
        n_excl = s.n_closes - s.n_unreconciled_flat
        excl_cell = (
            f"n={n_excl:4d} W={s.n_w:3d} L={s.n_l:3d} {wr_raw:5.1f}%"
        )
        # reconciled-only — TIME_RECONCILED rows (already won/lost via
        # Gamma resolution, so flats don't exist in this cohort).
        wr_rec = _wr(s.n_reconciled_w, s.n_reconciled_l)
        recon_cell = (
            f"n={s.n_reconciled_w + s.n_reconciled_l:4d} "
            f"W={s.n_reconciled_w:3d} L={s.n_reconciled_l:3d} {wr_rec:5.1f}%"
        )
        body += (
            f"{s.strategy:12s}  {raw_cell:>22s}  {excl_cell:>22s}  "
            f"{recon_cell:>22s}  ${s.pnl_raw:+9.2f}  ${s.pnl_reconciled_only:+9.2f}\n"
        )
    return header + sep + body


def _format_json(
    stats: dict[str, StrategyStats], *, since_ts: int | None
) -> str:
    payload = {
        "since_ts": since_ts,
        "generated_ts": int(time.time()),
        "strategies": [
            {
                "strategy": s.strategy,
                "n_closes": s.n_closes,
                "n_w": s.n_w,
                "n_l": s.n_l,
                "n_flat_total": s.n_flat,
                "n_flat_unreconciled": s.n_unreconciled_flat,
                "n_reconciled_win": s.n_reconciled_w,
                "n_reconciled_loss": s.n_reconciled_l,
                "wr_raw_pct": round(_wr(s.n_w, s.n_l), 2),
                "wr_reconciled_pct": round(
                    _wr(s.n_reconciled_w, s.n_reconciled_l), 2
                ),
                "pnl_raw_usd": round(s.pnl_raw, 4),
                "pnl_reconciled_only_usd": round(s.pnl_reconciled_only, 4),
            }
            for s in stats.values()
        ],
    }
    return json.dumps(payload, indent=2)


# ── CLI ─────────────────────────────────────────────────────────────


def _resolve_since(args: argparse.Namespace) -> int | None:
    if args.since is not None:
        return int(args.since)
    if args.hours is not None:
        return int(time.time() - float(args.hours) * 3600)
    if args.minutes is not None:
        return int(time.time() - float(args.minutes) * 60)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0] if __doc__ else None,
    )
    parser.add_argument(
        "--since",
        type=int,
        default=None,
        help="Unix epoch seconds — closes with closed_ts >= this value.",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=None,
        help="Closes from the last N hours (overrides --since if both given).",
    )
    parser.add_argument(
        "--minutes",
        type=float,
        default=None,
        help="Closes from the last N minutes.",
    )
    parser.add_argument(
        "--strategies",
        type=str,
        default=None,
        help="Comma-separated strategy filter (e.g. copy_scalp,copy_trade).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of the formatted table.",
    )
    args = parser.parse_args()

    db_path = os.environ.get("DB_PATH", "").strip() or _DEFAULT_DB_PATH
    if not os.path.exists(db_path):
        print(f"db not found: {db_path}", file=sys.stderr)
        return 2

    since_ts = _resolve_since(args)
    strategies = (
        [s.strip() for s in args.strategies.split(",") if s.strip()]
        if args.strategies else None
    )

    conn = sqlite3.connect(db_path)
    try:
        stats = _compute_strategy_stats(
            conn, since_ts=since_ts, strategies=strategies,
        )
    finally:
        conn.close()

    if args.json:
        print(_format_json(stats, since_ts=since_ts))
    else:
        if since_ts is not None:
            print(f"# closes since unix_ts={since_ts}")
        else:
            print("# all closes")
        print()
        print(_format_table(stats))
        # Footer with the unreconciled tail size if any.
        total_unrec = sum(s.n_unreconciled_flat for s in stats.values())
        if total_unrec:
            print(
                f"\n⚠  {total_unrec} unreconciled flat closes remain — run "
                f"`poly-resolution-reconcile` to recover their true PnL."
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
