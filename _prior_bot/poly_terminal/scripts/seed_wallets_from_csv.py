"""seed_wallets_from_csv — bootstrap wallet_scores from a leaderboard CSV.

Useful when no Data API leaderboard is available (offline test, alternate
data source, or pre-soak warm-up). The CSV must have at minimum these
columns: rank, proxyWallet, pnl, vol. Optional: userName, wallet_label.

Usage:
  python -m poly_terminal.scripts.seed_wallets_from_csv path/to/file.csv

Each row is upserted into wallet_scores with placeholder values that
survive ingestion until WalletIntelAgent.refresh_scores_and_rank() runs
against real history. The seeded conviction_score uses the same compression
as the Data API sync so leaderboard CSV and live API produce comparable
ranks.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import sys
import time
from pathlib import Path

from poly_terminal.config.settings import Settings
from poly_terminal.data.data_api.leaderboard import LeaderboardEntry
from poly_terminal.persistence.db import Database
from poly_terminal.persistence.repositories.wallets import (
    WalletScore,
    WalletsRepo,
)


def _parse_row(row: dict[str, str]) -> LeaderboardEntry | None:
    addr = row.get("proxyWallet") or row.get("address") or ""
    addr = addr.strip().lower()
    if not addr.startswith("0x") or len(addr) != 42:
        return None
    try:
        pnl = float(row.get("pnl", "0") or 0)
        vol = float(row.get("vol", "0") or 0)
    except ValueError:
        return None
    return LeaderboardEntry(address=addr, pnl=pnl, volume=vol, trades=0, raw=row)


def _seed_conviction(entry: LeaderboardEntry) -> float:
    pnl_term = math.log1p(max(0.0, entry.pnl))
    vol_term = math.log1p(max(0.0, entry.volume))
    return max(0.0, (pnl_term * 0.05) + (vol_term * 0.03))


async def seed_csv(
    path: Path, db: Database, category: str = "crypto"
) -> int:
    repo = WalletsRepo(db)
    now = int(time.time())
    written = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entry = _parse_row(row)
            if entry is None:
                continue
            avg_roi = (entry.pnl / entry.volume) if entry.volume > 0 else 0.0
            await repo.upsert_score(
                WalletScore(
                    wallet=entry.address,
                    win_rate=0.55,
                    avg_roi_pct=float(avg_roi),
                    trades_30d=0,
                    median_position_usd=0.0,
                    conviction_score=_seed_conviction(entry),
                    last_updated=now,
                    category=category,
                )
            )
            written += 1
    return written


async def _async_main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path", type=Path)
    p.add_argument(
        "--category",
        default="crypto",
        help="Category tag stored on every seeded wallet (default: crypto)",
    )
    args = p.parse_args(argv)
    if not args.csv_path.is_file():
        print(f"[seed-csv] file not found: {args.csv_path}", file=sys.stderr)
        return 2
    settings = Settings(_env_file=None)
    db = Database(settings.db_path)
    await db.initialize()
    written = await seed_csv(args.csv_path, db, category=args.category)
    print(
        json.dumps(
            {
                "status": "ok",
                "wallets_written": written,
                "category": args.category,
                "db": str(settings.db_path),
            },
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_async_main(argv if argv is not None else sys.argv[1:]))


if __name__ == "__main__":
    sys.exit(main())
