"""promote — record a mode transition in the audit log.

The bot's boot path checks for a fresh promotion before lifting the
READ_ONLY safety lock (see `--allow-mode` in main.py).

Usage:
  python -m poly_terminal.scripts.promote --to PAPER --signed-by "$USER@$(hostname)"
  python -m poly_terminal.scripts.promote --to READ_ONLY --reason "emergency stop"

Exit codes:
  0  success — promotion row written
  2  invalid arguments / preflight drift (caller should fix env first)
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import os
import sys
import time
from typing import Final

from poly_terminal.config.fingerprint import compute_fingerprint
from poly_terminal.config.preset_loader import apply_preset_to_env
from poly_terminal.config.settings import Settings
from poly_terminal.persistence.db import Database
from poly_terminal.persistence.repositories.mode_promotions import (
    ModePromotionsRepo,
)
from poly_terminal.scripts.preflight import main as preflight_main

_VALID_MODES: Final[frozenset[str]] = frozenset(
    {"READ_ONLY", "PAPER", "LIVE_DRY", "LIVE"}
)


def _confirm_interactively(to_mode: str, signed_by: str) -> bool:
    print(
        f"\nPromote BOT_MODE → {to_mode} signed by {signed_by!r}? [y/N] ",
        end="",
        flush=True,
    )
    try:
        answer = input().strip().lower()
    except EOFError:
        return False
    return answer == "y"


def _parse_argv(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote bot mode.")
    parser.add_argument(
        "--to", required=True, choices=sorted(_VALID_MODES),
        help="Target mode.",
    )
    parser.add_argument(
        "--signed-by", required=True,
        help='Operator identifier, typically "$USER@$(hostname)".',
    )
    parser.add_argument(
        "--reason", default="",
        help="Free-form reason; visible in mode_promotions.",
    )
    parser.add_argument(
        "--acceptance-report", default="",
        help="Optional path to an acceptance report JSON. Verified to exist.",
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="Skip the interactive confirmation prompt.",
    )
    return parser.parse_args(argv)


async def _async_main(argv: list[str]) -> int:
    args = _parse_argv(argv)

    # Some promotions require an acceptance report file to exist.
    # We only verify presence, not contents — the agent that produces
    # the report knows the schema.
    if args.acceptance_report and args.acceptance_report not in ("/dev/null",):
        if not os.path.isfile(args.acceptance_report):
            print(
                f"[promote] acceptance report not found: {args.acceptance_report}",
                file=sys.stderr,
            )
            return 2

    if not args.yes and not _confirm_interactively(args.to, args.signed_by):
        print("[promote] aborted.", file=sys.stderr)
        return 2

    # Apply preset overlay so the fingerprint we compute is the same one
    # the boot path will see.
    try:
        apply_preset_to_env()
    except Exception as exc:
        print(f"[promote] preset overlay failed: {exc}", file=sys.stderr)

    # Verify config drift fail-fast (skip for emergency demote to READ_ONLY).
    # Suppress preflight stdout so the only JSON the operator sees is ours;
    # if drift exists, surface it to stderr.
    if args.to != "READ_ONLY":
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = preflight_main()
        if rc != 0:
            print(buf.getvalue(), file=sys.stderr)
            return 2

    settings = Settings(_env_file=None)
    db = Database(settings.db_path)
    await db.initialize()
    repo = ModePromotionsRepo(db)
    latest = await repo.latest()
    from_mode = latest.to_mode if latest is not None else "READ_ONLY"
    fingerprint = compute_fingerprint(dict(os.environ))
    promotion_id = await repo.insert(
        from_mode=from_mode,
        to_mode=args.to,
        ts=int(time.time()),
        signed_by=args.signed_by,
        fingerprint=fingerprint,
        reason=args.reason or None,
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "promotion_id": promotion_id,
                "from": from_mode,
                "to": args.to,
                "fingerprint": fingerprint,
                "signed_by": args.signed_by,
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_async_main(argv if argv is not None else sys.argv[1:]))


if __name__ == "__main__":
    sys.exit(main())
