"""Sync POLY_API_{KEY,SECRET,PASSPHRASE} in .env to the live-derived creds.

2026-05-05 — `.env` can carry stale L2 API credentials that no longer
match the server-side state (Polymarket rotates / users delete keys via
the dashboard / a previous wallet was used). The bot tolerates this
because `_init_live_client` calls `derive_l2_creds_from_private_key`
on every boot and uses the fresh result, ignoring the .env values.

But anything else that reads .env directly — manual scripts, the
`poly_terminal.config.settings` snapshot at boot for diagnostics, or a
human checking the file — sees the stale values. This script fixes that
by:

  1. Loading the current .env values
  2. Calling `create_or_derive_api_key` against POLY_PRIVATE_KEY +
     POLY_PROXY_ADDRESS to get the canonical creds
  3. Verifying the derived creds against `/auth/api-keys` (an authed L2
     endpoint) — refuses to write if the server rejects them
  4. Backing up .env to `.env.YYYYMMDD-HHMMSS.bak`
  5. Rewriting only the three POLY_API_* lines, preserving comments and
     ordering

Usage:

    poly-sync-l2-creds                  # in-place sync, default .env
    poly-sync-l2-creds --dry-run        # show diff, don't write
    poly-sync-l2-creds --env-file .env  # explicit path

Environment:
    LOG_LEVEL    DEBUG|INFO|WARNING|ERROR
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sys
import time
from pathlib import Path

logger = logging.getLogger("poly_terminal.scripts.sync_l2_creds")


def _setup_logging() -> None:
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _read_env(path: Path) -> dict[str, str]:
    """Minimal .env reader — strips inline comments and surrounding
    whitespace. Doesn't shell-expand, doesn't handle quoted values
    (the bot's .env doesn't use those)."""
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


def _derive_fresh(host: str, private_key: str, funder: str) -> tuple[str, str, str]:
    """Return (api_key, api_secret, api_passphrase) freshly derived
    from the L1 wallet. Raises on any failure."""
    from py_clob_client_v2.client import ClobClient

    client = ClobClient(
        host=host, chain_id=137, key=private_key,
        signature_type=1, funder=funder,
    )
    creds = client.create_or_derive_api_key()
    if creds is None or not creds.api_key:
        raise RuntimeError("create_or_derive_api_key returned no creds")
    return creds.api_key, creds.api_secret, creds.api_passphrase


def _verify_creds(
    host: str, private_key: str, funder: str,
    api_key: str, api_secret: str, api_passphrase: str,
) -> bool:
    """Confirm the creds are accepted by an authed L2 endpoint."""
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds

    client = ClobClient(
        host=host, chain_id=137, key=private_key,
        signature_type=1, funder=funder,
        creds=ApiCreds(
            api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase,
        ),
    )
    try:
        resp = client.get_api_keys()
    except Exception:
        logger.exception("verify: get_api_keys raised")
        return False
    if not isinstance(resp, dict):
        logger.warning("verify: unexpected response shape: %r", resp)
        return False
    keys = resp.get("apiKeys") or []
    if api_key in keys:
        return True
    logger.warning(
        "verify: derived key %s not in server-listed keys %s",
        api_key, keys,
    )
    return False


def _rewrite_env(
    path: Path,
    *,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
) -> str:
    """Return the new .env contents with only the three POLY_API_* lines
    rewritten. Preserves comments, ordering, and any trailing
    whitespace on unchanged lines.

    Lines that don't match `^KEY=...` are left untouched. If a
    POLY_API_* line is missing from the source, it's appended at the
    end of its section (we don't try to be clever about where).
    """
    targets = {
        "POLY_API_KEY": api_key,
        "POLY_API_SECRET": api_secret,
        "POLY_API_PASSPHRASE": api_passphrase,
    }
    seen: set[str] = set()
    out_lines: list[str] = []
    pat = re.compile(r"^([A-Z][A-Z0-9_]*)\s*=\s*(.*)$")
    for line in path.read_text(encoding="utf-8").splitlines(keepends=True):
        body = line.rstrip("\r\n")
        m = pat.match(body)
        if m and m.group(1) in targets:
            key = m.group(1)
            seen.add(key)
            # Preserve any trailing inline comment
            tail = ""
            old_value_with_comment = m.group(2)
            if "#" in old_value_with_comment:
                comment_idx = old_value_with_comment.index("#")
                # Keep the inline comment exactly
                tail = "  " + old_value_with_comment[comment_idx:]
            new_line = f"{key}={targets[key]}{tail}"
            # Preserve the original line ending
            if line.endswith("\r\n"):
                new_line += "\r\n"
            elif line.endswith("\n"):
                new_line += "\n"
            out_lines.append(new_line)
        else:
            out_lines.append(line)
    # Append any missing keys at the end with a comment marker.
    missing = [k for k in targets if k not in seen]
    if missing:
        if out_lines and not out_lines[-1].endswith("\n"):
            out_lines.append("\n")
        out_lines.append("\n# ─── L2 creds appended by poly-sync-l2-creds ───\n")
        for k in missing:
            out_lines.append(f"{k}={targets[k]}\n")
    return "".join(out_lines)


def _diff_summary(
    old: dict[str, str], new_values: dict[str, str]
) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for key, new_val in new_values.items():
        old_val = old.get(key, "<unset>")
        rows.append((key, old_val, new_val))
    return rows


def main() -> int:
    _setup_logging()
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0] if __doc__ else None,
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=".env",
        help="Path to .env file (default: ./.env)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the proposed change but DO NOT write the file.",
    )
    args = parser.parse_args()

    env_path = Path(args.env_file).expanduser().resolve()
    if not env_path.exists():
        logger.error("env file not found: %s", env_path)
        return 2
    env = _read_env(env_path)

    private_key = env.get("POLY_PRIVATE_KEY", "")
    funder = env.get("POLY_PROXY_ADDRESS", "")
    host = env.get("CLOB_API_URL", "https://clob.polymarket.com")
    if not private_key or not funder:
        logger.error(
            "POLY_PRIVATE_KEY and POLY_PROXY_ADDRESS must be set in %s",
            env_path,
        )
        return 2

    logger.info(
        "deriving fresh L2 creds from L1 (host=%s funder=%s)",
        host, funder,
    )
    try:
        api_key, api_secret, api_passphrase = _derive_fresh(
            host, private_key, funder,
        )
    except Exception:
        logger.exception("derive failed; refusing to write")
        return 3

    logger.info("verifying derived creds against authed L2 endpoint")
    if not _verify_creds(
        host, private_key, funder, api_key, api_secret, api_passphrase,
    ):
        logger.error(
            "derived creds did NOT verify; refusing to write a broken .env"
        )
        return 4

    diff = _diff_summary(
        env,
        {
            "POLY_API_KEY": api_key,
            "POLY_API_SECRET": api_secret,
            "POLY_API_PASSPHRASE": api_passphrase,
        },
    )
    print()
    print(f"diff for {env_path}:")
    print("-" * 80)
    any_change = False
    for key, old, new in diff:
        same = (old == new)
        marker = "  " if same else "* "
        if not same:
            any_change = True
        print(f"  {marker}{key}")
        print(f"      old: {old}")
        print(f"      new: {new}")
    print("-" * 80)
    if not any_change:
        print("✓ .env is already in sync with derived creds — nothing to do.")
        return 0

    if args.dry_run:
        print("\n[dry-run] would back up .env and rewrite the three keys above.")
        return 0

    backup_name = f"{env_path.name}.{time.strftime('%Y%m%d-%H%M%S')}.bak"
    backup = env_path.with_name(backup_name)
    shutil.copy2(env_path, backup)
    logger.info("backup: %s", backup)

    new_contents = _rewrite_env(
        env_path,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )
    env_path.write_text(new_contents, encoding="utf-8")
    logger.info("wrote new .env (%d bytes)", len(new_contents))
    print(f"\n✓ {env_path} updated. Backup at {backup}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
