"""Versioned SQLite migration runner.

Each migration is a module under `poly_terminal.persistence.migrations` whose
filename matches `NNNN_<slug>.py` (4-digit zero-padded version). The module
exports an `apply(conn)` async function that issues DDL/DML against an open
`aiosqlite.Connection`.

`CURRENT_SCHEMA_VERSION` is computed dynamically from the highest migration
version on disk — no hand-maintained constant to drift from reality.
"""

from __future__ import annotations

import importlib
import pkgutil
import re
from dataclasses import dataclass
from typing import Awaitable, Callable

import aiosqlite

_MIGRATION_RE = re.compile(r"^(\d{4})_[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    apply: Callable[[aiosqlite.Connection], Awaitable[None]]


def list_migrations() -> list[Migration]:
    """Discover migrations by scanning the package and importing each module."""
    from poly_terminal.persistence import migrations as pkg

    out: list[Migration] = []
    for info in pkgutil.iter_modules(pkg.__path__):
        m = _MIGRATION_RE.match(info.name)
        if not m:
            continue
        module = importlib.import_module(f"{pkg.__name__}.{info.name}")
        if not hasattr(module, "apply"):
            msg = f"migration {info.name} missing apply(conn)"
            raise RuntimeError(msg)
        out.append(
            Migration(version=int(m.group(1)), name=info.name, apply=module.apply)
        )
    out.sort(key=lambda x: x.version)
    return out


def _current_version(migrations: list[Migration]) -> int:
    return migrations[-1].version if migrations else 0


CURRENT_SCHEMA_VERSION: int = _current_version(list_migrations())


async def _read_db_version(conn: aiosqlite.Connection) -> int:
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_meta ("
        "  key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    cur = await conn.execute(
        "SELECT value FROM schema_meta WHERE key='version'"
    )
    row = await cur.fetchone()
    return int(row[0]) if row else 0


async def apply_pending(conn: aiosqlite.Connection) -> int:
    """Apply every migration whose version > current. Returns number applied."""
    migrations = list_migrations()
    current = await _read_db_version(conn)
    applied = 0
    for m in migrations:
        if m.version <= current:
            continue
        await m.apply(conn)
        await conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(m.version),),
        )
        await conn.commit()
        applied += 1
    return applied
