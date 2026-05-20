"""SQLite connection helpers + initialization.

The Database class is the only place that knows about file paths, PRAGMAs,
and migration application. Repositories take an open connection or a
Database handle.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

from poly_terminal.persistence.migrations.runner import apply_pending

logger = logging.getLogger(__name__)


class Database:
    """SQLite handle with WAL + foreign_keys + migrations.

    Connections are short-lived: open per operation via `async with db.connect()`.
    aiosqlite uses a thread-per-connection model so this is the safe pattern
    for asyncio code.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def initialize(self) -> int:
        """Ensure parent dir, apply pending migrations. Returns count applied."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.path) as conn:
            await self._configure(conn)
            applied = await apply_pending(conn)
            if applied:
                logger.info("Applied %d migration(s) to %s", applied, self.path)
        return applied

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[aiosqlite.Connection]:
        async with aiosqlite.connect(self.path) as conn:
            await self._configure(conn)
            yield conn

    @staticmethod
    async def _configure(conn: aiosqlite.Connection) -> None:
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.execute("PRAGMA journal_mode = WAL")
        await conn.execute("PRAGMA synchronous = NORMAL")
        await conn.execute("PRAGMA busy_timeout = 5000")

    async def integrity_check(self) -> str:
        """Run `PRAGMA integrity_check` and return the first row value."""
        async with self.connect() as conn:
            cur = await conn.execute("PRAGMA integrity_check")
            row = await cur.fetchone()
        return str(row[0]) if row else ""
