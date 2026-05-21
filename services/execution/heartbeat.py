"""Heartbeat coroutine (spec §3.4, §11.1).

In live mode this coroutine pings the CLOB every 5s to keep open GTC
orders alive; in paper mode it ticks but does nothing externally so the
code path is identical (spec §15 Phase 1 step 6). Missing a window
triggers ``on_miss``, which in production self-fences (cancel all,
refuse new intents, alert)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class HeartbeatConfig:
    interval_seconds: float = 5.0
    miss_threshold_seconds: float = 8.0


class HeartbeatCoroutine:
    def __init__(
        self,
        *,
        open_orders: Callable[[], Iterable[str]],
        beat: Callable[[Iterable[str]], Awaitable[None]],
        on_miss: Callable[[], Awaitable[None]],
        config: HeartbeatConfig | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    ) -> None:
        self._open_orders = open_orders
        self._beat = beat
        self._on_miss = on_miss
        self._config = config or HeartbeatConfig()
        self._clock = clock
        self._last_beat_at = clock()
        self._stop_event = asyncio.Event()

    @property
    def last_beat_at(self) -> datetime:
        return self._last_beat_at

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            now = self._clock()
            age = (now - self._last_beat_at).total_seconds()
            if age > self._config.miss_threshold_seconds:
                await self._on_miss()
                return
            await self._beat(self._open_orders())
            self._last_beat_at = self._clock()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._config.interval_seconds
                )
            except TimeoutError:
                continue
