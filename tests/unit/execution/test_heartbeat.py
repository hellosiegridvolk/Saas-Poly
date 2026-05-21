from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

from services.execution.heartbeat import HeartbeatConfig, HeartbeatCoroutine


class _FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


async def test_heartbeat_invokes_beat_with_open_orders() -> None:
    beats: list[list[str]] = []

    async def beat(orders: Iterable[str]) -> None:
        beats.append(list(orders))

    async def on_miss() -> None:
        raise AssertionError("should not miss")

    hb = HeartbeatCoroutine(
        open_orders=lambda: ["o1", "o2"],
        beat=beat,
        on_miss=on_miss,
        config=HeartbeatConfig(interval_seconds=0.01, miss_threshold_seconds=10),
    )
    task = asyncio.create_task(hb.run())
    await asyncio.sleep(0.03)
    hb.stop()
    await asyncio.wait_for(task, timeout=1)
    assert beats and beats[0] == ["o1", "o2"]


async def test_heartbeat_calls_on_miss_when_window_blown() -> None:
    missed = asyncio.Event()

    async def beat(orders: Iterable[str]) -> None:
        return None

    async def on_miss() -> None:
        missed.set()

    clock = _FakeClock(datetime(2026, 5, 21, tzinfo=UTC))
    hb = HeartbeatCoroutine(
        open_orders=lambda: [],
        beat=beat,
        on_miss=on_miss,
        config=HeartbeatConfig(interval_seconds=0.01, miss_threshold_seconds=1.0),
        clock=clock,
    )
    # Advance the clock past the threshold before the first iteration runs.
    clock.advance(5.0)
    await hb.run()
    assert missed.is_set()


async def test_stop_terminates_loop_promptly() -> None:
    async def beat(orders: Iterable[str]) -> None:
        return None

    async def on_miss() -> None:
        return None

    hb = HeartbeatCoroutine(
        open_orders=lambda: [],
        beat=beat,
        on_miss=on_miss,
        config=HeartbeatConfig(interval_seconds=5.0, miss_threshold_seconds=60.0),
    )
    task = asyncio.create_task(hb.run())
    await asyncio.sleep(0.01)
    hb.stop()
    await asyncio.wait_for(task, timeout=1.0)
    assert task.done()
