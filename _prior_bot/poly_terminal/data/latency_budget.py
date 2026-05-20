"""Per-agent latency budget + circuit breaker.

Wraps every external call. Records elapsed milliseconds in a rolling window;
emits `EVT_LATENCY_BUDGET_BREACH` when rolling p95 exceeds the configured
ceiling. Latency-sensitive consumers subscribe to this event and self-degrade
(see docs/02_V3_ARCHITECTURE.md §4).

Usage:

    budget = LatencyBudget(name="gamma", ceiling_ms=1000, window_size=50, bus=bus)

    @latency_tracked(budget)
    async def fetch_market(slug: str) -> Market: ...
"""

from __future__ import annotations

import functools
import logging
import time
from collections import deque
from typing import Any, Awaitable, Callable, TypeVar

from poly_terminal.bus.event_bus import EventBus
from poly_terminal.bus.events import EVT_LATENCY_BUDGET_BREACH

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _percentile(samples: list[float], pct: float) -> float | None:
    if not samples:
        return None
    s = sorted(samples)
    # Nearest-rank percentile — fine for our small windows.
    k = max(0, min(len(s) - 1, int(round(pct * (len(s) - 1)))))
    return s[k]


class LatencyBudget:
    """Rolling p95 + circuit breaker.

    The circuit is **open** (= breached) when p95 over the current window
    exceeds `ceiling_ms`. When the next observation drops p95 back below the
    ceiling, the circuit closes.
    """

    def __init__(
        self,
        name: str,
        ceiling_ms: int,
        window_size: int = 50,
        bus: EventBus | None = None,
    ) -> None:
        self.name = name
        self.ceiling_ms = ceiling_ms
        self.window_size = window_size
        self._bus = bus
        self._samples: deque[float] = deque(maxlen=window_size)
        self._open = False

    @property
    def sample_count(self) -> int:
        return len(self._samples)

    def p50_ms(self) -> float | None:
        return _percentile(list(self._samples), 0.50)

    def p95_ms(self) -> float | None:
        return _percentile(list(self._samples), 0.95)

    def p99_ms(self) -> float | None:
        return _percentile(list(self._samples), 0.99)

    def is_open(self) -> bool:
        return self._open

    def observe(self, elapsed_ms: float) -> None:
        """Record one sample and update circuit state."""
        self._samples.append(float(elapsed_ms))
        p95 = self.p95_ms()
        if p95 is None:
            return
        was_open = self._open
        self._open = p95 > self.ceiling_ms
        if self._open and not was_open and self._bus is not None:
            # Fire-and-forget: the bus call is async but observe is sync;
            # surface the breach via task scheduling. Tests use observe_async
            # to await delivery deterministically.
            import asyncio

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            payload = self._breach_payload(p95)
            if loop is not None:
                loop.create_task(
                    self._bus.publish(EVT_LATENCY_BUDGET_BREACH, payload)
                )
            else:
                logger.warning(
                    "latency_budget breach for %s but no running loop",
                    self.name,
                )

    async def observe_async(self, elapsed_ms: float) -> None:
        """Await-able variant — guarantees breach is emitted before returning."""
        self._samples.append(float(elapsed_ms))
        p95 = self.p95_ms()
        if p95 is None:
            return
        was_open = self._open
        self._open = p95 > self.ceiling_ms
        if self._open and not was_open and self._bus is not None:
            await self._bus.publish(
                EVT_LATENCY_BUDGET_BREACH, self._breach_payload(p95)
            )

    def _breach_payload(self, p95: float) -> dict[str, Any]:
        return {
            "agent": self.name,
            "p95_ms": p95,
            "ceiling_ms": self.ceiling_ms,
            "sample_count": self.sample_count,
        }

    def summary(self) -> dict[str, Any]:
        return {
            "agent": self.name,
            "ceiling_ms": self.ceiling_ms,
            "window_size": self.window_size,
            "sample_count": self.sample_count,
            "p50_ms": self.p50_ms(),
            "p95_ms": self.p95_ms(),
            "p99_ms": self.p99_ms(),
            "is_open": self._open,
        }


def latency_tracked(
    budget: LatencyBudget,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator that times an async callable and feeds the budget."""

    def wrap(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(fn)
        async def inner(*args: Any, **kwargs: Any) -> T:
            start = time.monotonic()
            try:
                return await fn(*args, **kwargs)
            finally:
                elapsed_ms = (time.monotonic() - start) * 1000
                await budget.observe_async(elapsed_ms)

        return inner

    return wrap
