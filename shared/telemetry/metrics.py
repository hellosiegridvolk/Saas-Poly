"""Metrics protocol used across services (spec §19).

A Prometheus-backed implementation lives in services/observability (later
phase); for tests and paper mode the NoOpMetrics is enough.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Metrics(Protocol):
    def increment(self, name: str, value: float = 1.0, **tags: str) -> None: ...
    def observe(self, name: str, value: float, **tags: str) -> None: ...
    def gauge(self, name: str, value: float, **tags: str) -> None: ...


class NoOpMetrics:
    """A Metrics implementation that drops everything. Safe for tests."""

    def increment(self, name: str, value: float = 1.0, **tags: str) -> None:
        return None

    def observe(self, name: str, value: float, **tags: str) -> None:
        return None

    def gauge(self, name: str, value: float, **tags: str) -> None:
        return None
