"""Event-bus abstraction (spec §9).

The production implementation is Redis Streams; v1 paper mode runs
end-to-end on an in-memory bounded asyncio.Queue (spec §3.3 — bounded
queues, no global mutable state). Both implementations satisfy the
:class:`EventBus` Protocol so services swap between them via config.

Payloads are Pydantic models serialized with ``model_dump_json()``;
they are stored as raw JSON in the envelope so the bus stays
serializer-agnostic.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable
from uuid import UUID, uuid4

StreamName = Literal[
    "market.tick",
    "market.book",
    "signal.emitted",
    "intent.approved",
    "intent.rejected",
    "order.submitted",
    "order.canceled",
    "fill.received",
    "position.updated",
    "risk.kill_switch",
    "reconcile.mismatch",
]


@dataclass(frozen=True)
class EventEnvelope:
    """A single message on a stream."""

    id: UUID
    stream: StreamName
    payload: str
    """Serialized payload (JSON). Consumer is responsible for decoding."""


@runtime_checkable
class EventBus(Protocol):
    async def publish(self, stream: StreamName, payload: str) -> EventEnvelope: ...

    async def subscribe(
        self, stream: StreamName
    ) -> AsyncIterator[EventEnvelope]: ...


class InMemoryEventBus:
    """Bounded in-memory pub/sub. One queue per stream. Each call to
    :meth:`subscribe` returns its own async iterator backed by a private
    asyncio.Queue so multiple subscribers see every message.

    Bounded by ``maxsize`` per subscriber queue; backpressure surfaces as
    ``QueueFull`` from :meth:`publish`, which the publisher must handle
    (spec §3.3 — bounded queues, deliberate handoff)."""

    def __init__(self, *, maxsize: int = 1000) -> None:
        self._maxsize = maxsize
        self._lock = asyncio.Lock()
        self._subscribers: dict[StreamName, list[asyncio.Queue[EventEnvelope]]] = {}

    async def publish(self, stream: StreamName, payload: str) -> EventEnvelope:
        envelope = EventEnvelope(id=uuid4(), stream=stream, payload=payload)
        async with self._lock:
            queues = list(self._subscribers.get(stream, ()))
        for q in queues:
            await q.put(envelope)
        return envelope

    async def _new_subscriber_queue(
        self, stream: StreamName
    ) -> asyncio.Queue[EventEnvelope]:
        q: asyncio.Queue[EventEnvelope] = asyncio.Queue(maxsize=self._maxsize)
        async with self._lock:
            self._subscribers.setdefault(stream, []).append(q)
        return q

    async def subscribe(self, stream: StreamName) -> AsyncIterator[EventEnvelope]:
        queue = await self._new_subscriber_queue(stream)

        async def _iter() -> AsyncIterator[EventEnvelope]:
            while True:
                yield await queue.get()

        return _iter()
