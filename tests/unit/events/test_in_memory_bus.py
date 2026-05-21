from __future__ import annotations

import asyncio

import pytest

from shared.events import InMemoryEventBus


async def _drain(it, n: int):
    out = []
    for _ in range(n):
        out.append(await anext(it))
    return out


async def test_publish_subscribe_roundtrip() -> None:
    bus = InMemoryEventBus()
    it = await bus.subscribe("signal.emitted")
    envelope = await bus.publish("signal.emitted", '{"hello":"world"}')
    received = await anext(it)
    assert received.id == envelope.id
    assert received.payload == '{"hello":"world"}'


async def test_late_subscribers_miss_earlier_messages() -> None:
    bus = InMemoryEventBus()
    await bus.publish("signal.emitted", "first")
    it = await bus.subscribe("signal.emitted")
    await bus.publish("signal.emitted", "second")
    received = await anext(it)
    assert received.payload == "second"


async def test_multiple_subscribers_each_receive() -> None:
    bus = InMemoryEventBus()
    a = await bus.subscribe("intent.approved")
    b = await bus.subscribe("intent.approved")
    await bus.publish("intent.approved", "payload")
    msg_a = await anext(a)
    msg_b = await anext(b)
    assert msg_a.payload == msg_b.payload == "payload"
    assert msg_a.id == msg_b.id


async def test_streams_are_isolated() -> None:
    bus = InMemoryEventBus()
    sig_it = await bus.subscribe("signal.emitted")
    await bus.publish("intent.approved", "should-not-arrive")
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(sig_it), timeout=0.05)
