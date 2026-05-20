"""Shared exponential-backoff helper for WebSocket clients.

Used by both `MarketWebSocket` and `UserWebSocket`. No network code;
pure delay-computation that the consumer awaits between connect attempts.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass
class Backoff:
    """Exponential backoff with optional jitter.

    delay(n) = min(max_s, initial_s * factor ** n)
    With jitter, the result is uniformly drawn from [delay * 0.5, delay * 1.5].
    """

    initial_s: float = 1.0
    max_s: float = 60.0
    factor: float = 2.0
    jitter: bool = True
    attempts: int = 0

    def next_delay(self) -> float:
        base = min(self.max_s, self.initial_s * (self.factor ** self.attempts))
        self.attempts += 1
        if not self.jitter:
            return base
        low = base * 0.5
        high = min(self.max_s, base * 1.5)
        return random.uniform(low, high)

    def reset(self) -> None:
        self.attempts = 0
