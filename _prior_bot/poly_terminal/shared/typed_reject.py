"""Typed reject result returned by every gate.

Every gate returns either `None` (pass) or a `Reject(code, detail)` (fail).
Gate-rejection telemetry counts pass and per-`code` rejects so the operator
can see the rejection waterfall in `/metrics`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Reject:
    """A single typed rejection. `code` is a stable machine name; `detail`
    is human-readable extra context (free-form, never structured)."""

    code: str
    detail: str = ""
