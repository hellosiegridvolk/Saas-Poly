"""Deterministic hash of risk-critical config — see ADR 0004.

`compute_fingerprint(env)` returns a SHA-256 hex of the sorted
RISK_CRITICAL_KEYS pairs from `env`. The result is logged at boot, returned
in `/health`, and embedded in every export so outcomes can be attributed to
an exact config version.

`drift_against(expected, actual)` returns the per-key diff used by the
preflight script's fail-fast gate.
"""

from __future__ import annotations

import hashlib
from typing import Mapping

from poly_terminal.config.settings import RISK_CRITICAL_KEYS


def _normalize(env: Mapping[str, str], keys: frozenset[str]) -> list[tuple[str, str]]:
    return [(k, str(env.get(k, "")).strip()) for k in sorted(keys)]


def compute_fingerprint(env: Mapping[str, str]) -> str:
    """SHA-256 hex of sorted (key, value) pairs from RISK_CRITICAL_KEYS in `env`.

    Missing keys are treated as empty strings. Whitespace is stripped from
    values. Non-risk-critical keys are ignored.
    """
    pairs = _normalize(env, RISK_CRITICAL_KEYS)
    payload = "\n".join(f"{k}={v}" for k, v in pairs).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def drift_against(
    expected: Mapping[str, str], actual: Mapping[str, str]
) -> dict[str, dict[str, str]]:
    """Per-risk-critical-key diff between `expected` and `actual`.

    Returns `{key: {"expected": ..., "actual": ...}}` only for keys whose
    normalized values differ. An empty dict means no drift.
    """
    drift: dict[str, dict[str, str]] = {}
    for key, want in _normalize(expected, RISK_CRITICAL_KEYS):
        got = str(actual.get(key, "")).strip()
        if want != got:
            drift[key] = {"expected": want, "actual": got}
    return drift
