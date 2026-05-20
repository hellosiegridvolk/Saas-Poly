"""EV gate — break-even probability + safety margin for the
Endgame Yield strategy family.

For an entry at price `p` targeting exit `t`:
    gain = t - p
    loss = p
    break_even = loss / (loss + gain) = p / t

Trade is allowed only if:
    true_p >= break_even + margin

`margin` defaults to 3% per playbook §5. The gate is a pure function;
side-effects and event wiring live in `endgame_yield.py`.
"""
from __future__ import annotations

from dataclasses import dataclass


_DEFAULT_MARGIN = 0.03


@dataclass(frozen=True)
class EVMarginConfig:
    """Safety margin above break-even — the EV gate's sensitivity.

    Higher margin → more conservative (rejects marginal trades).
    Lower margin → more aggressive (accepts knife-edge trades).
    Default 0.03 per playbook §5.
    """
    margin: float = _DEFAULT_MARGIN


@dataclass(frozen=True)
class EVGateResult:
    """Forensic-audit-friendly evaluation result.

    `passed` is the only field strategies should branch on; the rest
    is metadata for `strategy_rejections` audit rows so an operator
    can trace exactly why a candidate was admitted or blocked.
    """
    passed: bool
    entry: float
    target: float
    true_p: float
    break_even: float
    threshold: float
    margin: float
    reason: str


def break_even_probability(*, entry: float, target: float) -> float:
    """Return p* such that EV(buy at `entry`, exit at `target`) = 0.

    p* = entry / target  when target > entry (upside path);
    1.0 otherwise (no upside ⇒ unreachable break-even).

    Raises ValueError on prices outside [0, 1].
    """
    if not (0.0 <= entry <= 1.0):
        raise ValueError(f"entry must be in [0,1], got {entry!r}")
    if not (0.0 <= target <= 1.0):
        raise ValueError(f"target must be in [0,1], got {target!r}")
    if target <= entry:
        return 1.0
    return entry / target


def evaluate_ev_gate(
    *,
    entry: float,
    target: float,
    true_p: float,
    cfg: EVMarginConfig | None = None,
) -> EVGateResult:
    """Evaluate the EV margin gate — pure function, no I/O."""
    cfg = cfg or EVMarginConfig()
    be = break_even_probability(entry=entry, target=target)
    threshold = be + cfg.margin
    if be >= 1.0:
        # No upside — fail without claiming a usable threshold.
        return EVGateResult(
            passed=False,
            entry=entry, target=target, true_p=true_p,
            break_even=1.0, threshold=1.0, margin=cfg.margin,
            reason=f"target {target:.4f} <= entry {entry:.4f}: no upside",
        )
    passed = true_p + 1e-12 >= threshold
    if passed:
        reason = (
            f"true_p {true_p:.4f} >= threshold {threshold:.4f} "
            f"(break_even {be:.4f} + margin {cfg.margin:.2f})"
        )
    else:
        reason = (
            f"true_p {true_p:.4f} < threshold {threshold:.4f} "
            f"(break_even {be:.4f} + margin {cfg.margin:.2f})"
        )
    return EVGateResult(
        passed=passed,
        entry=entry, target=target, true_p=true_p,
        break_even=be, threshold=threshold, margin=cfg.margin,
        reason=reason,
    )


__all__ = [
    "EVGateResult",
    "EVMarginConfig",
    "break_even_probability",
    "evaluate_ev_gate",
]
