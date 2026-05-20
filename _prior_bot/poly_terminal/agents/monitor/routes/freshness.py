"""GET /api/freshness — exit-path freshness rollup (Phase 30(b)).

Application-level readiness signal: `live_canary_ready` is True iff
every held position has a fresh tick AND a fresh exit-eval timestamp.
Container probes (Docker HEALTHCHECK, Kubernetes readiness) cannot
express this — they only know the process is alive.

Returns the per-position breakdown so an operator dashboard can show
exactly which position is going tick-blind.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/api/freshness")
async def freshness(request: Request) -> dict[str, object]:
    state = request.app.state.monitor
    tracker = getattr(state, "freshness_tracker", None)
    if tracker is None:
        return {
            "active": False,
            "reason": (
                "freshness tracker not wired "
                "(PAPER/READ_ONLY or tests)"
            ),
        }
    snap = tracker.snapshot()
    return {"active": True, **snap}
