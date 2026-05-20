"""GET /api/exit_evals — recent exit-evaluation traces.

Surfaces the per-tick observability rows written by ExitAgent +
ProfitTakerAgent (deep-research-23 item #1). Used for live operator
drilldown of "why didn't this position sell?" without having to drop
into a sqlite shell.

Two endpoints:
  - GET /api/exit_evals?limit=200          → most recent N rows
  - GET /api/exit_evals/{position_id}      → all rows for one position
  - GET /api/exit_evals/summary?since=epoch → aggregate counts (decision +
                                              block_reason) since `since`
"""

from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


@router.get("/api/exit_evals")
async def recent(
    request: Request, limit: int = Query(200, ge=1, le=2000)
) -> dict[str, object]:
    repo = getattr(request.app.state.monitor, "exit_evals_repo", None)
    if repo is None:
        return {"active": False, "rows": [], "reason": "repo not wired"}
    rows = await repo.recent(limit=limit)
    return {"active": True, "count": len(rows), "rows": rows}


@router.get("/api/exit_evals/summary")
async def summary(
    request: Request,
    since: int | None = Query(
        None,
        description="Epoch seconds; default = now - 1h",
    ),
) -> dict[str, object]:
    repo = getattr(request.app.state.monitor, "exit_evals_repo", None)
    if repo is None:
        return {"active": False, "reason": "repo not wired"}
    cutoff = int(since) if since is not None else int(time.time()) - 3600
    decisions = await repo.count_by_decision_since(cutoff)
    blocks = await repo.count_by_block_reason_since(cutoff)
    return {
        "active": True,
        "since": cutoff,
        "decisions": decisions,
        "blocks": blocks,
    }


@router.get("/api/exit_evals/{position_id}")
async def for_position(
    request: Request,
    position_id: int,
    limit: int = Query(500, ge=1, le=5000),
) -> dict[str, object]:
    repo = getattr(request.app.state.monitor, "exit_evals_repo", None)
    if repo is None:
        raise HTTPException(503, "exit_evals repo not wired")
    rows = await repo.fetch_for_position(position_id, limit=limit)
    return {
        "position_id": position_id,
        "count": len(rows),
        "rows": rows,
    }
