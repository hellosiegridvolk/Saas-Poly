"""GET /api/exit/preview — what ExitAgent would decide on next tick."""

from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query, Request

from poly_terminal.agents.exit.decision_engine import ExitDecisionEngine

router = APIRouter()


@router.get("/api/exit/preview")
async def exit_preview(
    request: Request,
    position_id: int = Query(..., ge=1),
    price: float = Query(..., gt=0),
    now_ts: float | None = Query(default=None),
) -> dict[str, object]:
    state = request.app.state.monitor
    if state.exit_agent is None:
        raise HTTPException(503, "exit_agent not wired")
    pos = state.exit_agent._positions.get(position_id)  # type: ignore[attr-defined]
    cfg = state.exit_agent._configs.get(position_id)    # type: ignore[attr-defined]
    if pos is None or cfg is None:
        raise HTTPException(404, "position not tracked")
    import time

    ts = now_ts if now_ts is not None else float(time.time())
    # Use a throwaway engine instance so we don't mutate the live one.
    decision = ExitDecisionEngine().evaluate(
        pos=pos, current_price=Decimal(str(price)), cfg=cfg, now_ts=ts
    )
    return {
        "position_id": position_id,
        "entry_price": str(pos.entry_price),
        "shares": str(pos.shares),
        "preview_price": price,
        "decision": decision.value,
        "config": {
            "sl_pct": str(cfg.sl_pct),
            "tp_pct": str(cfg.tp_pct),
            "sl_floor_usd": str(cfg.sl_floor_usd),
            "adverse_ticks_required": cfg.adverse_ticks_required,
            "max_hold_seconds": cfg.max_hold_seconds,
        },
    }
