"""GET /api/redemption — current redemption-queue snapshot.

Surfaces the RedeemerAgent's last-sweep stats plus a per-position list
of REDEEMABLE positions the operator needs to settle on the
Polymarket UI. Returns 200 even when the agent is disabled (e.g.
READ_ONLY mode) — body just reflects "agent inactive" so dashboards
don't error out on boot.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/api/redemption")
async def redemption(request: Request) -> dict[str, object]:
    state = request.app.state.monitor
    agent = getattr(state, "redeemer_agent", None)
    if agent is None:
        return {
            "active": False,
            "reason": "redeemer disabled (READ_ONLY mode or not wired)",
        }
    stats = agent.stats
    payload: dict[str, object] = {
        "active": True,
        "sweeps": stats.sweeps,
        "last_sweep_ts": stats.last_sweep_ts,
        "pending": stats.pending,
        "paper_skipped_total": stats.paper_skipped,
        "worthless_marked_total": stats.worthless_marked,
        "redeemable_count": stats.redeemable_count,
        "redeemable_usd": stats.redeemable_usd,
        "errors": stats.errors,
        "redeem_url": "https://polymarket.com/portfolio",
    }
    # Include the per-position redeemable list when there's something to
    # settle, so dashboards can render a one-click table. Capped at 50 to
    # bound response size on a stale queue.
    if state.positions_repo is not None and stats.redeemable_count > 0:
        rows = await state.positions_repo.fetch_closed_unredeemed()
        payload["redeemable"] = [
            {
                "position_id": int(r["position_id"]),
                "market_id": str(r["market_id"]),
                "token_id": str(r["token_id"]),
                "shares": float(r["shares"]),
                "closed_ts": int(r["closed_ts"]),
            }
            for r in rows[:50]
        ]
    return payload
