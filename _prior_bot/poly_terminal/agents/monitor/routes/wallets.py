"""GET /api/wallets/top — top-decile wallet snapshot."""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

router = APIRouter()


@router.get("/api/wallets/top")
async def wallets_top(
    request: Request,
    limit: int = Query(default=20, ge=1, le=200),
) -> dict[str, object]:
    state = request.app.state.monitor
    if state.wallets_repo is None:
        return {"wallets": [], "followed": []}
    scores = await state.wallets_repo.fetch_top(limit=limit)
    followed = (
        sorted(state.wallet_agent.followed_wallets)
        if state.wallet_agent is not None
        else []
    )
    return {
        "wallets": [
            {
                "wallet": s.wallet,
                "win_rate": s.win_rate,
                "avg_roi_pct": s.avg_roi_pct,
                "trades_30d": s.trades_30d,
                "median_position_usd": s.median_position_usd,
                "conviction_score": s.conviction_score,
                "last_updated": s.last_updated,
                "followed": s.wallet in (state.wallet_agent.followed_wallets if state.wallet_agent else set()),
            }
            for s in scores
        ],
        "followed": followed,
    }
