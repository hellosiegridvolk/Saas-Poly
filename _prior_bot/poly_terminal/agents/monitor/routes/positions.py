"""GET /api/positions — open positions with current PnL placeholder."""

from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/api/positions")
async def positions(request: Request) -> dict[str, object]:
    state = request.app.state.monitor
    if state.db is None:
        return {"positions": [], "open_count": 0}
    rows: list[dict[str, object]] = []
    async with state.db.connect() as conn:
        cur = await conn.execute(
            """
            SELECT position_id, market_id, token_id, side,
                   entry_price, shares, cost_basis_usd,
                   entry_intent_id, entry_ts, closed_ts,
                   exit_price, realized_pnl, outcome
            FROM positions
            ORDER BY entry_ts DESC
            LIMIT 200
            """
        )
        async for r in cur:
            rows.append(
                {
                    "position_id": r[0],
                    "market_id": r[1],
                    "token_id": r[2],
                    "side": r[3],
                    "entry_price": r[4],
                    "shares": r[5],
                    "cost_basis_usd": r[6],
                    "entry_intent_id": r[7],
                    "entry_ts": r[8],
                    "closed_ts": r[9],
                    "exit_price": r[10],
                    "realized_pnl": r[11],
                    "outcome": r[12],
                    "is_open": r[9] is None,
                }
            )
    open_count = sum(1 for x in rows if x["is_open"])
    return {"positions": rows, "open_count": open_count}
