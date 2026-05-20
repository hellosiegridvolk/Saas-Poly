"""GET /api/strategies — per-strategy 24h fire/fill/pnl rollup."""

from __future__ import annotations

import time

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/api/strategies")
async def strategies(request: Request) -> dict[str, object]:
    state = request.app.state.monitor
    intent_counts = dict(state.strategy_intent_counts)
    if state.db is None:
        return {"strategies": [], "intent_counts": intent_counts}
    cutoff = int(time.time()) - 86_400
    by_strategy: dict[str, dict[str, float]] = {}
    async with state.db.connect() as conn:
        cur = await conn.execute(
            """
            SELECT strategy, COUNT(*), SUM(realized_pnl)
            FROM paper_fills
            WHERE filled_at > ?
            GROUP BY strategy
            """,
            (cutoff,),
        )
        async for r in cur:
            name = str(r[0])
            by_strategy[name] = {
                "fills_24h": int(r[1] or 0),
                "realized_pnl_24h": float(r[2] or 0),
            }
    rows = [
        {
            "name": name,
            "fills_24h": stats["fills_24h"],
            "realized_pnl_24h": stats["realized_pnl_24h"],
            "intents_emitted": int(intent_counts.get(name, 0)),
        }
        for name, stats in by_strategy.items()
    ]
    # Surface zero-fill strategies the harness knows about.
    for name, count in intent_counts.items():
        if name not in by_strategy:
            rows.append(
                {
                    "name": name,
                    "fills_24h": 0,
                    "realized_pnl_24h": 0.0,
                    "intents_emitted": int(count),
                }
            )
    rows.sort(key=lambda r: r["name"])
    return {"strategies": rows, "intent_counts": intent_counts}
