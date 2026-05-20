"""GET /health — agent up/down + config fingerprint."""

from __future__ import annotations

import time

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> dict[str, object]:
    state = request.app.state.monitor
    now = int(time.time())
    agents = {
        name: {"last_heartbeat": ts, "stale": (now - ts) > 30}
        for name, ts in state.agent_heartbeat.items()
    }
    all_ok = (
        bool(state.config_fingerprint)
        and not any(a["stale"] for a in agents.values())
    )
    return {
        "ok": all_ok,
        "bot_mode": state.bot_mode,
        "config_fingerprint": state.config_fingerprint,
        "agents": agents,
        "uptime_s": now - state.started_at if state.started_at else 0,
    }
