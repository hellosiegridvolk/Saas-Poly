"""Monitor Agent — FastAPI on loopback only.

Read-only surface. The bot itself runs the agents; this app exposes them
for the operator dashboard, scrapers, and external scripts.
"""

from __future__ import annotations

import time

from fastapi import FastAPI

from poly_terminal.agents.monitor.routes import (
    exit_evals,
    exit_preview,
    freshness,
    health,
    inventory,
    metrics,
    positions,
    redemption,
    strategies,
    wallets,
)
from poly_terminal.agents.monitor.state import MonitorState


def build_app(state: MonitorState | None = None) -> FastAPI:
    """Return a FastAPI app with all routes mounted.

    Pass an existing `MonitorState` to wire production agents; tests can
    pass a freshly-constructed one with mocked attributes.
    """
    app = FastAPI(title="Poly Terminal Final monitor", version="0.1.0")
    monitor_state = state or MonitorState()
    if monitor_state.started_at == 0:
        monitor_state.started_at = int(time.time())
    app.state.monitor = monitor_state
    app.include_router(health.router)
    app.include_router(positions.router)
    app.include_router(strategies.router)
    app.include_router(wallets.router)
    app.include_router(exit_preview.router)
    app.include_router(exit_evals.router)
    app.include_router(inventory.router)
    app.include_router(metrics.router)
    app.include_router(redemption.router)
    app.include_router(freshness.router)
    return app
