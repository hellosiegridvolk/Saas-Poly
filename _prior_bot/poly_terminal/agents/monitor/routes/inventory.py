"""GET /api/inventory — last DB ↔ on-chain reconciliation report.

Surfaces the boot-time InventoryReconcilerAgent result so dashboards
can see at a glance whether the DB matches on-chain. Returns a
"not active" body in PAPER / READ_ONLY (where the reconciler skips).
"""

from __future__ import annotations

from dataclasses import asdict
from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/api/inventory")
async def inventory(request: Request) -> dict[str, object]:
    state = request.app.state.monitor
    report = getattr(state, "inventory_report", None)
    if report is None:
        return {
            "active": False,
            "reason": (
                "reconciler did not run (PAPER/READ_ONLY mode, "
                "no proxy wallet, or boot incomplete)"
            ),
        }
    return {
        "active": True,
        "started_at": report.started_at,
        "finished_at": report.finished_at,
        "proxy_wallet": report.proxy_wallet,
        "summary": report.summary(),
        "db_open_positions": report.db_open_positions,
        "db_unique_tokens": report.db_unique_tokens,
        "on_chain_calls": report.on_chain_calls,
        "failed_calls": report.failed_calls,
        "matches": report.matches,
        "hard_mismatches": [asdict(m) for m in report.mismatches],
        "soft_skipped": [asdict(m) for m in report.soft_skipped],
        "notes": list(report.notes),
    }
