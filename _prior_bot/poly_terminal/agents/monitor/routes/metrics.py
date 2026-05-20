"""GET /metrics — Prometheus exposition.

Plain-text format. Read-only — pulls counters from gate_metrics today and
latency snapshots from MonitorState.
"""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

router = APIRouter()


@router.get("/metrics")
async def metrics(request: Request) -> Response:
    state = request.app.state.monitor
    lines: list[str] = []
    lines.append("# HELP poly_uptime_seconds Process uptime in seconds.")
    lines.append("# TYPE poly_uptime_seconds gauge")
    import time

    lines.append(
        f"poly_uptime_seconds {max(0, int(time.time()) - state.started_at)}"
    )

    # Gate counters.
    if state.gate_metrics_repo is not None:
        rows = await state.gate_metrics_repo.fetch_today()
        lines.append("# HELP poly_gate_total Per-gate pass/reject counts (today, UTC).")
        lines.append("# TYPE poly_gate_total counter")
        for r in rows:
            lines.append(
                f'poly_gate_total{{gate="{r["gate_name"]}",outcome="{r["outcome"]}"}}'
                f" {r['count']}"
            )

    # Strategy intent counters.
    lines.append("# HELP poly_strategy_intents_total Intents emitted per strategy.")
    lines.append("# TYPE poly_strategy_intents_total counter")
    for name, count in state.strategy_intent_counts.items():
        lines.append(
            f'poly_strategy_intents_total{{strategy="{name}"}} {int(count)}'
        )

    # Latency budgets.
    if state.latency_budgets:
        lines.append("# HELP poly_latency_p95_ms Latency budget p95 ms per agent.")
        lines.append("# TYPE poly_latency_p95_ms gauge")
        lines.append("# HELP poly_latency_circuit_open Latency budget circuit open (1/0).")
        lines.append("# TYPE poly_latency_circuit_open gauge")
        for agent_name, summary_fn in state.latency_budgets.items():
            try:
                summary = summary_fn()
            except Exception:
                continue
            p95 = summary.get("p95_ms")
            if p95 is not None:
                lines.append(
                    f'poly_latency_p95_ms{{agent="{agent_name}"}} {float(p95)}'
                )
            lines.append(
                f'poly_latency_circuit_open{{agent="{agent_name}"}} '
                f"{1 if summary.get('is_open') else 0}"
            )

    body = "\n".join(lines) + "\n"
    return Response(content=body, media_type="text/plain; version=0.0.4")
