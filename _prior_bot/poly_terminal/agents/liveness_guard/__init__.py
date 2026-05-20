"""LivenessGuard — gates new BUY entries when the live exit path is
degraded (deep-research-23 item #3).

The 2026-05-05 canary surfaced a layered failure mode: the bot was
running, intents were firing, but the exit path was effectively dead
(WS silent, recorder asleep, ticks stale). Without a single
"live_exit_ready" boolean, RiskAgent has no way to reject new entries
under these conditions.

This package introduces:
  - LivenessGuardAgent: subscribes to EVT_MARKET_TICK + EVT_AGENT_HEARTBEAT
    and derives a `live_exit_ready` flag with reason strings on miss.
  - LivenessGate: the risk-pipeline gate that consults the agent.
    PAPER / READ_ONLY skip cleanly. LIVE / LIVE_DRY / CLOSE_ONLY enforce.

The agent is intentionally bus-native — it doesn't need WebSocket
internals or recorder DB writes; it just observes the events that
those subsystems already publish. Adding new signals later (e.g.
recorder snapshot heartbeat, profit_taker tick observed) is a
matter of subscribing to one more event.
"""
