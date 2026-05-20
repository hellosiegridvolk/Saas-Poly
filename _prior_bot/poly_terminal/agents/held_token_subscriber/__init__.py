"""HeldTokenSubscriberAgent — auto-subscribe held tokens on the market WS.

Deep-research-23 item #4 (companion to the best_bid_ask dispatcher
support in `data/websocket/market.py`).

Context: the recorder + bot share one MarketWebSocket subscribed to a
fixed set of "tokens of interest" (recorder_tokens.txt). When the bot
opens a position on a token NOT in that set, the WS feed for that
token is silent — TickPoller's REST fallback covers it but at 5s
intervals, far too slow for short-expiry markets.

This agent ensures every held token has a live WS subscription:
  - On EVT_POSITION_OPENED: call ws.subscribe_tokens([token_id])
  - On EVT_POSITION_CLOSED: leave subscribed (cheap, and other
    positions on the same token may exist). Operator-driven cleanup
    or a periodic reaper can prune stale subscriptions later.

The subscribe is idempotent — SubscriptionManager dedupes against
already-subscribed + already-pending so re-publishing the same
position event does no harm.
"""
