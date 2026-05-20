"""Inventory reconciler — DB ↔ on-chain CTF balanceOf gate.

Bug surface: 2026-05-05 canary disagreed with the Data API on whether
4 CANARY_REOPENED positions existed. The Data API returned `[]` while
the local DB had 50.76 shares booked across 4 rows. Without a trusted
inventory source, automated exits operate on possibly-fictional
shares — and the bot can issue SELLs against zero on-chain inventory.

This package fixes that by going past the Data API to the
ConditionalTokens (ERC1155) contract directly on Polygon. At every
LIVE/LIVE_DRY startup we read on-chain `balanceOf(proxy, token_id)`
for every open position in the DB, compute drift, and refuse to start
when drift exceeds a configurable threshold.

Components:
  - CTFBalanceReader: JSON-RPC `eth_call` wrapper with multi-RPC
    fallback. Mockable.
  - InventoryReconcilerAgent: orchestrator. Returns a
    ReconciliationReport. Configurable hard-fail threshold; PAPER
    mode logs the report without blocking.
"""
