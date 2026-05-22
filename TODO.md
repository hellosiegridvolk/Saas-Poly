# Saas-Poly progress tracker

> Single source of truth for what's done, in-flight, and blocked across Phase 1.
> The SessionStart hook (`.claude/settings.json`) surfaces this on every new
> Claude Code session; the `.github/workflows/track-progress.yml` cron mirrors
> it into a sticky tracking issue every 4 hours.
>
> **Spec:** `docs/POLYMARKET_PLATFORM_SPEC.md` (upload-only — Nicola pastes it
> into the session if Claude needs it).

---

## Current phase

**Phase 1 — Single-user paper-mode MVP** (spec §15).
DoD: seeded user creates a `ninety_cent` paper instance; signal → risk → paper
fill → reconciled position → dashboard within 1s; restart-safe; §3 invariants
verified by tests.

---

## Active PRs

| #  | Title                                                | Branch                        | Status          |
| -- | ---------------------------------------------------- | ----------------------------- | --------------- |
| #5 | automation tracker — SessionStart hook + cron + TODO | `claude/automation-tracker`   | Draft, CI green |

Foundation merge train (PRs A → B → C) landed on `main`; see **Done**
below. PR D is the next implementation PR — not yet opened.

---

## Next planned work (in build order)

- [ ] **PR D — repository + bus adapters** wiring the in-memory `EventBus`
      and SQLAlchemy repos to the engines landed in PRs A–C. No new logic;
      pure glue. Unblocks the integration test. **Next up.**
- [ ] **PR E — `services/marketdata/`** WebSocket subscriber for a hardcoded
      market list; publishes to `market.tick` and `market.book` streams (spec
      §15 step 4).
- [ ] **PR F — `strategies/ninety_cent/`** port (spec §15 step 9).
      **PARKED — needs `_prior_bot/` upload.** Decision: skip, do not
      build from scratch (see Blocked). Build order routes around it:
      PRs D, E, H, I run first.
- [ ] **PR G — end-to-end paper-mode integration test** (spec §15 step 11):
      driven through the strategy worker, demonstrates signal → fill →
      reconciled position. Phase-1 DoD gate.
- [ ] **PR H — Clerk auth on `apps/api`** (spec §15 step 1) and protected
      routes for read-side queries.
- [ ] **PR I — `apps/frontend/`** Next.js dashboard + strategies catalog +
      instance creation + WS-pushed live fills (spec §15 step 10).

---

## Blocked / parked (re-checked every 4 hours by the cron workflow)

- **`_prior_bot/` not uploaded — PR F parked.** The directory is excluded
  from lint/test but is empty. **Decision (2026-05-22): skip, do NOT build
  from scratch** — CLAUDE.md §17 and "do not infer requirements" forbid
  inventing spec-bound trading logic. Stays parked until the reference
  code is uploaded:
  - PR F (`ninety_cent` port — strategy logic + signal generation).
  - Bodies of `shared/risk_primitives/{tick_confirmation, percent_sltp,
    gamma_slug_builder}.py` (landed as `NotImplementedError("port from
    _prior_bot/")` stubs in PR A — interfaces are public, only the
    algorithms remain).
- **Spec not committed to repo.** Lives only in the upload area.
  **Decision (2026-05-22): proceed without it** on non-spec-bound PRs
  (D pure glue, H Clerk auth); pause and re-request from Nicola before
  any spec-bound decision.

---

## Done

- [x] **PR #4 — PR C: paper-mode execution + reconciler + in-memory event
      bus.** `shared/events` EventBus Protocol + bounded `asyncio.Queue`
      `InMemoryEventBus`; `services/execution` `simulate_fill` +
      `PaperExecutionEngine` + `HeartbeatCoroutine`;
      `services/reconciliation` eight-case position math +
      `apply_fill_to_balance` + `ReconciliationEngine` over
      `PositionStore` / `BalanceStore` Protocols. Squashed to `main` at
      `0e27d8f`.
- [x] **PR #3 — PR B: risk engine + gates 1–18 + telemetry.** `RiskEngine`
      orchestrator with fail-fast gate sequencing + per-gate
      `duration_ms`; `shared/telemetry` Metrics Protocol +
      `record_gate_decision`; 18 gates across identity/state, market
      freshness, pricing sanity, sizing/exposure. Gates 19–25 deferred to
      Phase 2; engine takes `Sequence[Gate]` so they slot in without API
      change. Squashed to `main` at `a8d9aee`.
- [x] **PR #2 — PR A: foundation.** Full v1 schema (migration `0002`,
      15 tables, append-only triggers, per-user query indexes);
      `UserScopedRepository` base + 3 exemplar repos with a contract
      test for the `user_id` first-arg rule; `shared/polymarket` helpers
      (`decimal_helpers`, async `GammaClient`); `strategies/_base`
      Protocols; `shared/risk_primitives` public interfaces (bodies =
      port-from-prior-bot stubs). Squashed to `main` at `e9f8fdb`.
- [x] **PR #1 — Phase 0 bootstrap.** Monorepo layout, pyproject, docker
      compose, Alembic with users table, domain skeleton, CI on Python
      3.11 & 3.12, `CLAUDE.md`, ADRs 0001 + 0002.
