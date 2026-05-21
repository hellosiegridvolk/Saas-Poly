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

## Active PRs (stacked, in merge order)

| #  | Title                                          | Branch                                          | Status                |
| -- | ---------------------------------------------- | ----------------------------------------------- | --------------------- |
| #2 | PR A — foundation (schema, repos, primitives)  | `claude/phase-1-ninety-cent-mvp`                | Ready for review      |
| #3 | PR B — risk engine + gates 1–18                | `claude/phase-1-pr-b-risk-engine`               | Draft, CI green       |
| #4 | PR C — paper exec + reconciler + event bus     | `claude/phase-1-pr-c-paper-exec-reconciler`     | Draft, CI green       |

Merge order: PR A → PR B → PR C → main. Each retargets to `main` after the
previous one merges.

---

## Next planned work (in build order)

- [ ] **PR D — repository + bus adapters** wiring the in-memory `EventBus`
      and SQLAlchemy repos to the engines built in PRs A–C. No new logic; pure
      glue. Unblocks the integration test.
- [ ] **PR E — `services/marketdata/`** WebSocket subscriber for a hardcoded
      market list; publishes to `market.tick` and `market.book` streams (spec
      §15 step 4).
- [ ] **PR F — `strategies/ninety_cent/`** port (spec §15 step 9). **Blocks
      on `_prior_bot/` upload — see below.**
- [ ] **PR G — end-to-end paper-mode integration test** (spec §15 step 11):
      driven through the strategy worker, demonstrates signal → fill →
      reconciled position. Phase-1 DoD gate.
- [ ] **PR H — Clerk auth on `apps/api`** (spec §15 step 1) and protected
      routes for read-side queries.
- [ ] **PR I — `apps/frontend/`** Next.js dashboard + strategies catalog +
      instance creation + WS-pushed live fills (spec §15 step 10).

---

## Blocked (re-checked every 4 hours by the cron workflow)

- **`_prior_bot/` not uploaded.** The directory is excluded from lint/test
  but is empty. Required for:
  - PR F (`ninety_cent` port — strategy logic + signal generation).
  - Bodies of `shared/risk_primitives/{tick_confirmation, percent_sltp,
    gamma_slug_builder}.py` (currently `raise NotImplementedError("port from
    _prior_bot/")`).
- **Spec not committed to repo.** Lives only in the upload area; if it
  expires Claude must re-request from Nicola before resuming spec-bound work.

---

## Done

- [x] PR #1 — Phase 0 bootstrap. Monorepo layout, pyproject, docker compose,
      Alembic with users table, domain skeleton, CI on Python 3.11 & 3.12,
      `CLAUDE.md`, ADRs 0001 + 0002.
