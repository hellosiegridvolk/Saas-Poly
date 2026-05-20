# CLAUDE.md — Saas-Poly

Authoritative build spec: `docs/POLYMARKET_PLATFORM_SPEC.md` (upload-only;
not in repo by default — Nicola pastes it into the session).

If the spec is not in the working tree, ask Nicola for it before writing
code. Do not infer requirements from this file alone.

---

## Read order (every session)

1. The full build spec (sections 1 → 22).
2. This file.
3. `docs/ADRs/` — every accepted ADR is binding.

---

## Non-negotiable invariants (spec §3 — short form)

Violating any of these is a build failure, not a code-review nit.

- **Execution flow:** Strategy → Signal → Risk Engine → Execution Engine → CLOB.
  Strategies never touch the SDK, the execution engine, or a wallet.
- **Money math:** `decimal.Decimal` always. `ROUND_DOWN` for price/size.
  USDC integer / `1_000_000` once at the boundary.
- **Concurrency:** one asyncio loop per process. Blocking SDK calls go
  through `asyncio.to_thread`. No `requests`/sync HTTP in async paths.
- **Order heartbeat:** GTC orders need a 5–10s heartbeat. Miss = self-fence
  (cancel all, refuse new intents, alert).
- **Reconciliation is truth.** In-memory caches are advisory. Reconciler
  wins on disagreement.
- **Multi-tenancy:** every row has `user_id`; every query filters by
  `user_id` at the repository layer. No raw SQL bypass.
- **Runtime:** Python 3.11 or 3.12 only. **3.13 breaks `ckzg`.**
- **Non-custodial only.** We never hold funds, never store wallet private
  keys. CLOB API credentials only.
- **Latency SLO:** p99 **signal → submit** ≤ 400ms. Signal → fill is
  observational, not an SLO.

## Anti-patterns (spec §17 — do NOT do)

If a proposed implementation matches one of these, stop and ask Nicola:

- Strategy class with a reference to the execution client.
- Float arithmetic near money.
- Sync HTTP in async coroutines.
- Global mutable position dict read by strategies.
- Heartbeat as `while True: time.sleep(5)`.
- Bare `except Exception` with no structured context.
- Repository query missing `user_id` filter.
- Hardcoded test user_id / wallet left in the codebase.
- Plain-text secrets in the DB.
- Retry loop with no backoff/cap.
- Tests hitting live Polymarket endpoints.

Full list: spec §17.

---

## Decisions D1–D7 (spec §22 — binding for v1)

| ID | Decision |
|----|----------|
| D1 | Non-custodial only. No platform custody, no pooling, no withdrawal authority. |
| D2 | v1 arbitrage = external-exchange probability arb + near-resolution decay arb only. Cross-market / synthetic / resolution arb deferred. |
| D3 | Latency SLO = p99 **signal → submit** ≤ 400ms. Fill latency is observational. |
| D4 | Auth = Clerk. Auth0 reserved for institutional v2. |
| D5 | Geo-block US + OFAC + UK + CA + FR + DE. No KYC at v1 beyond verified email + jurisdiction self-attestation + OFAC SDN screen on linked wallet. |
| D6 | UI-only API in v1. Read API in v2. Full developer platform in v3. |
| D7 | CI = GitHub Actions. |

Violating one of these without an ADR is forbidden.

---

## Phase order (spec §15)

Build strictly in this order. No phase starts until the previous DoD is
signed off.

- **Phase 0 — Bootstrap.** Monorepo layout, pyproject, compose, Alembic +
  users table, domain skeleton, CI, this file. **Current phase.**
- **Phase 1 — Single-user paper-mode MVP.** End-to-end `ninety_cent`.
- **Phase 2 — Multi-tenant + canary live + remaining seven strategies.**
- **Phase 3 — Production hardening + scale.**

---

## Repository layout (spec §6)

Implemented at the repo root (the spec's `platform/` = this repo).

```
apps/        # api (FastAPI), frontend (Next.js — Phase 1+)
services/    # scanner, risk_engine, execution, reconciliation, marketdata,
             # redeemer, notifier, billing — fleshed out per phase
strategies/  # _base + six built-ins (port from _prior_bot/) + arbitrage (new)
shared/      # domain, db, events, polymarket, risk_primitives, telemetry, config
infra/       # docker, compose, kubernetes (Phase 3), terraform (Phase 3), config
tests/       # unit, integration, e2e — mirror source tree
docs/ADRs/   # one markdown per architectural decision
_prior_bot/  # reference implementations for `port`-marked strategies
```

`_prior_bot/` is excluded from ruff, mypy, and pytest discovery. Do not
import from it at runtime; reference only.

---

## Conventions

- Domain models live in `shared/domain/`. Services and strategies import,
  never redefine.
- Strategies depend only on `shared/domain`, `shared/risk_primitives`,
  and read-only helpers in `shared/polymarket`. Importing
  `services/execution`, `services/risk_engine`, or any DB module from a
  strategy is forbidden.
- Tests mirror source paths (`tests/unit/strategies/test_copy_trade.py`
  ↔ `strategies/copy_trade/`).
- TDD for new code (spec §18). Failing test first.
- No live-endpoint tests in CI. Fixtures or SDK mocks only.

---

## Tooling

- `pip install -e ".[dev]"` — install runtime + dev deps.
- `docker compose -f infra/compose/docker-compose.dev.yml up` — bring up
  Postgres + Redis + MailHog + API.
- `alembic upgrade head` — apply migrations.
- `ruff check .` — lint.
- `mypy` — typecheck (`shared/`, `services/`, `apps/`).
- `pytest -q` — tests.

CI runs all four on Python 3.11 and 3.12.
