# Saas-Poly

Polymarket Trading Infrastructure Platform — multi-tenant SaaS that runs
hosted automated strategies against Polymarket's CLOB. Non-custodial,
execution-first.

See `CLAUDE.md` for the read order, invariants, and decisions log entry
points. The authoritative build spec lives outside the repo and is
referenced from `CLAUDE.md`.

## Status

**Phase 0 — Bootstrap.** Monorepo layout, dev compose stack, Alembic +
users table, domain skeleton, CI. No strategy or execution code yet.

## Quick start (dev)

```sh
cp .env.example .env
pip install -e ".[dev]"
docker compose -f infra/compose/docker-compose.dev.yml up
```

Then `curl http://localhost:8000/healthz`.

## Running checks

```sh
ruff check .
mypy
pytest -q
```
