# ADR 0002 — Dev-time secrets backend

**Status:** Accepted
**Date:** 2026-05-20
**Phase:** 0
**Spec ref:** §5, §20.1

## Context

§5 lists "Doppler or AWS Secrets Manager" for secrets. Phase 0 needs a
working dev loop without standing up a paid secrets backend. Per §20.1,
envelope encryption of per-user secrets only lands in Phase 2.

Free-tier options surveyed:

- **Doppler — Developer plan (free).** Hosted, CLI-driven injection,
  per-environment configs. Fits the SaaS workflow assumed by the spec.
- **AWS Secrets Manager.** Not free; $0.40/secret/month + API calls. Out
  for dev.
- **HashiCorp Vault dev-mode.** Free, self-hosted, but adds an
  always-on local dep with no production parity.
- **SOPS + age.** Free, file-based, but no hosted state — awkward for
  multi-machine dev.

## Decision

For **Phase 0 dev**: plain `.env` files driven by `pydantic-settings`,
gitignored, with `.env.example` checked in. No secrets backend is
required to bring up the compose stack.

For **Phase 1 onwards**: integrate **Doppler (Developer plan, free)** as
the dev/staging secrets backend. Per-user envelope-encrypted secrets in
the `user_secrets` table land in Phase 2.

For **production** (Phase 3): revisit. Doppler Team paid tier or AWS
Secrets Manager, decided in a Phase-3 ADR.

## Consequences

- Phase 0 ships with no secret material in the repo and no external
  dependency for local dev.
- Anyone running the dev stack copies `.env.example` to `.env` and edits.
- The Doppler integration is deferred work, not a Phase 0 deliverable.
