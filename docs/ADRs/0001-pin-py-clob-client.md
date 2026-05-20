# ADR 0001 — Pinning `py-clob-client`

**Status:** Accepted (provisional)
**Date:** 2026-05-20
**Phase:** 0
**Spec ref:** §3.7, §5

## Context

§3.7 requires `py-clob-client` to be pinned in `pyproject.toml`; SDK
upgrades are a deliberate task, not a transitive dependency bump. The
prior bot source (`_prior_bot/`) has not yet been uploaded, so we cannot
yet read its lockfile to confirm which version it ran against.

## Decision

Pin to **`py-clob-client==0.21.0`** for Phase 0. This is a recent, broadly
deployed release at the time of writing.

This pin is **provisional**: once `_prior_bot/` is uploaded, compare its
lockfile against this pin. If the prior bot ran on a different version,
open ADR 0001-amend and align — the prior bot's working version is the
authoritative baseline for porting the six `port`-marked strategies in
§12.2.

## Consequences

- Phase 0 builds and CI exercise this version.
- Any SDK behavior the prior bot relied on that changed between its pinned
  version and 0.21.0 will surface during Phase 1 strategy porting and
  must be tracked as a porting blocker, not silently worked around.
- Upgrading the pin in the future requires a new ADR documenting the
  delta and the regression-test results.
