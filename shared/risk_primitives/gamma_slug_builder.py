"""GammaSlugBuilder — builds Polymarket Gamma API slugs in the
unix-timestamp format the scalp strategies expect (spec §12.2 scalp_15m
fix, §16 operational lesson).

Public interface only. Implementation ported from ``_prior_bot/`` in a
follow-up PR.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GammaSlugBuilder:
    """Stateless slug builder."""

    def build(self, base_slug: str, end_ts_unix: int) -> str:
        """Return the canonical Gamma slug ``<base_slug>-<end_ts_unix>``.

        ``end_ts_unix`` is seconds since epoch in UTC (spec §3.8 — never
        local TZ). Using any other format silently misses markets — see
        spec §16 "Slug builder for Gamma scalp markets".
        """
        raise NotImplementedError("port from _prior_bot/")
