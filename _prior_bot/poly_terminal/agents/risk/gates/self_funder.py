"""Gate 13: self-funder block — refuse copying our own funder address."""

from __future__ import annotations

from poly_terminal.shared.typed_reject import Reject


class SelfFunderGate:
    def __init__(self, our_addresses: set[str]) -> None:
        self._ours = {a.lower() for a in our_addresses}

    async def __call__(self, intent: object) -> Reject | None:
        wallet = getattr(intent, "source_wallet", None)
        if wallet is None:
            return None
        if str(wallet).lower() in self._ours:
            return Reject(code="self_funder_block", detail=str(wallet))
        return None

    def update(self, our_addresses: set[str]) -> None:
        self._ours = {a.lower() for a in our_addresses}
