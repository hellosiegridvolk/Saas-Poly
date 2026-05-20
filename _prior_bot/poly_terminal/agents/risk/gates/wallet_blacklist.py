"""Gate 1b: source-wallet blacklist."""

from __future__ import annotations

from poly_terminal.shared.typed_reject import Reject


class WalletBlacklistGate:
    def __init__(self, blacklist: set[str]) -> None:
        self._blacklist = {w.lower() for w in blacklist}

    async def __call__(self, intent: object) -> Reject | None:
        wallet = getattr(intent, "source_wallet", None)
        if wallet is None:
            return None
        if str(wallet).lower() in self._blacklist:
            return Reject(code="wallet_blacklisted", detail=str(wallet))
        return None

    def update(self, blacklist: set[str]) -> None:
        self._blacklist = {w.lower() for w in blacklist}
