"""On-chain CTF (ERC1155) balanceOf reader for Polygon.

Polymarket conditional tokens live in the ConditionalTokens contract at
``0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`` (Polygon mainnet). The
balance for an outcome token uses ERC1155.balanceOf(address account,
uint256 id) → uint256.

The reader uses JSON-RPC ``eth_call`` over public endpoints with a
fallback chain — the same pattern the live-readiness gate uses for
USDC balanceOf. It is intentionally Web3-library-free so it adds zero
new top-level dependencies and stays trivially mockable for tests.

Decimals: Polymarket outcome tokens use the same 10^6 scaling as USDC.
A balance of 50_760_000 wei = 50.76 shares.

Multi-RPC fallback: public endpoints (drpc.org, publicnode) rate-limit
intermittently. The reader cycles through all configured + public
endpoints and returns from the first successful one.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass
from typing import Iterable

logger = logging.getLogger(__name__)


# Polygon mainnet ConditionalTokens contract (Polymarket outcomes).
CTF_CONTRACT_POLYGON: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# ERC1155 balanceOf(address account, uint256 id) selector — first 4
# bytes of keccak256("balanceOf(address,uint256)").
BALANCE_OF_SELECTOR: str = "0x00fdd58e"

# Polymarket outcome tokens use 10^6 scaling (matches USDC collateral).
TOKEN_DECIMALS: int = 6
TOKEN_SCALE: int = 10**TOKEN_DECIMALS


@dataclass(frozen=True)
class CTFReaderConfig:
    rpc_urls: tuple[str, ...]
    timeout_s: float = 8.0
    contract_address: str = CTF_CONTRACT_POLYGON

    def with_public_fallbacks(self) -> "CTFReaderConfig":
        """Append the same public RPCs the readiness gate uses. Idempotent."""
        defaults = (
            "https://polygon.drpc.org",
            "https://polygon.publicnode.com",
        )
        seen = {u.rstrip("/").lower() for u in self.rpc_urls}
        extras = tuple(d for d in defaults if d.rstrip("/").lower() not in seen)
        return CTFReaderConfig(
            rpc_urls=self.rpc_urls + extras,
            timeout_s=self.timeout_s,
            contract_address=self.contract_address,
        )


class CTFReadError(RuntimeError):
    """All RPC endpoints failed. The caller should treat this as
    'unknown' and refuse to gate decisions on the result."""


class CTFBalanceReader:
    """Synchronous (urllib-based) balanceOf reader. Caller wraps in
    ``asyncio.to_thread`` if it needs an async surface — the cost of
    introducing aiohttp/httpx for one shot of network I/O is not worth
    the complexity here, and matches the live-readiness gate's
    pattern."""

    def __init__(self, cfg: CTFReaderConfig) -> None:
        if not cfg.rpc_urls:
            raise ValueError("CTFReaderConfig.rpc_urls must be non-empty")
        self._cfg = cfg

    def balance_of(self, owner: str, token_id: str) -> int:
        """Return the raw on-chain balance (uint256, in 10^-6 share
        units) of `owner` for `token_id`.

        `owner` is a hex address ("0x..."). `token_id` is the decimal
        string form of the uint256 token id (Polymarket APIs return it
        this way). Raises `CTFReadError` if every endpoint failed.
        """
        owner_clean = owner.lower().removeprefix("0x").rjust(64, "0")
        token_int = int(token_id)
        token_hex = f"{token_int:064x}"
        data = BALANCE_OF_SELECTOR + owner_clean + token_hex
        body = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {"to": self._cfg.contract_address, "data": data},
                "latest",
            ],
        }).encode()

        last_err: str | None = None
        for rpc in self._cfg.rpc_urls:
            try:
                req = urllib.request.Request(
                    rpc,
                    data=body,
                    headers={
                        "Content-Type": "application/json",
                        # Public endpoints (drpc, publicnode) reject the
                        # default python-urllib UA. Match the readiness
                        # gate string so traffic looks consistent.
                        "User-Agent": (
                            "Mozilla/5.0 (poly-inventory-reconciler/1.0) "
                            "PolymarketCanary/1.0"
                        ),
                    },
                )
                with urllib.request.urlopen(
                    req, timeout=self._cfg.timeout_s
                ) as resp:
                    j = json.loads(resp.read())
            except Exception as exc:
                last_err = f"{rpc}: {type(exc).__name__}: {exc}"
                logger.debug("CTFBalanceReader: %s", last_err)
                continue
            if "result" not in j:
                last_err = f"{rpc}: rpc_error: {j.get('error', j)!r}"
                logger.debug("CTFBalanceReader: %s", last_err)
                continue
            try:
                return int(j["result"], 16)
            except (TypeError, ValueError) as exc:
                last_err = f"{rpc}: malformed_result: {j['result']!r} ({exc})"
                logger.debug("CTFBalanceReader: %s", last_err)
                continue
        raise CTFReadError(
            f"all {len(self._cfg.rpc_urls)} RPC endpoints failed; "
            f"last_err={last_err}"
        )

    def shares_of(self, owner: str, token_id: str) -> float:
        """Convenience: returns the on-chain balance scaled to human
        shares (divided by 10^6). Polymarket outcome tokens use USDC's
        10^6 scaling so 50_760_000 raw == 50.76 shares."""
        raw = self.balance_of(owner, token_id)
        return raw / TOKEN_SCALE


def reader_from_settings(rpc_primary: str, rpc_fallback: str = "") -> CTFBalanceReader:
    """Build a reader from the same env vars the rest of the bot uses.

    Empty / whitespace strings are stripped. Public fallbacks are
    appended automatically so a degraded private RPC doesn't cause a
    hard fail — the readiness gate observed drpc/publicnode flapping
    during the canary window.
    """
    urls: list[str] = []
    for v in (rpc_primary, rpc_fallback):
        v = (v or "").strip()
        if v:
            urls.append(v)
    cfg = CTFReaderConfig(rpc_urls=tuple(urls)).with_public_fallbacks()
    return CTFBalanceReader(cfg)
