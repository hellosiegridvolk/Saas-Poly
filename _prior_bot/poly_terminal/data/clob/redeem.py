"""RelayerRedeemer — auto-redeem resolved Polymarket positions.

Implements the on-chain redemption flow that py-clob-client v2 still
doesn't expose (GitHub issue #139, open since 2024). Adapted from the
Robot Traders pattern documented at:
    https://robottraders.io/blog/polymarket-auto-redeem-python

Two contract paths, identified per-position by the Data API's
`negativeRisk` boolean:

  1. STANDARD market → ConditionalTokens.redeemPositions(
         collateral=USDC, parentCollectionId=0x00..00,
         conditionId, indexSets=[1, 2])
     Hands back whichever outcome the wallet held; index sets [1,2]
     cover both outcomes of a binary so the call works regardless
     of which side won.

  2. NEG_RISK market → NegRiskAdapter.redeemPositions(
         conditionId, amounts=[a0, a1])
     where amounts[outcomeIndex] = position_size × 1e6 (USDC's 6
     decimals) and the other slot is 0.

Both calls are wrapped in a `SafeTransaction(operation=Call)` and
submitted via Polymarket's relayer (`https://relayer-v2.polymarket.com`).
The relayer covers gas, but requires:
  - Magic Link (signature_type=1) → RelayerTxType.PROXY
  - Browser/Safe (signature_type=0|2) → RelayerTxType.SAFE
  - Builder API key/secret/passphrase to authenticate the request

Safety surface:
  - `dry_run=True` builds and pretty-prints the calldata WITHOUT
    importing the relayer client or making any network call. This
    is the default for every new agent instance — ops must opt-in
    to live submission explicitly.
  - On submission, returns the on-chain tx hash. The caller
    (RedeemerAgent) is responsible for persisting that hash via
    `mark_redeemed` so a restart doesn't double-submit.
  - The relayer client is imported lazily so unit tests can run
    without `py_builder_relayer_client` installed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from eth_abi import encode as eth_abi_encode  # type: ignore[attr-defined]
from eth_utils import keccak  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


# ── Polygon mainnet contract addresses ──────────────────────────────────
USDC_ADDRESS: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER_ADDRESS: str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# ── Function selectors (first 4 bytes of keccak256) ─────────────────────
# Standard CTF redeem: redeemPositions(address,bytes32,bytes32,uint256[])
REDEEM_SELECTOR: bytes = keccak(
    text="redeemPositions(address,bytes32,bytes32,uint256[])"
)[:4]
# Neg-risk redeem: redeemPositions(bytes32,uint256[])
NEG_RISK_REDEEM_SELECTOR: bytes = keccak(
    text="redeemPositions(bytes32,uint256[])"
)[:4]

# ── Relayer endpoint (V2) ───────────────────────────────────────────────
RELAYER_URL: str = "https://relayer-v2.polymarket.com"
POLYGON_CHAIN_ID: int = 137

# ── Sentinel returned from a dry-run submit so the caller can clearly
#    distinguish "we calculated the call but didn't send it" from a real
#    on-chain hash. Persisted into positions.redeem_tx_hash to make the
#    audit trail self-explanatory.
DRY_RUN_TX_HASH_PREFIX: str = "DRY_RUN_REDEEM:"


@dataclass(frozen=True)
class RedeemCallData:
    """Captures what would be sent to the relayer for a single
    redemption — independent of whether we actually submit. Used by
    dry-run output and persisted in audit logs."""

    # to: contract address (CTF or NEG_RISK_ADAPTER)
    to_address: str
    # data: hex-encoded call (selector + ABI-encoded args), '0x'-prefixed
    data_hex: str
    # value: always "0" for redemption (we receive USDC, not pay it)
    value: str
    # human-readable: which contract path (regular vs neg-risk)
    market_type: str
    # the condition_id this redeems (0x-prefixed)
    condition_id: str

    def short_summary(self) -> str:
        return (
            f"{self.market_type} redeem to={self.to_address[:10]}... "
            f"cid={self.condition_id[:12]}... data_len={len(self.data_hex)//2}B"
        )


def build_standard_redeem_calldata(condition_id: str) -> RedeemCallData:
    """Build calldata for a standard (non-neg-risk) market redemption.

    The CTF call:
        redeemPositions(USDC, bytes32(0), conditionId, [1, 2])

    indexSets=[1, 2] are the two outcomes of a binary market. The CTF
    will pay out the wallet's holdings on whichever index won; the
    other contributes 0. So this single call drains a binary position
    regardless of which outcome resolved YES.
    """
    cid = _normalize_hex32(condition_id)
    args = eth_abi_encode(
        ["address", "bytes32", "bytes32", "uint256[]"],
        [
            USDC_ADDRESS,
            b"\x00" * 32,                # parentCollectionId
            bytes.fromhex(cid[2:]),      # conditionId
            [1, 2],                      # both indexSets
        ],
    )
    data_hex = "0x" + (REDEEM_SELECTOR + args).hex()
    return RedeemCallData(
        to_address=CTF_ADDRESS,
        data_hex=data_hex,
        value="0",
        market_type="standard",
        condition_id=cid,
    )


def build_neg_risk_redeem_calldata(
    *,
    condition_id: str,
    outcome_index: int,
    size: float,
) -> RedeemCallData:
    """Build calldata for a neg-risk market redemption.

    The adapter call:
        NegRiskAdapter.redeemPositions(conditionId, amounts)

    amounts is a 2-slot array where the non-zero slot corresponds to
    the wallet's outcome holding (in 6-decimal USDC base units —
    which equals share count × 1e6 because Polymarket shares are
    1:1 with USDC).
    """
    if outcome_index not in (0, 1):
        raise ValueError(
            f"outcome_index must be 0 or 1, got {outcome_index!r}"
        )
    if size <= 0:
        raise ValueError(f"size must be positive, got {size!r}")
    cid = _normalize_hex32(condition_id)
    size_raw = int(size * 1e6)
    amounts = [0, 0]
    amounts[outcome_index] = size_raw
    args = eth_abi_encode(
        ["bytes32", "uint256[]"],
        [bytes.fromhex(cid[2:]), amounts],
    )
    data_hex = "0x" + (NEG_RISK_REDEEM_SELECTOR + args).hex()
    return RedeemCallData(
        to_address=NEG_RISK_ADAPTER_ADDRESS,
        data_hex=data_hex,
        value="0",
        market_type="neg_risk",
        condition_id=cid,
    )


def _normalize_hex32(value: str) -> str:
    """Return value as a `0x`-prefixed 32-byte hex string (66 chars).
    Raises ValueError on malformed input."""
    if not isinstance(value, str):
        raise ValueError(f"hex32 must be a string, got {type(value)!r}")
    v = value.strip().lower()
    if v.startswith("0x"):
        v = v[2:]
    if len(v) != 64:
        raise ValueError(
            f"hex32 must be 64 hex chars (32 bytes), got {len(v)}: {value!r}"
        )
    int(v, 16)  # validates hex
    return "0x" + v


@dataclass(frozen=True)
class RelayerCreds:
    """Minimum surface to authenticate a relayer call. All five
    fields are required for live submission; dry-run mode validates
    only that they're non-empty so misconfiguration is caught early."""

    private_key: str        # L1 EOA key (POLY_PRIVATE_KEY in .env)
    funder_address: str     # Polymarket proxy (POLY_PROXY_ADDRESS)
    signature_type: int     # 1 = Magic/email proxy, 0|2 = Safe
    builder_api_key: str    # POLY_API_KEY
    builder_secret: str     # POLY_API_SECRET
    builder_passphrase: str # POLY_API_PASSPHRASE


class RelayerRedeemer:
    """Builds + (optionally) submits Polymarket position redemptions.

    Production wiring: pass `dry_run=False`. The first time the agent
    is asked to redeem, it imports `py_builder_relayer_client` and
    instantiates a single `RelayClient`. That client is reused across
    all subsequent submissions in the same process.

    Test wiring: pass `dry_run=True` (default). No imports beyond
    `eth_abi`/`eth_utils` happen; submissions return a sentinel
    `DRY_RUN_REDEEM:...` string so the caller's mark_redeemed audit
    trail still records something distinguishable from a real hash.

    Stage-1 deploy: even in production set `dry_run=True` and watch
    one sweep's output to confirm the calldata matches what the
    Polymarket UI would build for the same position. Then flip to
    `dry_run=False` for Stage 2.
    """

    def __init__(
        self,
        creds: RelayerCreds,
        *,
        dry_run: bool = True,
        relayer_url: str = RELAYER_URL,
        chain_id: int = POLYGON_CHAIN_ID,
    ) -> None:
        if not creds.private_key:
            raise ValueError("RelayerRedeemer: private_key is required")
        if not creds.funder_address:
            raise ValueError("RelayerRedeemer: funder_address is required")
        if creds.signature_type not in (0, 1, 2):
            raise ValueError(
                f"RelayerRedeemer: signature_type must be 0|1|2, "
                f"got {creds.signature_type!r}"
            )
        if not dry_run:
            # Live mode requires builder creds; dry-run can proceed
            # without them (useful for offline calldata verification).
            for f, v in (
                ("builder_api_key", creds.builder_api_key),
                ("builder_secret", creds.builder_secret),
                ("builder_passphrase", creds.builder_passphrase),
            ):
                if not v:
                    raise ValueError(
                        f"RelayerRedeemer (live mode): {f} is required"
                    )
        self._creds = creds
        self._dry_run = dry_run
        self._relayer_url = relayer_url
        self._chain_id = chain_id
        self._client: Any | None = None  # lazily instantiated in live mode

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    def build_calldata(
        self,
        *,
        condition_id: str,
        neg_risk: bool,
        outcome_index: int = 0,
        size: float = 0.0,
    ) -> RedeemCallData:
        """Build the redemption calldata WITHOUT submitting. Pure
        function — safe to call repeatedly and from tests."""
        if neg_risk:
            return build_neg_risk_redeem_calldata(
                condition_id=condition_id,
                outcome_index=outcome_index,
                size=size,
            )
        return build_standard_redeem_calldata(condition_id=condition_id)

    async def submit_redeem(
        self,
        *,
        condition_id: str,
        neg_risk: bool,
        outcome_index: int = 0,
        size: float = 0.0,
        market_label: str | None = None,
    ) -> str:
        """Build + submit a single redemption. Returns the on-chain
        tx hash (or `DRY_RUN_REDEEM:<cid-prefix>` in dry-run mode).

        Raises on relayer errors; the caller (RedeemerAgent) decides
        whether to retry or quarantine the condition_id.
        """
        calldata = self.build_calldata(
            condition_id=condition_id,
            neg_risk=neg_risk,
            outcome_index=outcome_index,
            size=size,
        )
        label = market_label or calldata.condition_id[:12]
        if self._dry_run:
            logger.info(
                "redeemer[DRY_RUN]: would submit %s | to=%s data=%s value=%s "
                "(market=%s, outcome=%d, size=%.4f)",
                calldata.market_type,
                calldata.to_address,
                calldata.data_hex,
                calldata.value,
                label,
                outcome_index,
                size,
            )
            # Sentinel hash — preserves audit trail without polluting
            # tx-hash analytics with fake on-chain values.
            return f"{DRY_RUN_TX_HASH_PREFIX}{calldata.condition_id[:12]}"

        # ── Live submission path ──────────────────────────────────────
        # Lazy import keeps tests / CI from needing the relayer client
        # installed.
        client = self._get_or_create_client()
        # Use the relayer client's own SafeTransaction model so
        # serialization matches whatever the package expects.
        from py_builder_relayer_client.models import (  # type: ignore
            OperationType,
            SafeTransaction,
        )

        txn = SafeTransaction(
            to=calldata.to_address,
            operation=OperationType.Call,
            data=calldata.data_hex,
            value=calldata.value,
        )
        logger.info(
            "redeemer: submitting %s redemption — to=%s cid=%s (market=%s)",
            calldata.market_type,
            calldata.to_address,
            calldata.condition_id[:12],
            label,
        )
        resp = client.execute([txn], f"redeem {calldata.condition_id[:12]}")
        # `wait()` blocks until the relayer reports the tx in a
        # terminal state. On MINED/CONFIRMED it returns the
        # SafeTransaction dict (with `transactionHash` populated);
        # on FAILED or poll-timeout it returns None.
        #
        # Polymarket's relayer leaves `transactionHash` empty in the
        # SUBMIT_TRANSACTION response (the tx hasn't mined yet), so
        # the response object's own `.transaction_hash` is "" at this
        # point. The on-chain hash only lives on the dict that wait()
        # returns — we MUST consume that return value.
        final_state = resp.wait()
        tx_hash = self._extract_tx_hash(resp, final_state)
        if not tx_hash:
            transaction_id = getattr(resp, "transaction_id", None)
            raise RuntimeError(
                "redeemer: relayer returned no tx hash after wait() — "
                f"transaction_id={transaction_id!r}, "
                f"final_state={final_state!r}, "
                f"cid={calldata.condition_id[:12]}, "
                f"to={calldata.to_address} — cannot mark redeemed"
            )
        return tx_hash

    def _get_or_create_client(self) -> Any:
        if self._client is not None:
            return self._client
        # Imports happen here so `import RelayerRedeemer` works in
        # environments without the relayer client.
        from py_builder_relayer_client.client import RelayClient  # type: ignore
        from py_builder_relayer_client.models import RelayerTxType
        from py_builder_signing_sdk.config import (  # type: ignore
            BuilderApiKeyCreds,
            BuilderConfig,
        )

        wallet_type = (
            RelayerTxType.PROXY
            if self._creds.signature_type == 1
            else RelayerTxType.SAFE
        )
        self._client = RelayClient(
            self._relayer_url,
            chain_id=self._chain_id,
            private_key=self._creds.private_key,
            builder_config=BuilderConfig(
                local_builder_creds=BuilderApiKeyCreds(
                    key=self._creds.builder_api_key,
                    secret=self._creds.builder_secret,
                    passphrase=self._creds.builder_passphrase,
                )
            ),
            relay_tx_type=wallet_type,
        )
        return self._client

    @staticmethod
    def _extract_tx_hash(
        resp: Any, final_state: Any | None = None
    ) -> str | None:
        """Best-effort tx-hash extraction across relayer response
        shapes.

        Priority:
          1. The dict returned by `resp.wait()` (final_state). The
             Polymarket relayer surfaces the on-chain hash here, NOT
             on the response object — the SUBMIT_TRANSACTION endpoint
             returns transactionHash empty because the tx hasn't been
             mined yet at that point.
          2. Attributes on the response object itself — kept for
             forward-compat with client versions that DO populate
             them, and for unit-test shapes used pre-fix.
          3. resp.result dict — alternate shape some versions return.
        """
        if isinstance(final_state, dict):
            for k in ("transactionHash", "transaction_hash", "hash"):
                v = final_state.get(k)
                if v:
                    return str(v)
        for attr in ("transaction_hash", "tx_hash", "hash"):
            v = getattr(resp, attr, None)
            if v:
                return str(v)
        # Some versions return a dict-like under .result
        result = getattr(resp, "result", None)
        if isinstance(result, dict):
            for k in ("transaction_hash", "tx_hash", "hash"):
                if result.get(k):
                    return str(result[k])
        return None
