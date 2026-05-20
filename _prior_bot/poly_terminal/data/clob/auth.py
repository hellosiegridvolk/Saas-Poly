"""Derive L2 API credentials from a Magic Link private key.

Polymarket Builder Codes can be revoked, lost, or rotated. The bot's
robust startup path is to derive fresh L2 credentials from the L1
(private key) each boot via py-clob-client-v2.

Polygon mainnet chain_id = 137. Magic Link signature_type = 1
(POLY_PROXY in V2 nomenclature).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

CHAIN_ID_POLYGON_MAINNET: int = 137
SIGNATURE_TYPE_MAGIC_LINK: int = 1


@dataclass(frozen=True)
class DerivedCreds:
    api_key: str
    api_secret: str
    api_passphrase: str


def derive_l2_creds_from_private_key(
    host: str,
    private_key: str,
    funder_address: str,
    chain_id: int = CHAIN_ID_POLYGON_MAINNET,
    signature_type: int = SIGNATURE_TYPE_MAGIC_LINK,
) -> DerivedCreds | None:
    """Build a ClobClient with L1 auth, then create-or-derive L2 creds.

    Returns None on any failure so the caller can fall back to
    explicit creds or skip the User WebSocket entirely.
    """
    if not private_key or not funder_address:
        return None
    try:
        from py_clob_client_v2.client import ClobClient
    except ImportError:
        logger.warning("py-clob-client-v2 not available — cannot derive L2 creds")
        return None
    try:
        client = ClobClient(
            host=host,
            chain_id=chain_id,
            key=private_key,
            signature_type=signature_type,
            funder=funder_address,
        )
        # V2 renamed create_or_derive_api_creds → create_or_derive_api_key.
        creds = client.create_or_derive_api_key()
    except Exception:
        logger.exception("derive_l2_creds: ClobClient call failed")
        return None
    if creds is None:
        return None
    return DerivedCreds(
        api_key=creds.api_key,
        api_secret=creds.api_secret,
        api_passphrase=creds.api_passphrase,
    )
