"""Utilities for extracting Anchor program-data log lines from a shredstream payload."""
from __future__ import annotations

import base64
import logging

LOGGER = logging.getLogger(__name__)

_PREFIX = "Program data: "
_PREFIX_LEN = len(_PREFIX)

# Known SOL/WSOL mint address
SOL_MINT = "So11111111111111111111111111111111111111112"


def extract_log_messages(payload: dict) -> list[str]:
    """Walk common protobuf-dict paths looking for log_messages."""
    # Yellowstone gRPC: payload['transaction']['meta']['log_messages']
    txn = payload.get("transaction")
    if isinstance(txn, dict):
        meta = txn.get("meta")
        if isinstance(meta, dict):
            logs = meta.get("log_messages")
            if isinstance(logs, list):
                return logs  # type: ignore[return-value]

    # Flat fallback
    logs = payload.get("log_messages")
    if isinstance(logs, list):
        return logs  # type: ignore[return-value]

    return []


def extract_program_data_bytes(payload: dict) -> list[bytes]:
    """Return decoded bytes for every 'Program data: …' log line."""
    results: list[bytes] = []
    for line in extract_log_messages(payload):
        if not isinstance(line, str) or not line.startswith(_PREFIX):
            continue
        b64_part = line[_PREFIX_LEN:]
        try:
            results.append(base64.b64decode(b64_part))
        except Exception:
            pass
    return results


def extract_signer(payload: dict) -> str | None:
    """Best-effort: return the first (fee-payer) account key as a Solana address."""
    import base64 as _b64

    txn = payload.get("transaction")
    if not isinstance(txn, dict):
        return None
    inner = txn.get("transaction")
    if not isinstance(inner, dict):
        return None
    message = inner.get("message")
    if not isinstance(message, dict):
        return None
    account_keys = message.get("account_keys", [])
    if not account_keys:
        return None
    raw_b64 = account_keys[0]
    if not isinstance(raw_b64, str):
        return None
    try:
        raw_bytes = _b64.b64decode(raw_b64)
    except Exception:
        return None
    if len(raw_bytes) != 32:
        return None
    # local import to avoid circular — borsh module has the b58 encoder
    from .borsh import _b58encode  # type: ignore[attr-defined]
    return _b58encode(raw_bytes)
