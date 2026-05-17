"""Raydium CPMM event parser.

The Raydium CPMM IDL defines a ``SwapEvent`` but its ``discriminator`` field
exists and no ``fields`` are listed, meaning the event body is empty in the
IDL definition.  In practice, CPMM swap data is available via instruction
accounts and instruction data rather than log events.

This parser registers the swap discriminator so the event is *recognised*
(and the raw event counted) but cannot produce a full ParsedSwap from log
data alone.  A future enhancement can read instruction accounts from the
transaction message to resolve pool/mint info.
"""
from __future__ import annotations

import logging
from datetime import datetime

from .base import ParsedSwap

LOGGER = logging.getLogger(__name__)


class RaydiumCPMMParser:
    """Recognises SwapEvent discriminator but produces no structured output."""

    def __init__(self, program_id: str, swap_disc: bytes) -> None:
        self._program_id = program_id
        self._swap_disc = swap_disc

    def try_parse(
        self,
        log_data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        if len(log_data) >= 8 and log_data[:8] == self._swap_disc:
            LOGGER.debug(
                "raydium_cpmm: SwapEvent detected sig=%s (no structured decode available)",
                signature,
            )
        return None
