from __future__ import annotations

import logging
from datetime import datetime

from ..event_parsers.base import ParsedSwap
from ..event_parsers.borsh import BorshReader
from ..event_parsers.log_utils import SOL_MINT
from ..pool_registry import PoolRegistry

LOGGER = logging.getLogger(__name__)

# Observed pumpswap instruction discriminators from entry data prefixes.
# These are best-effort until a formal IDL is added.
_SWAP_PREFIXES = {
    bytes.fromhex("66063d1201daebea"),
    bytes.fromhex("33e685a4017f83ad"),
}

# Observed prefix that appears less frequently; treated as a create/init hint.
_CREATE_PREFIXES = {
    bytes.fromhex("c62e1552b4d9e870"),
}

_TOKEN_DECIMALS = 6
_SOL_DECIMALS = 9


class PumpswapInstructionParser:
    def __init__(self, program_id: str, pool_registry: PoolRegistry) -> None:
        self._program_id = program_id
        self._pool_registry = pool_registry

    def try_parse(
        self,
        data_hex: str,
        accounts: list[str],
        slot: int | None,
        signature: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        if not data_hex:
            return None
        try:
            data = bytes.fromhex(data_hex)
        except ValueError:
            return None
        if len(data) < 8:
            return None

        disc = data[:8]
        payload = data[8:]

        if disc in _SWAP_PREFIXES:
            return self._parse_swap(payload, accounts, slot, signature, ingested_at)

        if disc in _CREATE_PREFIXES:
            LOGGER.debug(
                "pumpswap_ix: create_prefix_seen slot=%s signature=%s",
                slot,
                signature,
            )
            return None

        return None

    def _resolve_pool(self, accounts: list[str]) -> tuple[str, str, str] | None:
        for acct in accounts:
            if not isinstance(acct, str) or not acct or acct.startswith("lookup:"):
                continue
            hit = self._pool_registry.get(acct)
            if hit is not None:
                mint, quote = hit
                return acct, mint, quote
        return None

    def _parse_swap(
        self,
        payload: bytes,
        accounts: list[str],
        slot: int | None,
        signature: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        try:
            r = BorshReader(payload)
            amount_in = r.read_u64()
            min_out = r.read_u64()
        except (ValueError, IndexError):
            LOGGER.debug("pumpswap_ix: failed to decode swap", exc_info=True)
            return None

        resolved = self._resolve_pool(accounts)
        if resolved is None:
            LOGGER.debug(
                "pumpswap_ix_unresolved slot=%s signature=%s accounts=%d",
                slot,
                signature,
                len(accounts),
            )
            return None

        pool_address, mint, quote_mint = resolved
        price = 0.0
        price_sol = None
        if min_out > 0:
            price = amount_in / min_out
            if quote_mint == SOL_MINT:
                price_sol = (amount_in / 10 ** _SOL_DECIMALS) / (min_out / 10 ** _TOKEN_DECIMALS)

        return ParsedSwap(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="pumpswap",
            program_id=self._program_id,
            pool_address=pool_address,
            mint=mint,
            quote_mint=quote_mint,
            direction="swap",
            amount_in=amount_in,
            amount_out=min_out,
            price=price,
            price_sol=price_sol,
            real_base_reserve=None,
            real_quote_reserve=None,
            fee_amount=0,
            signer=None,
        )
