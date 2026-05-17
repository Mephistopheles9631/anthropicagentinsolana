"""Raydium LaunchLab event parser.

Handles TradeEvent and PoolCreateEvent.

LaunchLab uses a bonding curve similar to Pumpfun.  The base mint is the
new token; the quote is SOL.  Unlike Pumpfun, the mint is NOT embedded in
the TradeEvent — only pool_state is present.  We look it up via the pool
registry.  It IS available in PoolCreateEvent via base_mint_param metadata,
but the actual mint pubkey comes from the accounts list in the transaction.

Strategy:
  * PoolCreateEvent  → ParsedCreate (mint resolved from pool registry or set
                        to sentinel so the pool can be tracked later).
  * TradeEvent       → ParsedSwap  (mint looked up from pool registry keyed
                        on pool_state address; skipped if unknown).

Price formula (same as Pumpfun bonding curve):
    price_sol = (virtual_quote / 1e9) / (virtual_base / 1e6)
"""
from __future__ import annotations

import logging
from datetime import datetime

from .base import ParsedCreate, ParsedSwap
from .borsh import BorshReader
from .log_utils import SOL_MINT

LOGGER = logging.getLogger(__name__)

_TOKEN_DECIMALS = 6
_SOL_DECIMALS = 9


class RaydiumLaunchLabParser:
    def __init__(
        self,
        program_id: str,
        trade_disc: bytes,
        pool_create_disc: bytes,
        pool_registry: object,  # PoolRegistry — avoid circular import
    ) -> None:
        self._program_id = program_id
        self._trade_disc = trade_disc
        self._pool_create_disc = pool_create_disc
        self._pool_registry = pool_registry

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def try_parse(
        self,
        log_data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | ParsedCreate | None:
        if len(log_data) < 8:
            return None
        disc = log_data[:8]
        if disc == self._trade_disc:
            return self._parse_trade(log_data[8:], slot, signature, signer, ingested_at)
        if disc == self._pool_create_disc:
            return self._parse_pool_create(log_data[8:], slot, signature, signer, ingested_at)
        return None

    # ------------------------------------------------------------------
    # TradeEvent
    # Fields: pool_state, total_base_sell, virtual_base, virtual_quote,
    #   real_base_before, real_quote_before, real_base_after, real_quote_after,
    #   amount_in, amount_out, protocol_fee, platform_fee, creator_fee,
    #   share_fee, trade_direction (u8 enum), pool_status (u8 enum), exact_in
    # ------------------------------------------------------------------

    def _parse_trade(
        self,
        data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        try:
            r = BorshReader(data)
            pool_state = r.read_pubkey()
            _total_base_sell = r.read_u64()
            virtual_base = r.read_u64()
            virtual_quote = r.read_u64()
            _real_base_before = r.read_u64()
            _real_quote_before = r.read_u64()
            real_base_after = r.read_u64()
            real_quote_after = r.read_u64()
            amount_in = r.read_u64()
            amount_out = r.read_u64()
            protocol_fee = r.read_u64()
            platform_fee = r.read_u64()
            creator_fee = r.read_u64()
            _share_fee = r.read_u64()
            trade_direction = r.read_enum_variant()  # 0=buy,1=sell typically
            _pool_status = r.read_enum_variant()
            _exact_in = r.read_bool()
        except (ValueError, IndexError):
            LOGGER.debug("raydium_launchlab: failed to decode TradeEvent", exc_info=True)
            return None

        # Look up mint from pool registry
        pair = self._pool_registry.get(pool_state)
        if pair is None:
            LOGGER.debug("raydium_launchlab: unknown pool %s, skipping swap", pool_state)
            return None
        mint, quote_mint = pair

        direction = "buy" if trade_direction == 0 else "sell"

        price = 0.0
        price_sol = None
        if virtual_base > 0:
            price = virtual_quote / virtual_base
            price_sol = (virtual_quote / 10 ** _SOL_DECIMALS) / (virtual_base / 10 ** _TOKEN_DECIMALS)

        fee_total = protocol_fee + platform_fee + creator_fee

        return ParsedSwap(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="raydium_launchlab",
            program_id=self._program_id,
            pool_address=pool_state,
            mint=mint,
            quote_mint=quote_mint,
            direction=direction,
            amount_in=amount_in,
            amount_out=amount_out,
            price=price,
            price_sol=price_sol,
            real_base_reserve=real_base_after,
            real_quote_reserve=real_quote_after,
            fee_amount=fee_total,
            signer=signer,
        )

    # ------------------------------------------------------------------
    # PoolCreateEvent
    # Fields: pool_state, creator, config,
    #   base_mint_param { decimals(u8), name(str), symbol(str), uri(str) },
    #   curve_param (enum — skip by reading variant + depending on variant
    #                the nested struct; we skip the whole enum payload),
    #   vesting_param (skip), amm_fee_on (skip)
    #
    # Note: the actual base mint pubkey is NOT in this event.  The pool
    # registry must receive it when we see the accompanying account data.
    # We emit a ParsedCreate with mint="" as a sentinel; the analytics sink
    # will handle it gracefully.
    # ------------------------------------------------------------------

    def _parse_pool_create(
        self,
        data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedCreate | None:
        try:
            r = BorshReader(data)
            pool_state = r.read_pubkey()
            creator = r.read_pubkey()
            _config = r.read_pubkey()
            # MintParams struct: decimals, name, symbol, uri
            decimals = r.read_u8()
            name = r.read_string()
            symbol = r.read_string()
            uri = r.read_string()
        except (ValueError, IndexError):
            LOGGER.debug("raydium_launchlab: failed to decode PoolCreateEvent", exc_info=True)
            return None

        return ParsedCreate(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="raydium_launchlab",
            program_id=self._program_id,
            pool_address=pool_state,
            mint="",          # resolved later via account data or first TradeEvent
            quote_mint=SOL_MINT,
            creator=creator,
            name=name or None,
            symbol=symbol or None,
            uri=uri or None,
            decimals=decimals,
            initial_price=None,
            initial_supply=None,
            initial_liquidity_base=None,
            initial_liquidity_quote=None,
        )
