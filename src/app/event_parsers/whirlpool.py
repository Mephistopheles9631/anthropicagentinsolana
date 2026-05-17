"""Orca Whirlpool event parser.

Handles the ``Traded`` event and the ``PoolInitialized`` event.

The ``Traded`` event does NOT embed mint addresses — only the whirlpool pool
address.  Mints are resolved via the pool registry populated from
``PoolInitialized``.

Price formula (same as other CLMM programs, Q64.64 sqrt_price):
    price = (sqrt_price / 2**64) ** 2

Direction:
    aToB = True  → selling tokenA, buying tokenB  → "sell" (if A is base)
    aToB = False → selling tokenB, buying tokenA  → "buy"  (if A is base)
"""
from __future__ import annotations

import logging
from datetime import datetime

from .base import ParsedCreate, ParsedSwap
from .borsh import BorshReader
from .log_utils import SOL_MINT

LOGGER = logging.getLogger(__name__)

_Q64 = 2 ** 64


class WhirlpoolParser:
    def __init__(
        self,
        program_id: str,
        traded_disc: bytes,
        pool_init_disc: bytes,
        pool_registry: object,
    ) -> None:
        self._program_id = program_id
        self._traded_disc = traded_disc
        self._pool_init_disc = pool_init_disc
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
        if disc == self._traded_disc:
            return self._parse_traded(log_data[8:], slot, signature, signer, ingested_at)
        if disc == self._pool_init_disc:
            return self._parse_pool_init(log_data[8:], slot, signature, signer, ingested_at)
        return None

    # ------------------------------------------------------------------
    # Traded event
    # Fields: whirlpool(pubkey), aToB(bool),
    #   preSqrtPrice(u128), postSqrtPrice(u128),
    #   inputAmount(u64), outputAmount(u64),
    #   inputTransferFee(u64), outputTransferFee(u64),
    #   lpFee(u64), protocolFee(u64)
    # ------------------------------------------------------------------

    def _parse_traded(
        self,
        data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        try:
            r = BorshReader(data)
            whirlpool = r.read_pubkey()
            a_to_b = r.read_bool()
            _pre_sqrt = r.read_u128()
            post_sqrt = r.read_u128()
            input_amount = r.read_u64()
            output_amount = r.read_u64()
            _input_tf = r.read_u64()
            _output_tf = r.read_u64()
            lp_fee = r.read_u64()
            protocol_fee = r.read_u64()
        except (ValueError, IndexError):
            LOGGER.debug("whirlpool: failed to decode Traded", exc_info=True)
            return None

        pair = self._pool_registry.get(whirlpool)
        if pair is None:
            LOGGER.debug("whirlpool: unknown pool %s, skipping swap", whirlpool)
            return None
        token_a_mint, token_b_mint = pair

        # aToB=True means selling tokenA → direction depends on which is base
        if token_b_mint == SOL_MINT:
            mint, quote_mint = token_a_mint, token_b_mint
            direction = "sell" if a_to_b else "buy"
        else:
            mint, quote_mint = token_a_mint, token_b_mint
            direction = "sell" if a_to_b else "buy"

        price = (post_sqrt / _Q64) ** 2 if post_sqrt > 0 else 0.0

        return ParsedSwap(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="whirlpool",
            program_id=self._program_id,
            pool_address=whirlpool,
            mint=mint,
            quote_mint=quote_mint,
            direction=direction,
            amount_in=input_amount,
            amount_out=output_amount,
            price=price,
            price_sol=None,
            real_base_reserve=None,
            real_quote_reserve=None,
            fee_amount=lp_fee + protocol_fee,
            signer=signer,
        )

    # ------------------------------------------------------------------
    # PoolInitialized event
    # Fields from IDL (whirlpool.json):
    #   whirlpoolsConfig(pubkey), whirlpool(pubkey),
    #   tokenMintA(pubkey), tokenMintB(pubkey),
    #   tokenVaultA(pubkey), tokenVaultB(pubkey),
    #   feeTier(pubkey), tickSpacing(u16), initialSqrtPrice(u128)
    # ------------------------------------------------------------------

    def _parse_pool_init(
        self,
        data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedCreate | None:
        try:
            r = BorshReader(data)
            _config = r.read_pubkey()
            whirlpool = r.read_pubkey()
            token_mint_a = r.read_pubkey()
            token_mint_b = r.read_pubkey()
            _vault_a = r.read_pubkey()
            _vault_b = r.read_pubkey()
            _fee_tier = r.read_pubkey()
            _tick_spacing = r.read_u16()
            initial_sqrt_price = r.read_u128()
        except (ValueError, IndexError):
            LOGGER.debug("whirlpool: failed to decode PoolInitialized", exc_info=True)
            return None

        self._pool_registry.update(whirlpool, token_mint_a, token_mint_b)

        initial_price = (initial_sqrt_price / _Q64) ** 2 if initial_sqrt_price > 0 else None

        if token_b_mint_is_sol := (token_mint_b == SOL_MINT):
            mint, quote_mint = token_mint_a, token_mint_b
        else:
            mint, quote_mint = token_mint_a, token_mint_b
        _ = token_b_mint_is_sol  # suppress unused warning

        return ParsedCreate(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="whirlpool",
            program_id=self._program_id,
            pool_address=whirlpool,
            mint=mint,
            quote_mint=quote_mint,
            creator=signer or "",
            name=None,
            symbol=None,
            uri=None,
            decimals=None,
            initial_price=initial_price,
            initial_supply=None,
            initial_liquidity_base=None,
            initial_liquidity_quote=None,
        )
