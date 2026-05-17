"""Meteora DAMM v2 event parser.

Handles EvtSwap2 and EvtInitializePool.

Neither event embeds the mint addresses directly (except EvtInitializePool).
For swaps, we look up the pool in the pool registry which is populated by
EvtInitializePool events.

Price formula for CLMM-style sqrt_price (Q64.64 fixed-point):
    price = (sqrt_price / 2**64) ** 2
    This gives the price in terms of tokenB per tokenA (raw, no decimal adjustment).

Trade direction field values:
    0 = A→B (sell tokenA / buy tokenB)
    1 = B→A (sell tokenB / buy tokenA)

We treat tokenA as the base (non-SOL) token when tokenBMint is SOL.
"""
from __future__ import annotations

import logging
from datetime import datetime

from .base import ParsedCreate, ParsedSwap
from .borsh import BorshReader
from .log_utils import SOL_MINT

LOGGER = logging.getLogger(__name__)

_Q64 = 2 ** 64


class MeteoraDAMMv2Parser:
    def __init__(
        self,
        program_id: str,
        swap_disc: bytes,
        init_pool_disc: bytes,
        pool_registry: object,
    ) -> None:
        self._program_id = program_id
        self._swap_disc = swap_disc
        self._init_pool_disc = init_pool_disc
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
        if disc == self._swap_disc:
            return self._parse_swap(log_data[8:], slot, signature, signer, ingested_at)
        if disc == self._init_pool_disc:
            return self._parse_init_pool(log_data[8:], slot, signature, signer, ingested_at)
        return None

    # ------------------------------------------------------------------
    # EvtSwap2
    # Fields: pool(pubkey), tradeDirection(u8), collectFeeMode(u8),
    #   hasReferral(bool),
    #   params { amount0(u64), amount1(u64), swapMode(u8) },
    #   swapResult { includedFeeInputAmount(u64), excludedFeeInputAmount(u64),
    #                amountLeft(u64), outputAmount(u64), nextSqrtPrice(u128),
    #                claimingFee(u64), protocolFee(u64), compoundingFee(u64),
    #                referralFee(u64) },
    #   includedTransferFeeAmountIn(u64), includedTransferFeeAmountOut(u64),
    #   excludedTransferFeeAmountOut(u64), currentTimestamp(u64),
    #   reserveAAmount(u64), reserveBAmount(u64)
    # ------------------------------------------------------------------

    def _parse_swap(
        self,
        data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        try:
            r = BorshReader(data)
            pool = r.read_pubkey()
            trade_direction = r.read_u8()
            _collect_fee_mode = r.read_u8()
            _has_referral = r.read_bool()
            # SwapParameters2
            amount0 = r.read_u64()
            amount1 = r.read_u64()
            _swap_mode = r.read_u8()
            # SwapResult2
            _included_fee_in = r.read_u64()
            _excluded_fee_in = r.read_u64()
            _amount_left = r.read_u64()
            output_amount = r.read_u64()
            next_sqrt_price = r.read_u128()
            _claiming_fee = r.read_u64()
            protocol_fee = r.read_u64()
            _compounding_fee = r.read_u64()
            _referral_fee = r.read_u64()
            # remaining transfer-fee fields
            _incl_tf_in = r.read_u64()
            _incl_tf_out = r.read_u64()
            _excl_tf_out = r.read_u64()
            _current_ts = r.read_u64()
            reserve_a = r.read_u64()
            reserve_b = r.read_u64()
        except (ValueError, IndexError):
            LOGGER.debug("meteora_damm_v2: failed to decode EvtSwap2", exc_info=True)
            return None

        pair = self._pool_registry.get(pool)
        if pair is None:
            LOGGER.debug("meteora_damm_v2: unknown pool %s, skipping swap", pool)
            return None
        token_a_mint, token_b_mint = pair

        # Determine base/quote: if tokenB is SOL, tokenA is base; otherwise keep tokenA as base.
        if token_b_mint == SOL_MINT:
            mint, quote_mint = token_a_mint, token_b_mint
            base_reserve, quote_reserve = reserve_a, reserve_b
        else:
            # default: tokenA is base
            mint, quote_mint = token_a_mint, token_b_mint
            base_reserve, quote_reserve = reserve_a, reserve_b

        # trade_direction: 0 = A→B (input is tokenA = sell base),
        #                  1 = B→A (input is tokenB = buy base with SOL)
        if trade_direction == 1:
            direction = "buy"
            amount_in = amount0   # quote in
            amount_out = output_amount  # base out
        else:
            direction = "sell"
            amount_in = amount0   # base in
            amount_out = output_amount  # quote out

        # price derived from sqrt_price (Q64.64)
        price = (next_sqrt_price / _Q64) ** 2
        price_sol = None  # would need decimal info to normalize

        return ParsedSwap(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="meteora_damm_v2",
            program_id=self._program_id,
            pool_address=pool,
            mint=mint,
            quote_mint=quote_mint,
            direction=direction,
            amount_in=amount_in,
            amount_out=amount_out,
            price=price,
            price_sol=price_sol,
            real_base_reserve=base_reserve,
            real_quote_reserve=quote_reserve,
            fee_amount=protocol_fee,
            signer=signer,
        )

    # ------------------------------------------------------------------
    # EvtInitializePool
    # Fields: pool, tokenAMint, tokenBMint, creator, payer, alphaVault,
    #   poolFees(PoolFeeParameters — skip), sqrtMinPrice(u128),
    #   sqrtMaxPrice(u128), activationType(u8), collectFeeMode(u8),
    #   liquidity(u128), sqrtPrice(u128), activationPoint(u64),
    #   tokenAFlag(u8), tokenBFlag(u8), tokenAAmount(u64),
    #   tokenBAmount(u64), totalAmountA(u64), totalAmountB(u64), poolType(u8)
    #
    # PoolFeeParameters is a complex nested struct; we need to skip it.
    # Without knowing its exact byte size we decode fields before it and
    # note: from the IDL it appears poolFees comes after payer+alphaVault.
    # We skip any remaining bytes after collecting essential fields.
    # ------------------------------------------------------------------

    def _parse_init_pool(
        self,
        data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedCreate | None:
        try:
            r = BorshReader(data)
            pool = r.read_pubkey()
            token_a_mint = r.read_pubkey()
            token_b_mint = r.read_pubkey()
            creator = r.read_pubkey()
            _payer = r.read_pubkey()
            _alpha_vault = r.read_pubkey()
            # PoolFeeParameters — complex struct, skip remaining and extract
            # what we can from the trailing known-width fields by working from
            # the end of the buffer (the last 8 fixed-size fields).
            # tokenAAmount(8) + tokenBAmount(8) + totalAmountA(8) + totalAmountB(8) + poolType(1) = 33 bytes
            # activationPoint(8) + tokenAFlag(1) + tokenBFlag(1) = 10 bytes
            # liquidity(16) + sqrtPrice(16) = 32 bytes
            # sqrtMinPrice(16) + sqrtMaxPrice(16) = 32 bytes
            # activationType(1) + collectFeeMode(1) = 2 bytes
            # Total trailing (excluding poolFees): 2+32+32+10+33 = 109 bytes
            _TRAILING = 109
            if r.remaining < _TRAILING:
                LOGGER.debug("meteora_damm_v2: EvtInitializePool too short for trailing fields")
                return None
            # Jump to trailing fields
            r.skip(r.remaining - _TRAILING)
            _activation_type = r.read_u8()
            _collect_fee_mode = r.read_u8()
            _sqrt_min = r.read_u128()
            _sqrt_max = r.read_u128()
            _liquidity = r.read_u128()
            sqrt_price = r.read_u128()
            _activation_point = r.read_u64()
            _token_a_flag = r.read_u8()
            _token_b_flag = r.read_u8()
            token_a_amount = r.read_u64()
            token_b_amount = r.read_u64()
            _total_a = r.read_u64()
            _total_b = r.read_u64()
            _pool_type = r.read_u8()
        except (ValueError, IndexError):
            LOGGER.debug("meteora_damm_v2: failed to decode EvtInitializePool", exc_info=True)
            return None

        initial_price = (sqrt_price / _Q64) ** 2 if sqrt_price > 0 else None

        # Register pool → mints so future swaps can be resolved
        self._pool_registry.update(pool, token_a_mint, token_b_mint)

        if token_b_mint == SOL_MINT:
            mint, quote_mint = token_a_mint, token_b_mint
            liq_base, liq_quote = token_a_amount, token_b_amount
        else:
            mint, quote_mint = token_a_mint, token_b_mint
            liq_base, liq_quote = token_a_amount, token_b_amount

        return ParsedCreate(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="meteora_damm_v2",
            program_id=self._program_id,
            pool_address=pool,
            mint=mint,
            quote_mint=quote_mint,
            creator=creator,
            name=None,
            symbol=None,
            uri=None,
            decimals=None,
            initial_price=initial_price,
            initial_supply=None,
            initial_liquidity_base=liq_base,
            initial_liquidity_quote=liq_quote,
        )
