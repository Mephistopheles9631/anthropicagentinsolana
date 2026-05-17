"""Pumpfun event parser.

Handles TradeEvent and CreateEvent emitted by the Pumpfun bonding-curve program.
Both events embed the mint address directly, so no pool registry lookup is needed.

Price convention:
    price_sol = (sol_amount / 1e9) / (token_amount / 1e6)
    i.e. SOL per token using standard 9/6 decimal split.
"""
from __future__ import annotations

import logging
from datetime import datetime

from .base import ParsedCreate, ParsedSwap
from .borsh import BorshReader
from .log_utils import SOL_MINT

LOGGER = logging.getLogger(__name__)

# Pumpfun standard token decimals (almost all pumpfun tokens use 6).
_TOKEN_DECIMALS = 6
_SOL_DECIMALS = 9


class PumpfunParser:
    """Stateless parser for Pumpfun TradeEvent and CreateEvent."""

    def __init__(self, program_id: str, trade_disc: bytes, create_disc: bytes) -> None:
        self._program_id = program_id
        self._trade_disc = trade_disc
        self._create_disc = create_disc

    # ------------------------------------------------------------------
    # Public API
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
        if disc == self._create_disc:
            return self._parse_create(log_data[8:], slot, signature, signer, ingested_at)
        return None

    # ------------------------------------------------------------------
    # TradeEvent decoder
    # Fields (in Borsh order from IDL):
    #   mint, sol_amount, token_amount, is_buy, user, timestamp,
    #   virtual_sol_reserves, virtual_token_reserves,
    #   real_sol_reserves, real_token_reserves,
    #   fee_recipient, fee_basis_points, fee,
    #   creator, creator_fee_basis_points, creator_fee,
    #   track_volume, total_unclaimed_tokens, total_claimed_tokens,
    #   current_sol_volume, last_update_timestamp, ix_name
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
            mint = r.read_pubkey()
            sol_amount = r.read_u64()
            token_amount = r.read_u64()
            is_buy = r.read_bool()
            user = r.read_pubkey()
            _timestamp = r.read_i64()
            virtual_sol = r.read_u64()
            virtual_token = r.read_u64()
            real_sol = r.read_u64()
            real_token = r.read_u64()
            _fee_recipient = r.read_pubkey()
            _fee_bp = r.read_u64()
            fee = r.read_u64()
        except (ValueError, IndexError):
            LOGGER.debug("pumpfun: failed to decode TradeEvent", exc_info=True)
            return None

        direction = "buy" if is_buy else "sell"

        # price = SOL per token
        if token_amount > 0:
            price = sol_amount / token_amount
            price_sol = (sol_amount / 10 ** _SOL_DECIMALS) / (token_amount / 10 ** _TOKEN_DECIMALS)
        else:
            price = 0.0
            price_sol = None

        if is_buy:
            amount_in = sol_amount    # SOL spent
            amount_out = token_amount  # tokens received
        else:
            amount_in = token_amount  # tokens spent
            amount_out = sol_amount   # SOL received

        return ParsedSwap(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="pumpfun",
            program_id=self._program_id,
            pool_address="",          # bonding curve address not in event; set via pool_registry
            mint=mint,
            quote_mint=SOL_MINT,
            direction=direction,
            amount_in=amount_in,
            amount_out=amount_out,
            price=price,
            price_sol=price_sol,
            real_base_reserve=real_token,
            real_quote_reserve=real_sol,
            fee_amount=fee,
            signer=user,
        )

    # ------------------------------------------------------------------
    # CreateEvent decoder
    # Fields:
    #   name, symbol, uri, mint, bonding_curve, user, creator, timestamp,
    #   virtual_token_reserves, virtual_sol_reserves,
    #   real_token_reserves, token_total_supply, token_program,
    #   is_mayhem_mode
    # ------------------------------------------------------------------

    def _parse_create(
        self,
        data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedCreate | None:
        try:
            r = BorshReader(data)
            name = r.read_string()
            symbol = r.read_string()
            uri = r.read_string()
            mint = r.read_pubkey()
            bonding_curve = r.read_pubkey()
            _user = r.read_pubkey()
            creator = r.read_pubkey()
            _timestamp = r.read_i64()
            virtual_token = r.read_u64()
            virtual_sol = r.read_u64()
            real_token = r.read_u64()
            token_total_supply = r.read_u64()
        except (ValueError, IndexError):
            LOGGER.debug("pumpfun: failed to decode CreateEvent", exc_info=True)
            return None

        initial_price = None
        if virtual_token > 0:
            initial_price = (virtual_sol / 10 ** _SOL_DECIMALS) / (virtual_token / 10 ** _TOKEN_DECIMALS)

        return ParsedCreate(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="pumpfun",
            program_id=self._program_id,
            pool_address=bonding_curve,
            mint=mint,
            quote_mint=SOL_MINT,
            creator=creator,
            name=name or None,
            symbol=symbol or None,
            uri=uri or None,
            decimals=_TOKEN_DECIMALS,
            initial_price=initial_price,
            initial_supply=token_total_supply,
            initial_liquidity_base=real_token,
            initial_liquidity_quote=virtual_sol,
        )
