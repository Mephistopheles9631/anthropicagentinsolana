from __future__ import annotations

import logging
from datetime import datetime

from ..event_parsers.base import ParsedCreate, ParsedSwap
from ..event_parsers.borsh import BorshReader
from ..event_parsers.log_utils import SOL_MINT

LOGGER = logging.getLogger(__name__)

_TOKEN_DECIMALS = 6
_SOL_DECIMALS = 9


class PumpfunInstructionParser:
    def __init__(self, program_id: str, idl_program: object) -> None:
        self._program_id = program_id
        self._disc_by_name: dict[str, bytes] = {}
        self._name_by_disc: dict[bytes, str] = {}

        instruction_by_disc = getattr(idl_program, "instruction_by_discriminator", {})
        if isinstance(instruction_by_disc, dict):
            for disc, name in instruction_by_disc.items():
                if isinstance(disc, (bytes, bytearray)) and isinstance(name, str):
                    self._disc_by_name[name] = bytes(disc)
                    self._name_by_disc[bytes(disc)] = name

        self._buy_disc = self._disc_by_name.get("buy")
        self._buy_exact_sol_in_disc = self._disc_by_name.get("buy_exact_sol_in")
        self._sell_disc = self._disc_by_name.get("sell")
        self._create_disc = self._disc_by_name.get("create")
        self._create_v2_disc = self._disc_by_name.get("create_v2")

    def try_parse(
        self,
        data_hex: str,
        accounts: list[str],
        slot: int | None,
        signature: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | ParsedCreate | None:
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

        if disc == self._buy_disc:
            return self._parse_buy(payload, accounts, slot, signature, ingested_at)
        if disc == self._buy_exact_sol_in_disc:
            return self._parse_buy_exact_sol_in(payload, accounts, slot, signature, ingested_at)
        if disc == self._sell_disc:
            return self._parse_sell(payload, accounts, slot, signature, ingested_at)
        if disc == self._create_disc:
            return self._parse_create(payload, accounts, slot, signature, ingested_at)
        if disc == self._create_v2_disc:
            return self._parse_create_v2(payload, accounts, slot, signature, ingested_at)

        return None

    @staticmethod
    def _account_at(accounts: list[str], idx: int) -> str | None:
        if idx < 0 or idx >= len(accounts):
            return None
        value = accounts[idx]
        if isinstance(value, str) and value and not value.startswith("lookup:"):
            return value
        return None

    def _parse_buy(
        self,
        payload: bytes,
        accounts: list[str],
        slot: int | None,
        signature: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        try:
            r = BorshReader(payload)
            token_amount = r.read_u64()
            max_sol_cost = r.read_u64()
            _track_volume = r.read_bool()
        except (ValueError, IndexError):
            LOGGER.debug("pumpfun_ix: failed to decode buy", exc_info=True)
            return None

        mint = self._account_at(accounts, 2)
        bonding_curve = self._account_at(accounts, 3)
        signer = self._account_at(accounts, 6)
        if not mint or not bonding_curve:
            return None

        price = 0.0
        price_sol = None
        if token_amount > 0:
            price = max_sol_cost / token_amount
            price_sol = (max_sol_cost / 10 ** _SOL_DECIMALS) / (token_amount / 10 ** _TOKEN_DECIMALS)

        return ParsedSwap(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="pumpfun",
            program_id=self._program_id,
            pool_address=bonding_curve,
            mint=mint,
            quote_mint=SOL_MINT,
            direction="buy",
            amount_in=max_sol_cost,
            amount_out=token_amount,
            price=price,
            price_sol=price_sol,
            real_base_reserve=None,
            real_quote_reserve=None,
            fee_amount=0,
            signer=signer,
        )

    def _parse_buy_exact_sol_in(
        self,
        payload: bytes,
        accounts: list[str],
        slot: int | None,
        signature: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        try:
            r = BorshReader(payload)
            spendable_sol_in = r.read_u64()
            min_tokens_out = r.read_u64()
            _track_volume = r.read_bool()
        except (ValueError, IndexError):
            LOGGER.debug("pumpfun_ix: failed to decode buy_exact_sol_in", exc_info=True)
            return None

        mint = self._account_at(accounts, 2)
        bonding_curve = self._account_at(accounts, 3)
        signer = self._account_at(accounts, 6)
        if not mint or not bonding_curve:
            return None

        price = 0.0
        price_sol = None
        if min_tokens_out > 0:
            price = spendable_sol_in / min_tokens_out
            price_sol = (spendable_sol_in / 10 ** _SOL_DECIMALS) / (min_tokens_out / 10 ** _TOKEN_DECIMALS)

        return ParsedSwap(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="pumpfun",
            program_id=self._program_id,
            pool_address=bonding_curve,
            mint=mint,
            quote_mint=SOL_MINT,
            direction="buy",
            amount_in=spendable_sol_in,
            amount_out=min_tokens_out,
            price=price,
            price_sol=price_sol,
            real_base_reserve=None,
            real_quote_reserve=None,
            fee_amount=0,
            signer=signer,
        )

    def _parse_sell(
        self,
        payload: bytes,
        accounts: list[str],
        slot: int | None,
        signature: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | None:
        try:
            r = BorshReader(payload)
            token_amount = r.read_u64()
            min_sol_output = r.read_u64()
        except (ValueError, IndexError):
            LOGGER.debug("pumpfun_ix: failed to decode sell", exc_info=True)
            return None

        mint = self._account_at(accounts, 2)
        bonding_curve = self._account_at(accounts, 3)
        signer = self._account_at(accounts, 6)
        if not mint or not bonding_curve:
            return None

        price = 0.0
        price_sol = None
        if token_amount > 0:
            price = min_sol_output / token_amount
            price_sol = (min_sol_output / 10 ** _SOL_DECIMALS) / (token_amount / 10 ** _TOKEN_DECIMALS)

        return ParsedSwap(
            ingested_at=ingested_at,
            slot=slot,
            signature=signature,
            program_name="pumpfun",
            program_id=self._program_id,
            pool_address=bonding_curve,
            mint=mint,
            quote_mint=SOL_MINT,
            direction="sell",
            amount_in=token_amount,
            amount_out=min_sol_output,
            price=price,
            price_sol=price_sol,
            real_base_reserve=None,
            real_quote_reserve=None,
            fee_amount=0,
            signer=signer,
        )

    def _parse_create(
        self,
        payload: bytes,
        accounts: list[str],
        slot: int | None,
        signature: str | None,
        ingested_at: datetime,
    ) -> ParsedCreate | None:
        try:
            r = BorshReader(payload)
            name = r.read_string()
            symbol = r.read_string()
            uri = r.read_string()
            creator = r.read_pubkey()
        except (ValueError, IndexError):
            LOGGER.debug("pumpfun_ix: failed to decode create", exc_info=True)
            return None

        mint = self._account_at(accounts, 0)
        bonding_curve = self._account_at(accounts, 2)
        if not mint or not bonding_curve:
            return None

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
            initial_price=None,
            initial_supply=None,
            initial_liquidity_base=None,
            initial_liquidity_quote=None,
        )

    def _parse_create_v2(
        self,
        payload: bytes,
        accounts: list[str],
        slot: int | None,
        signature: str | None,
        ingested_at: datetime,
    ) -> ParsedCreate | None:
        try:
            r = BorshReader(payload)
            name = r.read_string()
            symbol = r.read_string()
            uri = r.read_string()
            creator = r.read_pubkey()
            _is_mayhem_mode = r.read_bool()
        except (ValueError, IndexError):
            LOGGER.debug("pumpfun_ix: failed to decode create_v2", exc_info=True)
            return None

        mint = self._account_at(accounts, 0)
        bonding_curve = self._account_at(accounts, 2)
        if not mint or not bonding_curve:
            return None

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
            initial_price=None,
            initial_supply=None,
            initial_liquidity_base=None,
            initial_liquidity_quote=None,
        )
