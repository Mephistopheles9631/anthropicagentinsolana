from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


# ---------------------------------------------------------------------------
# Normalized output dataclasses
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ParsedSwap:
    """Normalized swap record extracted from any supported protocol."""
    ingested_at: datetime
    slot: int | None
    signature: str | None
    program_name: str
    program_id: str
    pool_address: str
    mint: str               # base token (the one being bought/sold)
    quote_mint: str         # SOL or USDC mint address
    direction: str          # "buy" or "sell"
    amount_in: int          # raw u64, units of input token
    amount_out: int         # raw u64, units of output token
    price: float            # quote per base in raw lamport/token ratio
    price_sol: float | None # SOL per base token (normalised, may be None)
    real_base_reserve: int | None
    real_quote_reserve: int | None
    fee_amount: int
    signer: str | None      # wallet that signed the transaction


@dataclass(slots=True)
class ParsedCreate:
    """Normalized pool / token launch record."""
    ingested_at: datetime
    slot: int | None
    signature: str | None
    program_name: str
    program_id: str
    pool_address: str
    mint: str
    quote_mint: str
    creator: str
    name: str | None
    symbol: str | None
    uri: str | None
    decimals: int | None
    initial_price: float | None
    initial_supply: int | None
    initial_liquidity_base: int | None
    initial_liquidity_quote: int | None
