"""Factory that builds per-program event parsers from the IDL registry.

Each parser is keyed by ``program_id``.  At runtime, ``parse()`` is called
with the raw log-data bytes for every ``Program data:`` log line in a
transaction.  The correct parser is selected by ``program_id``, then each
parser checks its own discriminators.
"""
from __future__ import annotations

import logging
from datetime import datetime

from .base import ParsedCreate, ParsedSwap

LOGGER = logging.getLogger(__name__)


class EventParserRegistry:
    """Holds one parser per program_id and dispatches decode calls."""

    def __init__(self) -> None:
        self._parsers: dict[str, list[object]] = {}  # program_id → list of parsers

    def register(self, program_id: str, parser: object) -> None:
        self._parsers.setdefault(program_id, []).append(parser)

    def parse(
        self,
        program_id: str,
        log_data: bytes,
        slot: int | None,
        signature: str | None,
        signer: str | None,
        ingested_at: datetime,
    ) -> ParsedSwap | ParsedCreate | None:
        for parser in self._parsers.get(program_id, []):
            try:
                result = parser.try_parse(log_data, slot, signature, signer, ingested_at)  # type: ignore[attr-defined]
                if result is not None:
                    return result
            except Exception:
                LOGGER.debug("parser error program_id=%s", program_id, exc_info=True)
        return None

    def has_program(self, program_id: str) -> bool:
        return program_id in self._parsers


def build_parser_registry(
    idl_registry: object,
    pool_registry: object,
    settings: object,
) -> EventParserRegistry:
    """Construct an EventParserRegistry from the loaded IDL registry.

    ``idl_registry`` is ``IdlRegistry`` from ``app.idl_registry``.
    ``pool_registry`` is ``PoolRegistry`` from ``app.pool_registry``.
    ``settings`` is ``Settings`` from ``app.config``.
    """
    from ..idl_registry import IdlRegistry  # type: ignore[attr-defined]
    from ..config import Settings  # type: ignore[attr-defined]
    from .pumpfun import PumpfunParser
    from .raydium_launchlab import RaydiumLaunchLabParser
    from .meteora_damm_v2 import MeteoraDAMMv2Parser
    from .whirlpool import WhirlpoolParser
    from .raydium_cpmm import RaydiumCPMMParser

    assert isinstance(idl_registry, IdlRegistry)
    assert isinstance(settings, Settings)

    registry = EventParserRegistry()

    # Helper: resolve discriminator bytes from the IdlProgram event map.
    def _get_event_disc(idl: object, event_name: str) -> bytes | None:
        for disc_bytes, name in idl.event_by_discriminator.items():  # type: ignore[attr-defined]
            if name == event_name:
                return disc_bytes
        return None

    def _get_idl_for_id(program_id: str) -> object | None:
        return idl_registry._by_program_id.get(program_id)  # type: ignore[attr-defined]

    # ------------------------------------------------------------------ Pumpfun
    pf_id = settings.program_id_pumpfun.strip()
    if pf_id:
        idl = _get_idl_for_id(pf_id)
        if idl:
            trade_disc = _get_event_disc(idl, "TradeEvent")
            create_disc = _get_event_disc(idl, "CreateEvent")
            if trade_disc and create_disc:
                registry.register(pf_id, PumpfunParser(pf_id, trade_disc, create_disc))
                LOGGER.info("parser_registered program=pumpfun id=%s", pf_id)
            else:
                LOGGER.warning("pumpfun: missing discriminators trade=%s create=%s", trade_disc, create_disc)

    # ------------------------------------------------------------------ Raydium LaunchLab
    ll_ids = settings._single(settings.program_id_raydium)  # type: ignore[attr-defined]
    # LaunchLab is in the raydium merged set; find it by IDL label
    for pid in settings.tracked_programs.get("raydium", set()):
        idl = _get_idl_for_id(pid)
        if idl and "launchlab" in getattr(idl, "label", "").lower():
            trade_disc = _get_event_disc(idl, "TradeEvent")
            create_disc = _get_event_disc(idl, "PoolCreateEvent")
            if trade_disc and create_disc:
                registry.register(pid, RaydiumLaunchLabParser(pid, trade_disc, create_disc, pool_registry))
                LOGGER.info("parser_registered program=raydium_launchlab id=%s", pid)

    # ------------------------------------------------------------------ Meteora DAMM v2
    mdv2_id = settings.program_id_meteora_damm_v2.strip()
    if mdv2_id:
        idl = _get_idl_for_id(mdv2_id)
        if idl:
            swap_disc = _get_event_disc(idl, "EvtSwap2")
            init_disc = _get_event_disc(idl, "EvtInitializePool")
            if swap_disc and init_disc:
                registry.register(
                    mdv2_id,
                    MeteoraDAMMv2Parser(mdv2_id, swap_disc, init_disc, pool_registry),
                )
                LOGGER.info("parser_registered program=meteora_damm_v2 id=%s", mdv2_id)
            else:
                LOGGER.warning("meteora_damm_v2: missing discriminators swap=%s init=%s", swap_disc, init_disc)

    # ------------------------------------------------------------------ Whirlpool
    wp_id = settings.program_id_whirlpool.strip()
    if wp_id:
        idl = _get_idl_for_id(wp_id)
        if idl:
            traded_disc = _get_event_disc(idl, "Traded")
            init_disc = _get_event_disc(idl, "PoolInitialized")
            if traded_disc and init_disc:
                registry.register(
                    wp_id,
                    WhirlpoolParser(wp_id, traded_disc, init_disc, pool_registry),
                )
                LOGGER.info("parser_registered program=whirlpool id=%s", wp_id)
            else:
                LOGGER.warning("whirlpool: missing discriminators traded=%s init=%s", traded_disc, init_disc)

    # ------------------------------------------------------------------ Raydium CPMM
    for pid in settings.tracked_programs.get("raydium", set()):
        idl = _get_idl_for_id(pid)
        if idl and "cpmm" in getattr(idl, "label", "").lower():
            swap_disc = _get_event_disc(idl, "SwapEvent")
            if swap_disc:
                registry.register(pid, RaydiumCPMMParser(pid, swap_disc))
                LOGGER.info("parser_registered program=raydium_cpmm id=%s", pid)

    return registry
