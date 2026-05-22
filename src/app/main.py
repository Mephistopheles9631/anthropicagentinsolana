from __future__ import annotations

import asyncio
import logging

from .analytics_sink import AnalyticsSink
from .clickhouse_sink import ClickHouseSink
from .claude_scorer import ClaudeScorer
from .config import Settings
from .event_parsers.log_utils import extract_program_data_bytes, extract_signer
from .event_parsers.registry import build_parser_registry
from .idl_registry import IdlRegistry, validate_tracked_programs
from .models import FilteredEvent
from .pool_registry import PoolRegistry
from .profiler import MintProfiler
from .program_filter import extract_signature, extract_slot, find_tracked_program
from .shredstream_client import ShredstreamClient, payload_to_json
from .notifications import send_broadcast_message
from .entry_decoder import EntryDecoder
from .instruction_parsers.pumpfun_ix import PumpfunInstructionParser
from .instruction_parsers.pumpswap_ix import PumpswapInstructionParser


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def run() -> None:
    settings = Settings()
    logger = logging.getLogger("app")
    loop = asyncio.get_running_loop()

    if not settings.tracked_programs:
        logger.warning("no_program_ids_configured; set PROGRAM_ID_* values in .env")
    invalid_program_ids = settings.invalid_tracked_program_ids
    if invalid_program_ids:
        logger.warning(
            "invalid_program_ids_configured count=%d ids=%s",
            len(invalid_program_ids),
            ",".join(invalid_program_ids),
        )

    if settings.telegram_enabled or settings.discord_enabled:
        await send_broadcast_message(
            "Bot started: shredstream ingest pipeline is running.",
            telegram_token=settings.telegram_bot_token,
            telegram_chat_id=settings.telegram_chat_id,
            discord_webhook_url=settings.discord_webhook_url,
        )

    idl_registry = IdlRegistry.from_directory(settings.idl_dir, settings.idl_program_id_map)
    missing_program_ids = validate_tracked_programs(settings.tracked_program_lookup, idl_registry)
    if missing_program_ids:
        msg = "missing_idl_program_ids count=%d ids=%s"
        if settings.validate_idls_on_startup:
            raise RuntimeError(msg % (len(missing_program_ids), ",".join(missing_program_ids)))
        logger.warning(msg, len(missing_program_ids), ",".join(missing_program_ids))

    pool_registry = PoolRegistry()
    analytics_sink = AnalyticsSink(settings)
    await analytics_sink.ensure_schema()

    await pool_registry.bootstrap_from_clickhouse(
        analytics_sink._client,  # noqa: SLF001
        settings.clickhouse_database,
    )

    event_parsers = build_parser_registry(idl_registry, pool_registry, settings)
    logger.info("event_parsers_ready programs=%d", len(event_parsers._parsers))  # noqa: SLF001

    claude_scorer: ClaudeScorer | None = None
    if settings.claude_enabled:
        claude_scorer = ClaudeScorer(settings)
        logger.info(
            "claude_scorer_enabled model=%s threshold=%.2f",
            settings.claude_model,
            settings.claude_score_threshold,
        )
    else:
        logger.warning("claude_scorer_disabled reason=missing_api_key")

    profiler = MintProfiler(settings, claude_scorer=claude_scorer)
    profiler_task = asyncio.create_task(profiler.run_forever(), name="mint_profiler")

    sink = ClickHouseSink(settings)
    await sink.ensure_schema()

    client = ShredstreamClient(settings)
    entry_decoder = EntryDecoder(settings)
    entry_decoder_available = entry_decoder.is_available()
    if not entry_decoder_available:
        logger.warning("entry_decoder_unavailable path=%s", settings.entry_decoder_path)

    pumpfun_id = settings.program_id_pumpfun.strip()
    pumpfun_ix_parser: PumpfunInstructionParser | None = None
    if pumpfun_id:
        pf_idl = idl_registry._by_program_id.get(pumpfun_id)  # noqa: SLF001
        if pf_idl is not None:
            pumpfun_ix_parser = PumpfunInstructionParser(pumpfun_id, pf_idl)
            logger.info("instruction_parser_ready program=pumpfun")
        else:
            logger.warning("instruction_parser_missing program=pumpfun id=%s", pumpfun_id)

    pumpswap_id = settings.program_id_pumpswap.strip()
    pumpswap_ix_parser: PumpswapInstructionParser | None = None
    if pumpswap_id:
        pumpswap_ix_parser = PumpswapInstructionParser(pumpswap_id, pool_registry)
        logger.info("instruction_parser_ready program=pumpswap")

    batch: list[FilteredEvent] = []
    batch_size = max(1, settings.batch_size)
    flush_interval = max(0.1, settings.flush_interval_seconds)
    next_flush = loop.time() + flush_interval
    counters = {
        "payloads_seen": 0,
        "tracked_payloads": 0,
        "swaps_parsed": 0,
        "creates_parsed": 0,
        "parse_failures": 0,
    }

    async def process_payload(payload: dict, instruction_only: bool = False) -> None:
        nonlocal next_flush
        counters["payloads_seen"] += 1

        slot = extract_slot(payload)
        signature = extract_signature(payload)
        logger.debug("shredstream_entry_received slot=%s signature=%s", slot, signature)

        matched = find_tracked_program(payload, settings.tracked_programs)
        if matched is None:
            now = loop.time()
            if batch and now >= next_flush:
                await sink.insert_batch(batch)
                await analytics_sink.flush()
                logger.info("batch_flushed size=%d", len(batch))
                batch.clear()
                next_flush = now + flush_interval
            return

        program_name, program_id = matched
        counters["tracked_payloads"] += 1
        ingested_at = FilteredEvent.now_utc()
        decoded = idl_registry.decode(program_id, payload)

        signer = extract_signer(payload)
        log_chunks = extract_program_data_bytes(payload)
        if instruction_only and not log_chunks:
            analytics_sink.buffer_instruction_hit(
                program_name, program_id, slot, signature, ingested_at
            )
            logger.info(
                "instruction_hit program=%s program_id=%s slot=%s signature=%s",
                program_name,
                program_id,
                slot,
                signature,
            )
            now = loop.time()
            if now >= next_flush:
                await analytics_sink.flush()
                logger.info("analytics_flushed analytics_pending=%d", analytics_sink.pending)
                next_flush = now + flush_interval
            return

        for log_data in log_chunks:
            result, parser_errors = event_parsers.parse_with_diagnostics(
                program_id,
                log_data,
                slot,
                signature,
                signer,
                ingested_at,
            )
            if result is None:
                if parser_errors:
                    counters["parse_failures"] += 1
                    analytics_sink.buffer_parse_failure(
                        ingested_at,
                        slot,
                        signature,
                        program_name,
                        program_id,
                        parser_errors,
                        log_data.hex(),
                    )
                continue

            from .event_parsers.base import ParsedCreate, ParsedSwap

            if isinstance(result, ParsedSwap) and result.mint:
                counters["swaps_parsed"] += 1
                logger.info(
                    "swap_detected program=%s mint=%s pool=%s slot=%s signature=%s",
                    program_name,
                    result.mint,
                    result.pool_address,
                    slot,
                    signature,
                )
                analytics_sink.buffer_swap(result)
            elif isinstance(result, ParsedCreate):
                counters["creates_parsed"] += 1
                logger.info(
                    "mint_detected program=%s mint=%s pool=%s slot=%s signature=%s",
                    program_name,
                    result.mint,
                    result.pool_address,
                    slot,
                    signature,
                )
                analytics_sink.buffer_create(result)
                if result.pool_address and result.mint and result.quote_mint:
                    pool_registry.update(result.pool_address, result.mint, result.quote_mint)

        batch.append(
            FilteredEvent(
                program_name=program_name,
                program_id=program_id,
                idl_label=decoded.idl_label,
                idl_instruction=decoded.instruction_name,
                idl_event=decoded.event_name,
                slot=slot,
                signature=signature,
                source="shredstream",
                raw_json=payload_to_json(payload),
                ingested_at=ingested_at,
            )
        )

        now = loop.time()
        if len(batch) >= batch_size or now >= next_flush:
            await sink.insert_batch(batch)
            await analytics_sink.flush()
            logger.info(
                "batch_flushed size=%d analytics_pending=%d payloads_seen=%d tracked_payloads=%d swaps_parsed=%d creates_parsed=%d parse_failures=%d",
                len(batch),
                analytics_sink.pending,
                counters["payloads_seen"],
                counters["tracked_payloads"],
                counters["swaps_parsed"],
                counters["creates_parsed"],
                counters["parse_failures"],
            )
            batch.clear()
            next_flush = now + flush_interval

    try:
        async for payload in client.stream():
            if isinstance(payload, dict) and "entries" in payload and "transaction" not in payload:
                entries_b64 = payload.get("entries")
                if isinstance(entries_b64, str) and entries_b64 and entry_decoder_available:
                    decoded_txs = await entry_decoder.decode_transactions(entries_b64)
                    if decoded_txs:
                        logger.info("entry_transactions_decoded count=%d", len(decoded_txs))
                        for item in decoded_txs:
                            signature = item.get("signature")
                            instructions = item.get("instructions") or []
                            for inst in instructions:
                                if not isinstance(inst, dict):
                                    continue
                                program_id = inst.get("program_id")
                                data_hex = inst.get("data_hex") or ""
                                accounts = inst.get("accounts") or []
                                if not isinstance(program_id, str):
                                    continue
                                if program_id in settings.tracked_program_lookup:
                                    program_name = settings.tracked_program_lookup[program_id]
                                    prefix = data_hex[:16]
                                    logger.info(
                                        "swap_candidate program=%s program_id=%s slot=%s signature=%s data_prefix=%s",
                                        program_name,
                                        program_id,
                                        payload.get("slot"),
                                        signature,
                                        prefix,
                                    )
                                    if program_id == pumpfun_id and pumpfun_ix_parser is not None:
                                        from .event_parsers.base import ParsedCreate, ParsedSwap

                                        if not isinstance(accounts, list):
                                            accounts = []
                                        ingested_at = FilteredEvent.now_utc()
                                        parsed = pumpfun_ix_parser.try_parse(
                                            data_hex,
                                            accounts,
                                            payload.get("slot"),
                                            signature,
                                            ingested_at,
                                        )
                                        if isinstance(parsed, ParsedSwap):
                                            logger.info(
                                                "swap_detected program=%s mint=%s pool=%s slot=%s signature=%s source=instruction",
                                                program_name,
                                                parsed.mint,
                                                parsed.pool_address,
                                                payload.get("slot"),
                                                signature,
                                            )
                                            analytics_sink.buffer_swap(parsed)
                                        elif isinstance(parsed, ParsedCreate):
                                            logger.info(
                                                "mint_detected program=%s mint=%s pool=%s slot=%s signature=%s source=instruction",
                                                program_name,
                                                parsed.mint,
                                                parsed.pool_address,
                                                payload.get("slot"),
                                                signature,
                                            )
                                            analytics_sink.buffer_create(parsed)
                                            if parsed.pool_address and parsed.mint and parsed.quote_mint:
                                                pool_registry.update(parsed.pool_address, parsed.mint, parsed.quote_mint)
                                    if program_id == pumpswap_id and pumpswap_ix_parser is not None:
                                        from .event_parsers.base import ParsedSwap

                                        if not isinstance(accounts, list):
                                            accounts = []
                                        ingested_at = FilteredEvent.now_utc()
                                        parsed = pumpswap_ix_parser.try_parse(
                                            data_hex,
                                            accounts,
                                            payload.get("slot"),
                                            signature,
                                            ingested_at,
                                        )
                                        if isinstance(parsed, ParsedSwap):
                                            logger.info(
                                                "swap_detected program=%s mint=%s pool=%s slot=%s signature=%s source=instruction",
                                                program_name,
                                                parsed.mint,
                                                parsed.pool_address,
                                                payload.get("slot"),
                                                signature,
                                            )
                                            analytics_sink.buffer_swap(parsed)
                                tx_payload = {
                                    "slot": payload.get("slot"),
                                    "signature": signature,
                                    "program_id": program_id,
                                }
                                await process_payload(tx_payload, instruction_only=True)
                elif isinstance(entries_b64, str) and entries_b64 and not entry_decoder_available:
                    logger.debug("entry_decoder_skip_unavailable")
                continue

            await process_payload(payload)
    finally:
        profiler_task.cancel()
        try:
            await profiler_task
        except asyncio.CancelledError:
            pass
        profiler.close()
        if batch:
            await sink.insert_batch(batch)
            logger.info("batch_flushed size=%d", len(batch))
        await analytics_sink.flush()
        sink.close()
        analytics_sink.close()


if __name__ == "__main__":
    configure_logging()
    asyncio.run(run())
