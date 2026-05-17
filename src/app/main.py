from __future__ import annotations

import asyncio
import logging

from .analytics_sink import AnalyticsSink
from .clickhouse_sink import ClickHouseSink
from .claude_scorer import ClaudeScorer
from .config import Settings
from .event_parsers import EventParserRegistry
from .event_parsers.log_utils import extract_program_data_bytes, extract_signer
from .event_parsers.registry import build_parser_registry
from .idl_registry import IdlRegistry, validate_tracked_programs
from .models import FilteredEvent
from .pool_registry import PoolRegistry
from .profiler import MintProfiler
from .program_filter import extract_signature, extract_slot, find_tracked_program
from .shredstream_client import ShredstreamClient, payload_to_json
from .notifications import send_telegram_message


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

    if not settings.claude_enabled:
        raise RuntimeError("ANTHROPIC_API_KEY is required for Claude scoring")

    if settings.telegram_enabled:
        await send_telegram_message(
            settings.telegram_bot_token,
            settings.telegram_chat_id,
            "Bot started: shredstream ingest pipeline is running.",
        )

    idl_registry = IdlRegistry.from_directory(settings.idl_dir, settings.idl_program_id_map)
    missing_program_ids = validate_tracked_programs(settings.tracked_program_lookup, idl_registry)
    if missing_program_ids:
        msg = "missing_idl_program_ids count=%d ids=%s"
        if settings.validate_idls_on_startup:
            raise RuntimeError(msg % (len(missing_program_ids), ",".join(missing_program_ids)))
        logger.warning(msg, len(missing_program_ids), ",".join(missing_program_ids))

    # ---- structured analytics pipeline --------------------------------
    pool_registry = PoolRegistry()
    analytics_sink = AnalyticsSink(settings)
    await analytics_sink.ensure_schema()

    # Bootstrap pool registry from previously stored pool_creates rows
    await pool_registry.bootstrap_from_clickhouse(
        analytics_sink._client,  # noqa: SLF001 — shared CH client is fine
        settings.clickhouse_database,
    )

    event_parsers = build_parser_registry(idl_registry, pool_registry, settings)
    logger.info("event_parsers_ready programs=%d", len(event_parsers._parsers))  # noqa: SLF001

    # ---- Claude scorer (required) --------------------------------------
    claude_scorer = ClaudeScorer(settings)
    logger.info("claude_scorer_enabled model=%s threshold=%.2f", settings.claude_model, settings.claude_score_threshold)

    # ---- Mint profiler (background task) --------------------------------
    profiler = MintProfiler(settings, claude_scorer=claude_scorer)
    profiler_task = asyncio.create_task(profiler.run_forever(), name="mint_profiler")

    # ---- raw event sink ------------------------------------------------
    sink = ClickHouseSink(settings)
    await sink.ensure_schema()

    client = ShredstreamClient(settings)

    batch: list[FilteredEvent] = []
    batch_size = max(1, settings.batch_size)
    flush_interval = max(0.1, settings.flush_interval_seconds)
    next_flush = loop.time() + flush_interval

    try:
        async for payload in client.stream():
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
                continue

            program_name, program_id = matched
            ingested_at = FilteredEvent.now_utc()

            decoded = idl_registry.decode(program_id, payload)

            # ---- structured event parsing --------------------------------
            signer = extract_signer(payload)
            for log_data in extract_program_data_bytes(payload):
                result = event_parsers.parse(
                    program_id, log_data, slot, signature, signer, ingested_at
                )
                if result is None:
                    continue
                from .event_parsers.base import ParsedCreate, ParsedSwap
                if isinstance(result, ParsedSwap) and result.mint:
                    analytics_sink.buffer_swap(result)
                elif isinstance(result, ParsedCreate):
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
                logger.info("batch_flushed size=%d analytics_pending=%d", len(batch), analytics_sink.pending)
                batch.clear()
                next_flush = now + flush_interval
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
