from __future__ import annotations

import asyncio
from collections.abc import Sequence

import clickhouse_connect

from .config import Settings
from .models import FilteredEvent


class ClickHouseSink:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_username,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    async def ensure_schema(self) -> None:
        table = self._settings.clickhouse_table
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            ingested_at DateTime64(3, 'UTC'),
            slot Nullable(UInt64),
            signature Nullable(String),
            program_name LowCardinality(String),
            program_id String,
            idl_label Nullable(String),
            idl_instruction Nullable(String),
            idl_event Nullable(String),
            source String,
            raw_json String
        )
        ENGINE = MergeTree
        ORDER BY (program_name, ingested_at)
        """
        await asyncio.to_thread(self._client.command, ddl)

    async def insert_batch(self, events: Sequence[FilteredEvent]) -> None:
        if not events:
            return

        table = self._settings.clickhouse_table
        rows = [
            [
                event.ingested_at,
                event.slot,
                event.signature,
                event.program_name,
                event.program_id,
                event.idl_label,
                event.idl_instruction,
                event.idl_event,
                event.source,
                event.raw_json,
            ]
            for event in events
        ]
        await asyncio.to_thread(
            self._client.insert,
            table,
            rows,
            column_names=[
                "ingested_at",
                "slot",
                "signature",
                "program_name",
                "program_id",
                "idl_label",
                "idl_instruction",
                "idl_event",
                "source",
                "raw_json",
            ],
        )

    def close(self) -> None:
        self._client.close()
