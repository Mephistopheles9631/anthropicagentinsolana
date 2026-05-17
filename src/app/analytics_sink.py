"""AnalyticsSink — writes structured swap/create events to dedicated ClickHouse tables.

Tables created here:
  - ``swaps``       : one row per decoded swap across all protocols
  - ``pool_creates`` : one row per pool / token launch event
  - ``mint_ohlcv``   : 1-minute OHLCV candles, populated by a materialized view
  - ``mint_profiles`` : per-mint behavioural fingerprints (written by the background
                        profiler job, schema created here for readiness)
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

import clickhouse_connect

from .config import Settings
from .event_parsers.base import ParsedCreate, ParsedSwap

LOGGER = logging.getLogger(__name__)

# ClickHouse NULL sentinel for nullable columns
_NULL = None


class AnalyticsSink:
    def __init__(self, settings: Settings) -> None:
        self._db = settings.clickhouse_database
        self._client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_username,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
        self._swap_buf: list[ParsedSwap] = []
        self._create_buf: list[ParsedCreate] = []

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    async def ensure_schema(self) -> None:
        await asyncio.to_thread(self._create_tables)

    def _create_tables(self) -> None:
        db = self._db

        # ---- swaps -------------------------------------------------------
        self._client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.swaps (
            ingested_at         DateTime64(3, 'UTC'),
            slot                Nullable(UInt64),
            signature           Nullable(String),
            program_name        LowCardinality(String),
            program_id          String,
            pool_address        String,
            mint                String,
            quote_mint          String,
            direction           LowCardinality(String),
            amount_in           UInt64,
            amount_out          UInt64,
            price               Float64,
            price_sol           Nullable(Float64),
            real_base_reserve   Nullable(UInt64),
            real_quote_reserve  Nullable(UInt64),
            fee_amount          UInt64,
            signer              Nullable(String)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(ingested_at)
        ORDER BY (mint, ingested_at)
        """)

        # ---- pool_creates ------------------------------------------------
        self._client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.pool_creates (
            ingested_at              DateTime64(3, 'UTC'),
            slot                     Nullable(UInt64),
            signature                Nullable(String),
            program_name             LowCardinality(String),
            program_id               String,
            pool_address             String,
            mint                     String,
            quote_mint               String,
            creator                  String,
            name                     Nullable(String),
            symbol                   Nullable(String),
            uri                      Nullable(String),
            decimals                 Nullable(UInt8),
            initial_price            Nullable(Float64),
            initial_supply           Nullable(UInt64),
            initial_liquidity_base   Nullable(UInt64),
            initial_liquidity_quote  Nullable(UInt64)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(ingested_at)
        ORDER BY (ingested_at, mint)
        """)

        # ---- mint_ohlcv (1-minute candles via materialized view) ----------
        self._client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.mint_ohlcv (
            bucket          DateTime,
            mint            String,
            quote_mint      String,
            open            AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
            high            SimpleAggregateFunction(max, Float64),
            low             SimpleAggregateFunction(min, Float64),
            close           AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
            volume_in       SimpleAggregateFunction(sum, UInt64),
            volume_out      SimpleAggregateFunction(sum, UInt64),
            trade_count     SimpleAggregateFunction(sum, UInt64),
            buy_count       SimpleAggregateFunction(sum, UInt64),
            sell_count      SimpleAggregateFunction(sum, UInt64)
        )
        ENGINE = AggregatingMergeTree
        PARTITION BY toYYYYMMDD(bucket)
        ORDER BY (mint, bucket)
        """)

        self._client.command(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.mint_ohlcv_mv
        TO {db}.mint_ohlcv
        AS
        SELECT
            toStartOfMinute(ingested_at) AS bucket,
            mint,
            quote_mint,
            argMinState(price, ingested_at)  AS open,
            max(price)                       AS high,
            min(price)                       AS low,
            argMaxState(price, ingested_at)  AS close,
            sum(amount_in)                   AS volume_in,
            sum(amount_out)                  AS volume_out,
            count()                          AS trade_count,
            countIf(direction = 'buy')       AS buy_count,
            countIf(direction = 'sell')      AS sell_count
        FROM {db}.swaps
        WHERE price > 0
        GROUP BY bucket, mint, quote_mint
        """)

        # ---- mint_profiles (populated by background profiler) ------------
        self._client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.mint_profiles (
            mint                    String,
            created_at              DateTime64(3, 'UTC'),
            program_name            LowCardinality(String),
            creator                 String,
            symbol                  Nullable(String),
            name                    Nullable(String),
            initial_price           Nullable(Float64),
            price_5m                Nullable(Float64),
            price_15m               Nullable(Float64),
            price_1h                Nullable(Float64),
            price_peak              Nullable(Float64),
            peak_multiplier         Nullable(Float64),
            volume_quote_5m         Nullable(Float64),
            volume_quote_15m        Nullable(Float64),
            volume_quote_1h         Nullable(Float64),
            trade_count_5m          Nullable(UInt32),
            buy_sell_ratio_5m       Nullable(Float32),
            unique_buyers_5m        Nullable(UInt32),
            unique_buyers_15m       Nullable(UInt32),
            wallet_concentration_5m Nullable(Float32),
            creator_past_tokens     Nullable(UInt16),
            creator_avg_peak_mult   Nullable(Float32),
            outcome_label           Nullable(String),
            updated_at              DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY mint
        """)

        # ---- creator_profiles -------------------------------------------
        self._client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.creator_profiles (
            creator             String,
            total_tokens        UInt32,
            avg_peak_mult       Float32,
            median_peak_mult    Float32,
            pct_pumped          Float32,
            total_volume        Float64,
            last_token_at       DateTime64(3, 'UTC'),
            updated_at          DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY creator
        """)

        # ---- mint_scores (Claude AI assessments) ------------------------
        self._client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.mint_scores (
            mint                String,
            scored_at           DateTime64(3, 'UTC'),
            model               String,
            score               Float32,
            confidence          LowCardinality(String),
            positive_signals    Array(String),
            negative_signals    Array(String),
            recommendation      String,
            profile_age_minutes UInt16,
            raw_response        String
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(scored_at)
        ORDER BY (mint, scored_at)
        """)

        LOGGER.info("analytics_schema_ready db=%s", db)

    # ------------------------------------------------------------------
    # Buffering
    # ------------------------------------------------------------------

    def buffer_swap(self, swap: ParsedSwap) -> None:
        self._swap_buf.append(swap)

    def buffer_create(self, create: ParsedCreate) -> None:
        self._create_buf.append(create)

    @property
    def pending(self) -> int:
        return len(self._swap_buf) + len(self._create_buf)

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    async def flush(self) -> None:
        swaps, self._swap_buf = self._swap_buf, []
        creates, self._create_buf = self._create_buf, []
        if swaps:
            await asyncio.to_thread(self._insert_swaps, swaps)
            LOGGER.info("swaps_flushed count=%d", len(swaps))
        if creates:
            await asyncio.to_thread(self._insert_creates, creates)
            LOGGER.info("creates_flushed count=%d", len(creates))

    def _insert_swaps(self, swaps: Sequence[ParsedSwap]) -> None:
        rows = [
            [
                s.ingested_at,
                s.slot,
                s.signature,
                s.program_name,
                s.program_id,
                s.pool_address,
                s.mint,
                s.quote_mint,
                s.direction,
                s.amount_in,
                s.amount_out,
                s.price,
                s.price_sol,
                s.real_base_reserve,
                s.real_quote_reserve,
                s.fee_amount,
                s.signer,
            ]
            for s in swaps
        ]
        self._client.insert(
            f"{self._db}.swaps",
            rows,
            column_names=[
                "ingested_at", "slot", "signature", "program_name", "program_id",
                "pool_address", "mint", "quote_mint", "direction",
                "amount_in", "amount_out", "price", "price_sol",
                "real_base_reserve", "real_quote_reserve", "fee_amount", "signer",
            ],
        )

    def _insert_creates(self, creates: Sequence[ParsedCreate]) -> None:
        rows = [
            [
                c.ingested_at,
                c.slot,
                c.signature,
                c.program_name,
                c.program_id,
                c.pool_address,
                c.mint,
                c.quote_mint,
                c.creator,
                c.name,
                c.symbol,
                c.uri,
                c.decimals,
                c.initial_price,
                c.initial_supply,
                c.initial_liquidity_base,
                c.initial_liquidity_quote,
            ]
            for c in creates
        ]
        self._client.insert(
            f"{self._db}.pool_creates",
            rows,
            column_names=[
                "ingested_at", "slot", "signature", "program_name", "program_id",
                "pool_address", "mint", "quote_mint", "creator",
                "name", "symbol", "uri", "decimals",
                "initial_price", "initial_supply",
                "initial_liquidity_base", "initial_liquidity_quote",
            ],
        )

    def close(self) -> None:
        self._client.close()
