"""MintProfiler — background task that computes per-mint behavioural metrics.

Workflow (runs every ``profile_check_interval_seconds``):
  1. Query ``pool_creates`` for mints older than ``profile_delay_minutes``
     that have no row in ``mint_profiles`` yet.
  2. For each pending mint, run a batch of ClickHouse queries to extract:
       - Price at T+1 min, T+5 min, T+15 min, T+1 hr
       - Volume (quote token) in each window
       - Trade count, buy count, sell count in first 5 min
       - Unique buyers in first 5 min and 15 min
       - Buy/sell ratio in first 5 min
       - Wallet concentration (top-5-wallet share of volume)
       - Peak price multiplier over first 4 hours
  3. Write a ``mint_profiles`` row.
  4. Update ``creator_profiles`` for the creator.
  5. If a ClaudeScorer is configured, trigger async scoring.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import clickhouse_connect

from .config import Settings

LOGGER = logging.getLogger(__name__)

# ------------------------------------------------------------------
# SQL helpers  (uses ClickHouse named-param syntax {name:Type})
# ------------------------------------------------------------------

_Q_PENDING_MINTS = """
SELECT
    pc.mint,
    pc.pool_address,
    pc.program_name,
    pc.creator,
    pc.symbol,
    pc.name,
    pc.initial_price,
    pc.ingested_at
FROM {db}.pool_creates AS pc
WHERE pc.mint != ''
  AND pc.ingested_at <= now() - INTERVAL {delay} MINUTE
  AND pc.ingested_at >= now() - INTERVAL {max_age} MINUTE
  AND pc.mint NOT IN (SELECT mint FROM {db}.mint_profiles)
LIMIT {limit}
"""

_Q_SWAP_WINDOW = """
SELECT
    count()                                             AS trade_count,
    countIf(direction = 'buy')                          AS buy_count,
    countIf(direction = 'sell')                         AS sell_count,
    uniqExact(signer)                                   AS unique_buyers,
    sum(if(direction = 'buy', amount_in, amount_out))   AS volume_quote,
    max(price)                                          AS max_price,
    argMin(price, ingested_at)                          AS first_price,
    argMax(price, ingested_at)                          AS last_price
FROM {db}.swaps
WHERE mint = {{mint:String}}
  AND price > 0
  AND ingested_at BETWEEN {{start:DateTime64(3)}} AND {{end:DateTime64(3)}}
"""

_Q_PRICE_AT = """
SELECT price
FROM {db}.swaps
WHERE mint = {{mint:String}}
  AND price > 0
  AND ingested_at >= {{after:DateTime64(3)}}
ORDER BY ingested_at ASC
LIMIT 1
"""

_Q_PEAK_PRICE = """
SELECT max(price) AS peak
FROM {db}.swaps
WHERE mint = {{mint:String}}
  AND price > 0
  AND ingested_at BETWEEN {{start:DateTime64(3)}} AND {{end:DateTime64(3)}}
"""

_Q_WALLET_CONCENTRATION = """
SELECT
    arraySum(arraySlice(
        arrayReverseSort(groupArray(wallet_vol)),
        1, 5
    )) / greatest(total_vol, 1) AS top5_share
FROM (
    SELECT
        signer,
        sum(if(direction = 'buy', amount_in, amount_out)) AS wallet_vol
    FROM {db}.swaps
    WHERE mint = {{mint:String}}
      AND signer != ''
      AND ingested_at BETWEEN {{start:DateTime64(3)}} AND {{end:DateTime64(3)}}
    GROUP BY signer
) AS t
CROSS JOIN (
    SELECT sum(if(direction = 'buy', amount_in, amount_out)) AS total_vol
    FROM {db}.swaps
    WHERE mint = {{mint:String}}
      AND ingested_at BETWEEN {{start:DateTime64(3)}} AND {{end:DateTime64(3)}}
) AS tv
"""

_Q_UPDATE_CREATOR = """
INSERT INTO {db}.creator_profiles
SELECT
    creator,
    count()                                        AS total_tokens,
    avgOrNull(peak_multiplier)                     AS avg_peak_mult,
    quantileOrNull(0.5)(peak_multiplier)           AS median_peak_mult,
    countIf(peak_multiplier >= 5) / count()        AS pct_pumped,
    sumOrNull(volume_quote_1h)                     AS total_volume,
    max(created_at)                                AS last_token_at,
    now()                                          AS updated_at
FROM {db}.mint_profiles
WHERE creator = {{creator:String}}
  AND peak_multiplier IS NOT NULL
GROUP BY creator
"""


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


class MintProfiler:
    def __init__(self, settings: Settings, claude_scorer: object | None = None) -> None:
        self._db = settings.clickhouse_database
        self._delay_min = settings.profile_delay_minutes
        self._max_age_min = settings.profile_max_age_minutes
        self._check_interval = settings.profile_check_interval_seconds
        self._claude_scorer = claude_scorer
        self._client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_username,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    async def run_forever(self) -> None:
        LOGGER.info(
            "profiler_started delay_min=%d check_interval=%ds",
            self._delay_min,
            self._check_interval,
        )
        while True:
            try:
                await self._profile_pending_mints()
            except Exception:
                LOGGER.exception("profiler_cycle_error")
            await asyncio.sleep(self._check_interval)

    # ------------------------------------------------------------------

    async def _profile_pending_mints(self) -> None:
        query = _Q_PENDING_MINTS.format(
            db=self._db,
            delay=self._delay_min,
            max_age=self._max_age_min,
            limit=50,
        )
        rows = await asyncio.to_thread(self._client.query, query)
        if not rows.result_rows:
            return

        LOGGER.info("profiler_pending mints=%d", len(rows.result_rows))
        for row in rows.result_rows:
            mint, pool_addr, program_name, creator, symbol, name, initial_price, created_at = row
            created_at = _utc(created_at)
            try:
                await self._profile_one(
                    mint, pool_addr, program_name, creator, symbol, name,
                    initial_price, created_at,
                )
            except Exception:
                LOGGER.exception("profiler_error mint=%s", mint)

    async def _profile_one(
        self,
        mint: str,
        pool_address: str,
        program_name: str,
        creator: str,
        symbol: str | None,
        name: str | None,
        initial_price: float | None,
        created_at: datetime,
    ) -> None:
        now = datetime.now(timezone.utc)
        db = self._db

        def q(sql: str, params: dict | None = None) -> list:
            return self._client.query(sql, parameters=params or {}).result_rows

        def ts(dt: datetime) -> str:
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]

        t = created_at
        t1m = t + timedelta(minutes=1)
        t5m = t + timedelta(minutes=5)
        t15m = t + timedelta(minutes=15)
        t1h = t + timedelta(hours=1)
        t4h = t + timedelta(hours=4)

        # ---- price at windows ----------------------------------------
        def price_at(after: datetime) -> float | None:
            rows = q(
                _Q_PRICE_AT.format(db=db),
                {"mint": mint, "after": ts(after)},
            )
            return rows[0][0] if rows and rows[0][0] else None

        # ---- swap-window metrics --------------------------------------
        def window_metrics(start: datetime, end: datetime) -> dict:
            rows = q(
                _Q_SWAP_WINDOW.format(db=db),
                {"mint": mint, "start": ts(start), "end": ts(end)},
            )
            if not rows:
                return {}
            r = rows[0]
            return {
                "trade_count": int(r[0] or 0),
                "buy_count": int(r[1] or 0),
                "sell_count": int(r[2] or 0),
                "unique_buyers": int(r[3] or 0),
                "volume_quote": float(r[4] or 0),
                "max_price": float(r[5] or 0),
                "first_price": float(r[6] or 0),
                "last_price": float(r[7] or 0),
            }

        # ---- wallet concentration -------------------------------------
        def wallet_conc(start: datetime, end: datetime) -> float | None:
            rows = q(
                _Q_WALLET_CONCENTRATION.format(db=db),
                {"mint": mint, "start": ts(start), "end": ts(end)},
            )
            if rows and rows[0][0] is not None:
                return float(rows[0][0])
            return None

        # ---- peak price over 4 hrs -----------------------------------
        def peak_price(start: datetime, end: datetime) -> float | None:
            rows = q(
                _Q_PEAK_PRICE.format(db=db),
                {"mint": mint, "start": ts(start), "end": ts(end)},
            )
            return float(rows[0][0]) if rows and rows[0][0] else None

        # Run all queries concurrently
        (
            p5m, p15m, p1h,
            m5m, m15m, m1h,
            conc5m,
            peak,
        ) = await asyncio.gather(
            asyncio.to_thread(price_at, t5m),
            asyncio.to_thread(price_at, t15m),
            asyncio.to_thread(price_at, t1h),
            asyncio.to_thread(window_metrics, t, t5m),
            asyncio.to_thread(window_metrics, t, t15m),
            asyncio.to_thread(window_metrics, t, t1h),
            asyncio.to_thread(wallet_conc, t, t5m),
            asyncio.to_thread(peak_price, t, t4h),
        )

        # ---- derived metrics -----------------------------------------
        effective_initial = initial_price or m5m.get("first_price") or 0.0
        peak_mult: float | None = None
        if effective_initial and effective_initial > 0 and peak:
            peak_mult = peak / effective_initial

        buy_sell_5m: float | None = None
        if m5m.get("sell_count", 0) > 0:
            buy_sell_5m = m5m.get("buy_count", 0) / m5m["sell_count"]
        elif m5m.get("buy_count", 0) > 0:
            buy_sell_5m = float(m5m["buy_count"])  # infinite → use buy count

        # ---- creator history -----------------------------------------
        creator_rows = q(
            f"SELECT total_tokens, avg_peak_mult FROM {db}.creator_profiles "
            "FINAL WHERE creator = {creator:String}",
            {"creator": creator},
        )
        creator_past = int(creator_rows[0][0]) if creator_rows else 0
        creator_avg_mult = float(creator_rows[0][1]) if creator_rows else None

        # ---- write mint_profiles -------------------------------------
        now_str = ts(datetime.now(timezone.utc))
        self._client.insert(
            f"{db}.mint_profiles",
            [[
                mint,
                ts(created_at),
                program_name,
                creator,
                symbol,
                name,
                initial_price if initial_price else effective_initial or None,
                p5m,
                p15m,
                p1h,
                peak,
                peak_mult,
                m5m.get("volume_quote"),
                m15m.get("volume_quote"),
                m1h.get("volume_quote"),
                m5m.get("trade_count"),
                buy_sell_5m,
                m5m.get("unique_buyers"),
                m15m.get("unique_buyers"),
                conc5m,
                creator_past if creator_past else None,
                creator_avg_mult,
                None,   # outcome_label — labelled later
                now_str,
            ]],
            column_names=[
                "mint", "created_at", "program_name", "creator",
                "symbol", "name",
                "initial_price", "price_5m", "price_15m", "price_1h",
                "price_peak", "peak_multiplier",
                "volume_quote_5m", "volume_quote_15m", "volume_quote_1h",
                "trade_count_5m", "buy_sell_ratio_5m",
                "unique_buyers_5m", "unique_buyers_15m",
                "wallet_concentration_5m",
                "creator_past_tokens", "creator_avg_peak_mult",
                "outcome_label", "updated_at",
            ],
        )
        LOGGER.info(
            "mint_profiled mint=%s program=%s peak_mult=%.2f trade5m=%d",
            mint, program_name, peak_mult or 0, m5m.get("trade_count", 0),
        )

        # ---- update creator_profiles ---------------------------------
        if creator:
            try:
                self._client.command(
                    _Q_UPDATE_CREATOR.format(db=db),
                    parameters={"creator": creator},
                )
            except Exception:
                LOGGER.debug("creator_profile_update_failed creator=%s", creator, exc_info=True)

        # ---- Claude scoring ------------------------------------------
        if self._claude_scorer is not None:
            profile_dict = {
                "mint": mint,
                "program": program_name,
                "symbol": symbol,
                "initial_price": initial_price or effective_initial,
                "price_5m": p5m,
                "price_15m": p15m,
                "price_1h": p1h,
                "peak_multiplier": peak_mult,
                "volume_quote_5m": m5m.get("volume_quote"),
                "trade_count_5m": m5m.get("trade_count"),
                "buy_sell_ratio_5m": buy_sell_5m,
                "unique_buyers_5m": m5m.get("unique_buyers"),
                "wallet_concentration_5m": conc5m,
                "creator_past_tokens": creator_past,
                "creator_avg_peak_mult": creator_avg_mult,
                "profile_age_minutes": int(
                    (datetime.now(timezone.utc) - created_at).total_seconds() / 60
                ),
            }
            asyncio.create_task(
                self._claude_scorer.score_and_store(mint, profile_dict, db, self._client)  # type: ignore[attr-defined]
            )

    def close(self) -> None:
        self._client.close()
