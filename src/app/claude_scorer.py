"""ClaudeScorer — uses Anthropic Claude to score new mints against historical patterns.

Flow:
  1. Fetch top-N historically pumped mints from ``mint_profiles`` as positive examples.
  2. Fetch bottom-N mints as negative examples.
  3. Build a compact JSON prompt containing the new mint's behavioural profile.
  4. Call Claude with tool_use to get a structured JSON response.
  5. Store the score in ``mint_scores``.
  6. Emit a prominent log alert if score >= configured threshold.

The scorer is optional — if ``ANTHROPIC_API_KEY`` is not set the profiler
simply skips calling it.
"""
from __future__ import annotations

import json
import logging
import asyncio
from datetime import datetime, timezone

import anthropic

from .config import Settings
from .dexscreener import DexScreenerClient, DexScreenerSnapshot
from .notifications import send_broadcast_message

LOGGER = logging.getLogger(__name__)

_ANALYSIS_TOOL = {
    "name": "report_mint_analysis",
    "description": (
        "Report a structured analysis of a new Solana token's likelihood of "
        "reaching 5x its launch price within 2 hours."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "score": {
                "type": "number",
                "description": "Probability 0.0–1.0 that this token reaches 5x in 2 hours.",
            },
            "confidence": {
                "type": "string",
                "enum": ["low", "medium", "high"],
                "description": "Confidence in the score estimate.",
            },
            "positive_signals": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Key bullish signals observed in the profile.",
            },
            "negative_signals": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Key bearish or risky signals observed.",
            },
            "recommendation": {
                "type": "string",
                "description": "One-sentence action recommendation.",
            },
        },
        "required": ["score", "confidence", "positive_signals", "negative_signals", "recommendation"],
    },
}

_SYSTEM_PROMPT = """\
You are a Solana memecoin trading analyst with deep expertise in on-chain \
behavioural patterns. You receive a new token's early behavioural profile \
(extracted from real-time DEX swap data) and compare it to historical examples \
of tokens that pumped (5x+ in 2 hours) vs tokens that failed.

Focus on these signals (in order of importance):
1. Buy/sell ratio in first 5 minutes — >3 is very bullish
2. Unique buyers in first 5 minutes — >20 suggests organic interest
3. Wallet concentration — top-5 wallets owning >60% of volume = wash risk
4. Volume in first 5 minutes — higher = more momentum
5. Creator history — past pumps by same creator are strong signal
6. Price trajectory — price_5m / initial_price trend

Respond using the report_mint_analysis tool only. Be concise in signals.\
"""


class ClaudeScorer:
    def __init__(self, settings: Settings) -> None:
        self._model = settings.claude_model
        self._threshold = settings.claude_score_threshold
        self._max_examples = settings.claude_max_examples
        self._retry_attempts = max(1, settings.claude_retry_attempts)
        self._retry_max_wait_seconds = max(1, settings.claude_retry_max_wait_seconds)
        self._alert_dedup_minutes = max(1, settings.claude_alert_dedup_minutes)
        self._opportunity_min_confidence = settings.opportunity_min_confidence.strip().lower() or "medium"
        self._opportunity_min_unique_buyers_5m = max(0, settings.opportunity_min_unique_buyers_5m)
        self._opportunity_max_wallet_concentration_5m = max(
            0.0,
            min(1.0, settings.opportunity_max_wallet_concentration_5m),
        )
        self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        self._telegram_bot_token = settings.telegram_bot_token
        self._telegram_chat_id = settings.telegram_chat_id
        self._discord_webhook_url = settings.discord_webhook_url
        self._telegram_enabled = settings.telegram_enabled
        self._discord_enabled = settings.discord_enabled
        self._dexscreener_client: DexScreenerClient | None = None
        if settings.dexscreener_enabled:
            self._dexscreener_client = DexScreenerClient(
                settings.dexscreener_base_url,
                settings.dexscreener_timeout_seconds,
            )
        self._score_locks: dict[str, asyncio.Lock] = {}
        self._score_locks_guard = asyncio.Lock()
        self._clickhouse_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public entry point called by the profiler
    # ------------------------------------------------------------------

    async def score_and_store(
        self,
        mint: str,
        profile: dict,
        db: str,
        ch_client: object,
    ) -> None:
        """Score the mint with Claude and persist result to mint_scores."""
        try:
            positive_examples, negative_examples = await self._run_clickhouse_query(
                self._fetch_examples,
                db,
                ch_client,
            )
            result = await self._call_claude_with_retry(profile, positive_examples, negative_examples)
            if result is None:
                return

            mint_lock = await self._get_score_lock(mint)
            async with mint_lock:
                recent_score_exists = await self._run_clickhouse_query(
                    self._has_recent_score,
                    db,
                    ch_client,
                    mint,
                )
                if recent_score_exists:
                    LOGGER.info(
                        "mint_score_skip_existing mint=%s window_minutes=%d",
                        mint,
                        self._alert_dedup_minutes,
                    )
                    return

                recent_high_alert = await self._run_clickhouse_query(
                    self._has_recent_high_score_alert,
                    db,
                    ch_client,
                    mint,
                )
                inserted = await self._run_clickhouse_query(
                    self._store_score_if_absent,
                    db,
                    ch_client,
                    mint,
                    profile.get("profile_age_minutes", 0),
                    result,
                )
                if not inserted:
                    LOGGER.info(
                        "mint_score_skip_insert_if_absent mint=%s window_minutes=%d",
                        mint,
                        self._alert_dedup_minutes,
                    )
                    return

            is_good_opportunity = self._is_good_opportunity(result, profile)
            if is_good_opportunity and not recent_high_alert:
                LOGGER.warning(
                    "good_opportunity_detected mint=%s score=%.2f confidence=%s recommendation=%s",
                    mint,
                    result["score"],
                    result["confidence"],
                    result["recommendation"],
                )
                if self._telegram_enabled or self._discord_enabled:
                    dex = await self._fetch_dexscreener_snapshot(mint)
                    text = self._format_opportunity_alert(mint, profile, result, dex)
                    await send_broadcast_message(
                        text,
                        telegram_token=self._telegram_bot_token,
                        telegram_chat_id=self._telegram_chat_id,
                        discord_webhook_url=self._discord_webhook_url,
                    )
            elif is_good_opportunity:
                LOGGER.info(
                    "opportunity_alert_skipped_dedup mint=%s window_minutes=%d",
                    mint,
                    self._alert_dedup_minutes,
                )
            else:
                LOGGER.info(
                    "opportunity_not_qualified mint=%s score=%.2f confidence=%s",
                    mint,
                    result["score"],
                    result["confidence"],
                )
        except Exception:
            LOGGER.exception("claude_scorer_error mint=%s", mint)

    async def _run_clickhouse_query(self, fn, *args):
        # clickhouse_connect sessions are not safe for concurrent queries.
        async with self._clickhouse_lock:
            return await asyncio.to_thread(fn, *args)

    async def _get_score_lock(self, mint: str) -> asyncio.Lock:
        async with self._score_locks_guard:
            lock = self._score_locks.get(mint)
            if lock is None:
                lock = asyncio.Lock()
                self._score_locks[mint] = lock
            return lock

    # ------------------------------------------------------------------
    # Fetch historical examples
    # ------------------------------------------------------------------

    def _fetch_examples(
        self, db: str, ch_client: object
    ) -> tuple[list[dict], list[dict]]:
        pumped_q = f"""
        SELECT mint, program_name, initial_price, price_5m, peak_multiplier,
               volume_quote_5m, trade_count_5m, buy_sell_ratio_5m,
               unique_buyers_5m, wallet_concentration_5m,
               creator_past_tokens, creator_avg_peak_mult
        FROM {db}.mint_profiles FINAL
        WHERE peak_multiplier >= 5
        ORDER BY peak_multiplier DESC
        LIMIT {{limit:UInt16}}
        """
        dead_q = f"""
        SELECT mint, program_name, initial_price, price_5m, peak_multiplier,
               volume_quote_5m, trade_count_5m, buy_sell_ratio_5m,
               unique_buyers_5m, wallet_concentration_5m,
               creator_past_tokens, creator_avg_peak_mult
        FROM {db}.mint_profiles FINAL
        WHERE peak_multiplier < 1.5 AND peak_multiplier IS NOT NULL
        ORDER BY created_at DESC
        LIMIT {{limit:UInt16}}
        """
        limit = self._max_examples // 2

        def row_to_dict(r: tuple) -> dict:
            keys = [
                "mint", "program", "initial_price", "price_5m", "peak_multiplier",
                "volume_quote_5m", "trade_count_5m", "buy_sell_ratio_5m",
                "unique_buyers_5m", "wallet_concentration_5m",
                "creator_past_tokens", "creator_avg_peak_mult",
            ]
            return {k: v for k, v in zip(keys, r)}

        pumped_rows = ch_client.query(pumped_q, parameters={"limit": limit}).result_rows  # type: ignore[attr-defined]
        dead_rows = ch_client.query(dead_q, parameters={"limit": limit}).result_rows  # type: ignore[attr-defined]
        return [row_to_dict(r) for r in pumped_rows], [row_to_dict(r) for r in dead_rows]

    # ------------------------------------------------------------------
    # Build prompt and call Claude
    # ------------------------------------------------------------------

    async def _call_claude(
        self,
        profile: dict,
        positive_examples: list[dict],
        negative_examples: list[dict],
    ) -> dict | None:
        user_content = self._build_user_message(profile, positive_examples, negative_examples)

        response = await self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            tools=[_ANALYSIS_TOOL],  # type: ignore[list-item]
            tool_choice={"type": "tool", "name": "report_mint_analysis"},
            messages=[{"role": "user", "content": user_content}],
        )

        # Extract tool_use block
        for block in response.content:
            if block.type == "tool_use" and block.name == "report_mint_analysis":
                inp = block.input
                return {
                    "score": float(inp.get("score", 0.0)),
                    "confidence": str(inp.get("confidence", "low")),
                    "positive_signals": list(inp.get("positive_signals", [])),
                    "negative_signals": list(inp.get("negative_signals", [])),
                    "recommendation": str(inp.get("recommendation", "")),
                    "raw": json.dumps(inp),
                }

        LOGGER.warning("claude_scorer: no tool_use block in response")
        return None

    async def _call_claude_with_retry(
        self,
        profile: dict,
        positive_examples: list[dict],
        negative_examples: list[dict],
    ) -> dict | None:
        last_error: Exception | None = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                return await self._call_claude(profile, positive_examples, negative_examples)
            except Exception as exc:
                last_error = exc
                if attempt >= self._retry_attempts:
                    break
                backoff_seconds = min(self._retry_max_wait_seconds, 2 ** (attempt - 1))
                LOGGER.warning(
                    "claude_retry attempt=%d/%d backoff_seconds=%d error=%s",
                    attempt,
                    self._retry_attempts,
                    backoff_seconds,
                    type(exc).__name__,
                )
                await asyncio.sleep(backoff_seconds)

        if last_error is not None:
            raise last_error
        return None

    @staticmethod
    def _build_user_message(
        profile: dict,
        positive_examples: list[dict],
        negative_examples: list[dict],
    ) -> str:
        def fmt(d: dict) -> str:
            # Round floats for compactness
            clean = {
                k: (round(v, 6) if isinstance(v, float) else v)
                for k, v in d.items()
                if v is not None
            }
            return json.dumps(clean)

        parts = ["NEW MINT PROFILE:\n" + fmt(profile)]
        if positive_examples:
            parts.append(
                "\nHISTORICAL PUMPED MINTS (5x+ in 2 hrs, for reference):\n"
                + "\n".join(fmt(e) for e in positive_examples)
            )
        if negative_examples:
            parts.append(
                "\nHISTORICAL DEAD MINTS (failed to 1.5x, for reference):\n"
                + "\n".join(fmt(e) for e in negative_examples)
            )
        parts.append(
            "\nAnalyze the new mint profile against these historical patterns "
            "and use the report_mint_analysis tool to submit your assessment."
        )
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Persist score
    # ------------------------------------------------------------------

    def _store_score(
        self,
        db: str,
        ch_client: object,
        mint: str,
        profile_age_minutes: int,
        result: dict,
    ) -> None:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
        ch_client.insert(  # type: ignore[attr-defined]
            f"{db}.mint_scores",
            [[
                mint,
                now_str,
                self._model,
                result["score"],
                result["confidence"],
                result["positive_signals"],
                result["negative_signals"],
                result["recommendation"],
                profile_age_minutes,
                result["raw"],
            ]],
            column_names=[
                "mint", "scored_at", "model", "score", "confidence",
                "positive_signals", "negative_signals", "recommendation",
                "profile_age_minutes", "raw_response",
            ],
        )
        LOGGER.info(
            "mint_scored mint=%s score=%.2f confidence=%s",
            mint, result["score"], result["confidence"],
        )

    def _store_score_if_absent(
        self,
        db: str,
        ch_client: object,
        mint: str,
        profile_age_minutes: int,
        result: dict,
    ) -> bool:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
        query = f"""
        INSERT INTO {db}.mint_scores (
            mint, scored_at, model, score, confidence,
            positive_signals, negative_signals, recommendation,
            profile_age_minutes, raw_response
        )
        SELECT
            {{mint:String}}, {{scored_at:DateTime64(3)}}, {{model:String}}, {{score:Float32}}, {{confidence:String}},
            {{positive_signals:Array(String)}}, {{negative_signals:Array(String)}}, {{recommendation:String}},
            {{profile_age_minutes:UInt16}}, {{raw_response:String}}
        WHERE NOT EXISTS (
            SELECT 1
            FROM {db}.mint_scores
            WHERE mint = {{mint:String}}
              AND model = {{model:String}}
              AND scored_at >= now() - INTERVAL {self._alert_dedup_minutes} MINUTE
        )
        """
        verify_query = f"""
        SELECT count()
        FROM {db}.mint_scores
        WHERE mint = {{mint:String}}
          AND model = {{model:String}}
          AND scored_at = {{scored_at:DateTime64(3)}}
        """
        params = {
            "mint": mint,
            "scored_at": now_str,
            "model": self._model,
            "score": float(result["score"]),
            "confidence": str(result["confidence"]),
            "positive_signals": list(result["positive_signals"]),
            "negative_signals": list(result["negative_signals"]),
            "recommendation": str(result["recommendation"]),
            "profile_age_minutes": int(profile_age_minutes),
            "raw_response": str(result["raw"]),
        }
        try:
            ch_client.command(  # type: ignore[attr-defined]
                query,
                parameters=params,
            )
            rows = ch_client.query(  # type: ignore[attr-defined]
                verify_query,
                parameters={
                    "mint": mint,
                    "model": self._model,
                    "scored_at": now_str,
                },
            ).result_rows
            inserted = bool(rows and int(rows[0][0] or 0) > 0)
            if inserted:
                LOGGER.info(
                    "mint_scored mint=%s score=%.2f confidence=%s",
                    mint,
                    result["score"],
                    result["confidence"],
                )
            return inserted
        except Exception:
            LOGGER.exception("mint_score_insert_if_absent_failed mint=%s", mint)
            return False

    def _has_recent_high_score_alert(
        self,
        db: str,
        ch_client: object,
        mint: str,
    ) -> bool:
        query = f"""
        SELECT count()
        FROM {db}.mint_scores
        WHERE mint = {{mint:String}}
          AND score >= {{threshold:Float32}}
          AND scored_at >= now() - INTERVAL {self._alert_dedup_minutes} MINUTE
        """
        try:
            rows = ch_client.query(  # type: ignore[attr-defined]
                query,
                parameters={"mint": mint, "threshold": self._threshold},
            ).result_rows
            return bool(rows and int(rows[0][0] or 0) > 0)
        except Exception:
            LOGGER.exception("high_score_dedup_query_failed mint=%s", mint)
            return False

    def _is_good_opportunity(self, result: dict, profile: dict) -> bool:
        confidence_rank = {"low": 1, "medium": 2, "high": 3}
        min_confidence = confidence_rank.get(self._opportunity_min_confidence, 2)
        actual_confidence = confidence_rank.get(str(result.get("confidence", "low")).lower(), 1)
        unique_buyers = int(profile.get("unique_buyers_5m") or 0)
        wallet_conc = profile.get("wallet_concentration_5m")
        wallet_conc_value = float(wallet_conc) if wallet_conc is not None else 1.0

        if float(result.get("score", 0.0)) < self._threshold:
            return False
        if actual_confidence < min_confidence:
            return False
        if unique_buyers < self._opportunity_min_unique_buyers_5m:
            return False
        if wallet_conc_value > self._opportunity_max_wallet_concentration_5m:
            return False
        return True

    @staticmethod
    def _format_opportunity_alert(
        mint: str,
        profile: dict,
        result: dict,
        dex: DexScreenerSnapshot | None,
    ) -> str:
        symbol = profile.get("symbol") or "n/a"
        program = profile.get("program") or "unknown"
        score = float(result.get("score", 0.0))
        confidence = result.get("confidence", "low")
        buy_sell = profile.get("buy_sell_ratio_5m")
        buyers = int(profile.get("unique_buyers_5m") or 0)
        volume_5m = profile.get("volume_quote_5m")
        wallet_conc = profile.get("wallet_concentration_5m")
        recommendation = result.get("recommendation", "")
        positives = ", ".join(result.get("positive_signals", [])[:3]) or "n/a"
        negatives = ", ".join(result.get("negative_signals", [])[:3]) or "n/a"
        dex_block = ClaudeScorer._format_dex_block(dex)

        return (
            "GOOD OPPORTUNITY\n"
            f"symbol: {symbol}\n"
            f"mint: {mint}\n"
            f"program: {program}\n"
            f"score: {score:.2f}\n"
            f"confidence: {confidence}\n"
            f"buy/sell 5m: {buy_sell}\n"
            f"unique buyers 5m: {buyers}\n"
            f"volume quote 5m: {volume_5m}\n"
            f"wallet concentration 5m: {wallet_conc}\n"
            f"recommendation: {recommendation}\n"
            f"positive signals: {positives}\n"
            f"negative signals: {negatives}\n"
            f"{dex_block}"
        )

    @staticmethod
    def _format_dex_block(dex: DexScreenerSnapshot | None) -> str:
        if dex is None:
            return "dexscreener: no market data yet (possibly too new or not indexed)"
        return (
            "dexscreener:\n"
            f"  price usd: {ClaudeScorer._fmt_price(dex.price_usd)}\n"
            f"  liquidity usd: {ClaudeScorer._fmt_num(dex.liquidity_usd)}\n"
            f"  fdv: {ClaudeScorer._fmt_num(dex.fdv)}\n"
            f"  market cap: {ClaudeScorer._fmt_num(dex.market_cap)}\n"
            f"  volume m5/h1/h24: {ClaudeScorer._fmt_num(dex.volume_5m)}/{ClaudeScorer._fmt_num(dex.volume_1h)}/{ClaudeScorer._fmt_num(dex.volume_24h)}\n"
            f"  txns m5 buys/sells: {dex.buys_5m}/{dex.sells_5m}\n"
            f"  dex: {dex.dex_id}\n"
            f"  pair: {dex.pair_address}\n"
            f"  url: {dex.pair_url}"
        )

    @staticmethod
    def _fmt_num(value: float | None) -> str:
        if value is None:
            return "n/a"
        return f"{value:,.4f}" if abs(value) < 1000 else f"{value:,.2f}"

    @staticmethod
    def _fmt_price(value: float | None) -> str:
        if value is None:
            return "n/a"
        if value == 0:
            return "0"

        # Keep far more precision for tiny token prices so non-zero values are visible.
        text = f"{value:.18f}".rstrip("0").rstrip(".")
        return text if text else "0"

    async def _fetch_dexscreener_snapshot(self, mint: str) -> DexScreenerSnapshot | None:
        if self._dexscreener_client is None:
            return None
        return await asyncio.to_thread(self._dexscreener_client.get_best_pair_snapshot, mint)

    def _has_recent_score(
        self,
        db: str,
        ch_client: object,
        mint: str,
    ) -> bool:
        query = f"""
        SELECT count()
        FROM {db}.mint_scores
        WHERE mint = {{mint:String}}
          AND model = {{model:String}}
          AND scored_at >= now() - INTERVAL {self._alert_dedup_minutes} MINUTE
        """
        try:
            rows = ch_client.query(  # type: ignore[attr-defined]
                query,
                parameters={"mint": mint, "model": self._model},
            ).result_rows
            return bool(rows and int(rows[0][0] or 0) > 0)
        except Exception:
            LOGGER.exception("score_dedup_query_failed mint=%s", mint)
            return False
