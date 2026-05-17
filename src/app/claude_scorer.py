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
from .notifications import send_telegram_message

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
        self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        self._telegram_bot_token = settings.telegram_bot_token
        self._telegram_chat_id = settings.telegram_chat_id
        self._telegram_enabled = settings.telegram_enabled

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
            positive_examples, negative_examples = await asyncio.to_thread(
                self._fetch_examples, db, ch_client
            )
            result = await self._call_claude(profile, positive_examples, negative_examples)
            if result is None:
                return
            await asyncio.to_thread(
                self._store_score, db, ch_client, mint, profile.get("profile_age_minutes", 0), result
            )
            if result["score"] >= self._threshold:
                LOGGER.warning(
                    "🚨 HIGH_SCORE_MINT mint=%s score=%.2f confidence=%s recommendation=%s",
                    mint,
                    result["score"],
                    result["confidence"],
                    result["recommendation"],
                )
                if self._telegram_enabled:
                    text = (
                        "HIGH SCORE MINT\n"
                        f"mint: {mint}\n"
                        f"score: {result.get('score'):.2f}\n"
                        f"confidence: {result.get('confidence')}\n"
                        f"recommendation: {result.get('recommendation')}"
                    )
                    await send_telegram_message(
                        self._telegram_bot_token,
                        self._telegram_chat_id,
                        text,
                    )
        except Exception:
            LOGGER.exception("claude_scorer_error mint=%s", mint)

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
