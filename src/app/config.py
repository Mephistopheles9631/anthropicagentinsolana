from __future__ import annotations

import json
import logging

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


LOGGER = logging.getLogger(__name__)
_B58_CHARS = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    shredstream_grpc_target: str = Field(default="localhost:50051", alias="SHREDSTREAM_GRPC_TARGET")

    grpc_messages_module: str = Field(default="shredstream_pb2", alias="GRPC_MESSAGES_MODULE")
    grpc_stub_module: str = Field(default="shredstream_pb2_grpc", alias="GRPC_STUB_MODULE")
    grpc_stub_class: str = Field(default="ShredstreamStub", alias="GRPC_STUB_CLASS")
    grpc_stream_method: str = Field(default="Subscribe", alias="GRPC_STREAM_METHOD")
    grpc_request_class: str = Field(default="SubscribeRequest", alias="GRPC_REQUEST_CLASS")
    grpc_request_json: str = Field(default="{}", alias="GRPC_REQUEST_JSON")

    entry_decoder_path: str = Field(
        default="tools/entry_decoder/target/release/entry_decoder",
        alias="ENTRY_DECODER_PATH",
    )
    entry_decoder_timeout_seconds: int = Field(default=8, alias="ENTRY_DECODER_TIMEOUT_SECONDS")

    program_id_pumpfun: str = Field(default="", alias="PROGRAM_ID_PUMPFUN")
    program_id_pumpswap: str = Field(default="", alias="PROGRAM_ID_PUMPSWAP")
    program_id_raydium: str = Field(default="", alias="PROGRAM_ID_RAYDIUM")
    program_ids_raydium: str = Field(default="", alias="PROGRAM_IDS_RAYDIUM")
    program_id_meteora: str = Field(default="", alias="PROGRAM_ID_METEORA")
    program_ids_meteora: str = Field(default="", alias="PROGRAM_IDS_METEORA")
    program_id_meteora_damm_v2: str = Field(default="", alias="PROGRAM_ID_METEORA_DAMM_V2")
    program_id_whirlpool: str = Field(default="", alias="PROGRAM_ID_WHIRLPOOL")

    idl_dir: str = Field(default="idl", alias="IDL_DIR")
    idl_program_id_map_json: str = Field(default="{}", alias="IDL_PROGRAM_ID_MAP_JSON")
    validate_idls_on_startup: bool = Field(default=True, alias="VALIDATE_IDLS_ON_STARTUP")

    clickhouse_host: str = Field(default="localhost", alias="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=8123, alias="CLICKHOUSE_PORT")
    clickhouse_username: str = Field(default="default", alias="CLICKHOUSE_USERNAME")
    clickhouse_password: str = Field(default="", alias="CLICKHOUSE_PASSWORD")
    clickhouse_database: str = Field(default="default", alias="CLICKHOUSE_DATABASE")
    clickhouse_table: str = Field(default="shredstream_events", alias="CLICKHOUSE_TABLE")

    batch_size: int = Field(default=250, alias="BATCH_SIZE")
    flush_interval_seconds: float = Field(default=1.0, alias="FLUSH_INTERVAL_SECONDS")

    # Mint profiler settings
    profile_delay_minutes: int = Field(default=5, alias="PROFILE_DELAY_MINUTES")
    profile_max_age_minutes: int = Field(default=240, alias="PROFILE_MAX_AGE_MINUTES")
    profile_check_interval_seconds: int = Field(default=60, alias="PROFILE_CHECK_INTERVAL_SECONDS")

    # Claude / Anthropic settings
    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")
    claude_model: str = Field(default="claude-haiku-4-5", alias="CLAUDE_MODEL")
    claude_score_threshold: float = Field(default=0.70, alias="CLAUDE_SCORE_THRESHOLD")
    claude_max_examples: int = Field(default=10, alias="CLAUDE_MAX_EXAMPLES")
    claude_retry_attempts: int = Field(default=3, alias="CLAUDE_RETRY_ATTEMPTS")
    claude_retry_max_wait_seconds: int = Field(default=8, alias="CLAUDE_RETRY_MAX_WAIT_SECONDS")
    claude_alert_dedup_minutes: int = Field(default=240, alias="CLAUDE_ALERT_DEDUP_MINUTES")
    opportunity_min_confidence: str = Field(default="medium", alias="OPPORTUNITY_MIN_CONFIDENCE")
    opportunity_min_unique_buyers_5m: int = Field(default=0, alias="OPPORTUNITY_MIN_UNIQUE_BUYERS_5M")
    opportunity_max_wallet_concentration_5m: float = Field(
        default=1.0,
        alias="OPPORTUNITY_MAX_WALLET_CONCENTRATION_5M",
    )
    dexscreener_enabled: bool = Field(default=True, alias="DEXSCREENER_ENABLED")
    dexscreener_base_url: str = Field(
        default="https://api.dexscreener.com/latest/dex/tokens",
        alias="DEXSCREENER_BASE_URL",
    )
    dexscreener_timeout_seconds: int = Field(default=8, alias="DEXSCREENER_TIMEOUT_SECONDS")

    # Telegram notifications
    telegram_bot_token: str = Field(default="", alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field(default="", alias="TELEGRAM_CHAT_ID")
    discord_webhook_url: str = Field(default="", alias="DISCORD_WEBHOOK_URL")

    @property
    def claude_enabled(self) -> bool:
        return bool(self.anthropic_api_key.strip())

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token.strip() and self.telegram_chat_id.strip())

    @property
    def discord_enabled(self) -> bool:
        return bool(self.discord_webhook_url.strip())

    @property
    def tracked_programs(self) -> dict[str, set[str]]:
        by_name = {
            "pumpfun": self._single(self.program_id_pumpfun),
            "pumpswap": self._single(self.program_id_pumpswap),
            "raydium": self._merge_ids(self.program_id_raydium, self.program_ids_raydium),
            "meteora": self._merge_ids(self.program_id_meteora, self.program_ids_meteora),
            "meteora_damm_v2": self._single(self.program_id_meteora_damm_v2),
            "whirlpool": self._single(self.program_id_whirlpool),
        }
        return {name: ids for name, ids in by_name.items() if ids}

    @property
    def invalid_tracked_program_ids(self) -> list[str]:
        bad: list[str] = []
        for ids in self.tracked_programs.values():
            for program_id in ids:
                if not self._looks_like_program_id(program_id):
                    bad.append(program_id)
        return sorted(set(bad))

    @property
    def tracked_program_lookup(self) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for program_name, ids in self.tracked_programs.items():
            for program_id in ids:
                lookup.setdefault(program_id, program_name)
        return lookup

    @property
    def idl_program_id_map(self) -> dict[str, str]:
        try:
            payload = json.loads(self.idl_program_id_map_json)
        except json.JSONDecodeError:
            LOGGER.warning("invalid_idl_program_id_map_json")
            return {}

        if not isinstance(payload, dict):
            LOGGER.warning("invalid_idl_program_id_map_type expected=dict")
            return {}
        output: dict[str, str] = {}
        for key, value in payload.items():
            if isinstance(key, str) and isinstance(value, str) and value.strip():
                output[key] = value.strip()
        return output

    @staticmethod
    def _single(value: str) -> set[str]:
        clean = value.strip()
        return {clean} if clean else set()

    def _merge_ids(self, single: str, csv_values: str) -> set[str]:
        merged = self._single(single)
        merged.update(self._split_csv(csv_values))
        return merged

    @staticmethod
    def _split_csv(value: str) -> set[str]:
        return {item.strip() for item in value.split(",") if item.strip()}

    @staticmethod
    def _looks_like_program_id(value: str) -> bool:
        clean = value.strip()
        if len(clean) < 32 or len(clean) > 44:
            return False
        return all(ch in _B58_CHARS for ch in clean)
