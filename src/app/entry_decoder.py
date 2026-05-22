from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess

from .config import Settings

LOGGER = logging.getLogger(__name__)


class EntryDecoder:
    def __init__(self, settings: Settings) -> None:
        self._path = settings.entry_decoder_path
        self._timeout_seconds = max(1, settings.entry_decoder_timeout_seconds)

    def is_available(self) -> bool:
        if not self._path:
            return False
        if not os.path.isfile(self._path):
            return False
        return os.access(self._path, os.X_OK)

    async def decode_transactions(self, entries_b64: str) -> list[dict]:
        if not self.is_available():
            LOGGER.error("entry_decoder_unavailable path=%s", self._path)
            return []
        if not entries_b64.strip():
            return []
        return await asyncio.to_thread(self._decode_sync, entries_b64)

    def _decode_sync(self, entries_b64: str) -> list[dict]:
        try:
            proc = subprocess.run(
                [self._path],
                input=entries_b64,
                text=True,
                capture_output=True,
                check=False,
                timeout=self._timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            LOGGER.error(
                "entry_decoder_timeout timeout_seconds=%d",
                self._timeout_seconds,
            )
            return []
        if proc.returncode != 0:
            LOGGER.error(
                "entry_decoder_failed code=%d stderr=%s",
                proc.returncode,
                (proc.stderr or "").strip(),
            )
            return []
        try:
            payload = json.loads(proc.stdout or "[]")
        except json.JSONDecodeError:
            LOGGER.error("entry_decoder_invalid_json")
            return []
        if not isinstance(payload, list):
            return []
        output: list[dict] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            signature = item.get("signature")
            instructions = item.get("instructions")
            if signature is not None and not isinstance(signature, str):
                continue
            if not isinstance(instructions, list):
                continue
            clean_instructions: list[dict] = []
            for inst in instructions:
                if not isinstance(inst, dict):
                    continue
                program_id = inst.get("program_id")
                data_hex = inst.get("data_hex")
                accounts = inst.get("accounts")
                if not isinstance(program_id, str) or not isinstance(data_hex, str):
                    continue
                if accounts is None:
                    accounts = []
                if not isinstance(accounts, list) or not all(isinstance(a, str) for a in accounts):
                    accounts = []
                clean_instructions.append({
                    "program_id": program_id,
                    "data_hex": data_hex,
                    "accounts": accounts,
                })
            output.append({"signature": signature, "instructions": clean_instructions})
        return output
