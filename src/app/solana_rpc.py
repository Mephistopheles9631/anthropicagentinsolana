from __future__ import annotations

import asyncio
import json
import logging
import urllib.request

from .config import Settings

LOGGER = logging.getLogger(__name__)


class SolanaRpcClient:
    def __init__(self, settings: Settings) -> None:
        self._url = settings.solana_rpc_url
        self._timeout = settings.rpc_timeout_seconds
        self._batch_size = max(1, settings.rpc_batch_size)
        self._semaphore = asyncio.Semaphore(max(1, settings.rpc_max_concurrency))

    async def get_transactions(self, signatures: list[str]) -> list[dict]:
        if not signatures:
            return []
        if not self._url:
            LOGGER.error("solana_rpc_url_missing")
            return []

        results: list[dict] = []
        for i in range(0, len(signatures), self._batch_size):
            chunk = signatures[i : i + self._batch_size]
            payloads = await self._fetch_chunk(chunk)
            results.extend(payloads)
        return results

    async def _fetch_chunk(self, signatures: list[str]) -> list[dict]:
        async with self._semaphore:
            return await asyncio.to_thread(self._fetch_chunk_sync, signatures)

    def _fetch_chunk_sync(self, signatures: list[str]) -> list[dict]:
        requests = []
        for idx, sig in enumerate(signatures):
            requests.append(
                {
                    "jsonrpc": "2.0",
                    "id": idx,
                    "method": "getTransaction",
                    "params": [
                        sig,
                        {
                            "encoding": "json",
                            "maxSupportedTransactionVersion": 0,
                        },
                    ],
                }
            )

        data = json.dumps(requests).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = resp.read()
        except Exception:
            LOGGER.exception("solana_rpc_request_failed")
            return []

        try:
            response = json.loads(body)
        except json.JSONDecodeError:
            LOGGER.error("solana_rpc_invalid_json")
            return []

        payloads: list[dict] = []
        if not isinstance(response, list):
            return payloads

        for item in response:
            if not isinstance(item, dict):
                continue
            result = item.get("result")
            if not isinstance(result, dict):
                continue
            payload = self._result_to_payload(result)
            if payload:
                payloads.append(payload)
        return payloads

    @staticmethod
    def _result_to_payload(result: dict) -> dict | None:
        txn = result.get("transaction")
        if not isinstance(txn, dict):
            return None
        message = txn.get("message")
        if not isinstance(message, dict):
            return None
        meta = result.get("meta")
        if not isinstance(meta, dict):
            meta = {}

        logs = meta.get("logMessages")
        if logs is None:
            logs = []

        signatures = txn.get("signatures") or []
        signature = signatures[0] if signatures else None

        return {
            "slot": result.get("slot"),
            "signature": signature,
            "transaction": {
                "transaction": {
                    "message": {
                        "account_keys": message.get("accountKeys", []),
                    },
                },
                "meta": {
                    "log_messages": logs,
                },
            },
        }
