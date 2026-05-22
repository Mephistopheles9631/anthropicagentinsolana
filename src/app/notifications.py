from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
import urllib.request
import urllib.error
import time

LOGGER = logging.getLogger(__name__)
_DISCORD_MAX_CONTENT = 2000
_DISCORD_SAFE_CONTENT = 1900


def _post_telegram_message(token: str, chat_id: str, text: str) -> None:
    payload = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    req = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


async def send_telegram_message(token: str, chat_id: str, text: str) -> None:
    try:
        await asyncio.to_thread(_post_telegram_message, token, chat_id, text)
        LOGGER.info("telegram_message_sent")
    except Exception:
        LOGGER.exception("telegram_message_failed")


def _split_discord_chunks(text: str, max_len: int = _DISCORD_SAFE_CONTENT) -> list[str]:
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    remaining = text
    while len(remaining) > max_len:
        split_at = remaining.rfind("\n", 0, max_len)
        if split_at <= 0:
            split_at = max_len
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip("\n")
    if remaining:
        chunks.append(remaining)
    return chunks


def _post_discord_payload(webhook_url: str, content: str) -> None:
    payload = json.dumps({"content": content[:_DISCORD_MAX_CONTENT]}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "solana-mint-intel/1.0",
        },
        method="POST",
    )

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp.read()
            return
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < 2:
                retry_after_header = exc.headers.get("Retry-After", "1")
                try:
                    retry_after = float(retry_after_header)
                except ValueError:
                    retry_after = 1.0
                time.sleep(max(0.2, retry_after))
                continue
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="ignore")
            except Exception:
                body = ""
            raise RuntimeError(f"discord_http_error status={exc.code} body={body[:300]}") from exc


def _post_discord_message(webhook_url: str, text: str) -> None:
    chunks = _split_discord_chunks(text)
    for idx, chunk in enumerate(chunks, start=1):
        content = chunk if len(chunks) == 1 else f"[{idx}/{len(chunks)}]\n{chunk}"
        _post_discord_payload(webhook_url, content)


async def send_discord_message(webhook_url: str, text: str) -> None:
    try:
        await asyncio.to_thread(_post_discord_message, webhook_url, text)
        LOGGER.info("discord_message_sent")
    except Exception:
        LOGGER.exception("discord_message_failed")


async def send_broadcast_message(
    text: str,
    telegram_token: str = "",
    telegram_chat_id: str = "",
    discord_webhook_url: str = "",
) -> None:
    tasks: list[asyncio.Task] = []
    if telegram_token.strip() and telegram_chat_id.strip():
        tasks.append(asyncio.create_task(send_telegram_message(telegram_token, telegram_chat_id, text)))
    if discord_webhook_url.strip():
        tasks.append(asyncio.create_task(send_discord_message(discord_webhook_url, text)))
    if tasks:
        await asyncio.gather(*tasks)
