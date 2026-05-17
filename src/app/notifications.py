from __future__ import annotations

import asyncio
import logging
import urllib.parse
import urllib.request

LOGGER = logging.getLogger(__name__)


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
