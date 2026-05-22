from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error
from dataclasses import dataclass

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class DexScreenerSnapshot:
    pair_url: str | None
    dex_id: str | None
    pair_address: str | None
    price_usd: float | None
    liquidity_usd: float | None
    fdv: float | None
    market_cap: float | None
    volume_5m: float | None
    volume_1h: float | None
    volume_24h: float | None
    buys_5m: int | None
    sells_5m: int | None


class DexScreenerClient:
    def __init__(self, base_url: str, timeout_seconds: int = 8) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = max(1, timeout_seconds)

    def get_best_pair_snapshot(self, mint: str) -> DexScreenerSnapshot | None:
        if not mint.strip():
            return None
        url = f"{self._base_url}/{mint.strip()}"
        req = urllib.request.Request(
            url,
            method="GET",
            headers={
                "User-Agent": "solana-mint-intel/1.0",
                "Accept": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            LOGGER.warning(
                "dexscreener_http_error mint=%s status=%s",
                mint,
                exc.code,
            )
            return None
        except urllib.error.URLError as exc:
            LOGGER.warning("dexscreener_network_error mint=%s reason=%s", mint, exc.reason)
            return None
        except Exception:
            LOGGER.exception("dexscreener_request_failed mint=%s", mint)
            return None

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            LOGGER.warning("dexscreener_invalid_json mint=%s", mint)
            return None

        pairs = payload.get("pairs")
        if not isinstance(pairs, list) or not pairs:
            return None

        solana_pairs = [p for p in pairs if isinstance(p, dict) and p.get("chainId") == "solana"]
        candidates = solana_pairs if solana_pairs else [p for p in pairs if isinstance(p, dict)]
        if not candidates:
            return None

        def liquidity_value(pair: dict) -> float:
            liq = pair.get("liquidity")
            if isinstance(liq, dict):
                usd = liq.get("usd")
                try:
                    return float(usd)
                except (TypeError, ValueError):
                    return 0.0
            return 0.0

        best = max(candidates, key=liquidity_value)
        volume = best.get("volume") if isinstance(best.get("volume"), dict) else {}
        txns = best.get("txns") if isinstance(best.get("txns"), dict) else {}
        txns_m5 = txns.get("m5") if isinstance(txns.get("m5"), dict) else {}

        return DexScreenerSnapshot(
            pair_url=_to_str(best.get("url")),
            dex_id=_to_str(best.get("dexId")),
            pair_address=_to_str(best.get("pairAddress")),
            price_usd=_to_float(best.get("priceUsd")),
            liquidity_usd=_to_float((best.get("liquidity") or {}).get("usd") if isinstance(best.get("liquidity"), dict) else None),
            fdv=_to_float(best.get("fdv")),
            market_cap=_to_float(best.get("marketCap")),
            volume_5m=_to_float(volume.get("m5")),
            volume_1h=_to_float(volume.get("h1")),
            volume_24h=_to_float(volume.get("h24")),
            buys_5m=_to_int(txns_m5.get("buys")),
            sells_5m=_to_int(txns_m5.get("sells")),
        )


def _to_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
