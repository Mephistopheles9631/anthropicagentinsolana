"""In-memory pool registry: maps pool_address → (token_a_mint, token_b_mint).

Pre-populated at startup from the ``pool_creates`` ClickHouse table, then kept
current from live events.  Thread-safe reads are fine (dict lookup is atomic
in CPython) but updates go through a simple lock to be safe across asyncio
tasks.
"""
from __future__ import annotations

import asyncio
import logging

LOGGER = logging.getLogger(__name__)


class PoolRegistry:
    """Maps pool address → (base_mint, quote_mint) pair."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[str, str]] = {}
        self._lock = asyncio.Lock()

    def get(self, pool_address: str) -> tuple[str, str] | None:
        return self._store.get(pool_address)

    def update(self, pool_address: str, token_a: str, token_b: str) -> None:
        """Register or overwrite a pool → mint pair (sync, non-locking)."""
        if pool_address and token_a and token_b:
            self._store[pool_address] = (token_a, token_b)

    async def update_async(self, pool_address: str, token_a: str, token_b: str) -> None:
        async with self._lock:
            self.update(pool_address, token_a, token_b)

    def size(self) -> int:
        return len(self._store)

    async def bootstrap_from_clickhouse(self, client: object, database: str) -> None:
        """Load pool → mint mappings from the pool_creates table on startup."""
        import asyncio

        query = f"""
        SELECT pool_address, mint, quote_mint
        FROM {database}.pool_creates
        WHERE pool_address != '' AND mint != ''
        """
        try:
            rows = await asyncio.to_thread(client.query, query)  # type: ignore[attr-defined]
            count = 0
            for row in rows.result_rows:
                pool_addr, mint, quote_mint = row[0], row[1], row[2]
                self.update(pool_addr, mint, quote_mint)
                count += 1
            LOGGER.info("pool_registry_bootstrapped pools=%d", count)
        except Exception as exc:
            # Table may not exist yet on first run — that's fine.
            LOGGER.warning("pool_registry_bootstrap_failed: %s", exc)
