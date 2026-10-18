"""Student implementation area. The starter deliberately bypasses Redis.

Keep the public constructor and method signatures. Internal design is yours.
See docs/ASSIGNMENT.md for the required behavior and incremental milestones.
"""
import time
from collections.abc import Callable

from redis.asyncio import Redis

from app.models import Product, ProductPatch
from app.source import CatalogSource


class ProductService:
    def __init__(
        self, source: CatalogSource, cache: Redis, *, prefix: str,
        fresh_ttl: float = 5, stale_ttl: float = 10,
        mode: str = "cache_aside", clock: Callable[[], float] = time.time,
    ):
        self.source = source
        self.cache = cache
        self.prefix = prefix
        self.fresh_ttl = fresh_ttl
        self.stale_ttl = stale_ttl
        self.mode = mode
        self.clock = clock

    async def get(self, product_id: int) -> Product:
        # TODO B1/B2: cache-aside with bounded TTL, handling corrupt entries.
        # TODO B4: Redis failure must not make the source unavailable.
        # TODO P: per-key singleflight. TODO E: stale-while-revalidate.
        return await self.source.get(product_id)

    async def update(self, product_id: int, patch: ProductPatch) -> Product:
        # TODO B3/B5: invalidate after the successful source update.
        # TODO E: coordinate invalidation with an in-flight refresh.
        return await self.source.update(product_id, patch)

    async def wait_idle(self) -> None:
        """E: await refreshes already scheduled by get; used by deterministic tests."""

    async def aclose(self) -> None:
        """E: stop/await background tasks. Redis client belongs to the caller."""
