"""Redis cache-aside and stale-while-revalidate for a single event loop."""
import asyncio
import logging
import math
import time
from collections.abc import Callable
from weakref import WeakValueDictionary

from pydantic import BaseModel, ConfigDict, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.models import Product, ProductPatch
from app.source import CatalogSource

logger = logging.getLogger(__name__)


class _CacheEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    product: Product
    fresh_until: float
    expires_at: float


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
        # Active holders/waiters keep locks alive; arbitrary missing IDs do not.
        self._locks: WeakValueDictionary[int, asyncio.Lock] = WeakValueDictionary()
        self._dirty: set[int] = set()
        self._refreshes: dict[int, asyncio.Task[None]] = {}
        self._closed = False

    def _key(self, product_id: int) -> str:
        return f"{self.prefix}:product:{product_id}"

    def _lock(self, product_id: int) -> asyncio.Lock:
        lock = self._locks.get(product_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[product_id] = lock
        return lock

    async def _read_entry(self, product_id: int) -> _CacheEntry | None:
        if product_id in self._dirty:
            return None
        try:
            raw = await self.cache.get(self._key(product_id))
        except RedisError:
            logger.warning("Redis GET failed for product %s", product_id, exc_info=True)
            return None
        # PATCH may have started while GET was awaiting Redis.
        if raw is None or product_id in self._dirty:
            return None
        try:
            entry = _CacheEntry.model_validate_json(raw, strict=True)
        except (ValidationError, ValueError, TypeError):
            return None
        if (entry.product.id != product_id
                or entry.expires_at < entry.fresh_until
                or self.clock() >= entry.expires_at):
            return None
        return entry

    async def _invalidate(self, product_id: int) -> None:
        """Called under the product lock; failures retain the distrust marker."""
        try:
            await self.cache.delete(self._key(product_id))
        except RedisError:
            logger.warning("Redis DEL failed for product %s", product_id, exc_info=True)
        else:
            self._dirty.discard(product_id)

    async def _load(self, product_id: int) -> Product:
        """Load and publish under the same lock used by PATCH and refresh."""
        if product_id in self._dirty:
            await self._invalidate(product_id)
        product = await self.source.get(product_id)
        if product_id in self._dirty:
            # Until DEL succeeds, the old Redis value must remain untrusted.
            return product
        lifetime = self.fresh_ttl + (self.stale_ttl if self.mode == "swr" else 0)
        now = self.clock()
        entry = _CacheEntry(product=product, fresh_until=now + self.fresh_ttl,
                            expires_at=now + lifetime)
        try:
            await self.cache.set(self._key(product_id), entry.model_dump_json(),
                                 px=max(1, math.ceil(lifetime * 1000)))
        except RedisError:
            logger.warning("Redis SET failed for product %s", product_id, exc_info=True)
        return product

    def _schedule_refresh(self, product_id: int) -> None:
        if self._closed or product_id in self._refreshes:
            return
        task = asyncio.create_task(self._refresh(product_id),
                                   name=f"refresh:{self._key(product_id)}")
        self._refreshes[product_id] = task
        task.add_done_callback(lambda done: self._refresh_done(product_id, done))

    def _refresh_done(self, product_id: int, task: asyncio.Task[None]) -> None:
        if self._refreshes.get(product_id) is task:
            del self._refreshes[product_id]
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error("Background refresh failed for product %s", product_id,
                             exc_info=(type(error), error, error.__traceback__))

    async def _refresh(self, product_id: int) -> None:
        async with self._lock(product_id):
            entry = await self._read_entry(product_id)
            if entry is not None and self.clock() < entry.fresh_until:
                return
            await self._load(product_id)

    async def get(self, product_id: int) -> Product:
        entry = await self._read_entry(product_id)
        now = self.clock()
        if entry is not None and now < entry.expires_at:
            if now < entry.fresh_until:
                return entry.product
            if self.mode == "swr":
                self._schedule_refresh(product_id)
                return entry.product
        async with self._lock(product_id):
            # Another request may have populated the key while we waited.
            entry = await self._read_entry(product_id)
            now = self.clock()
            if entry is not None and now < entry.expires_at:
                if now < entry.fresh_until:
                    return entry.product
                if self.mode == "swr":
                    self._schedule_refresh(product_id)
                    return entry.product
            return await self._load(product_id)

    async def update(self, product_id: int, patch: ProductPatch) -> Product:
        async with self._lock(product_id):
            # Mark before any await: cancellation after a source write or during
            # DEL must not let a subsequent reader trust the previous value.
            self._dirty.add(product_id)
            product = await self.source.update(product_id, patch)
            await self._invalidate(product_id)
            return product

    async def wait_idle(self) -> None:
        """Wait for the refreshes that have already been scheduled."""
        tasks = tuple(self._refreshes.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def aclose(self) -> None:
        """Cancel refreshes, including those blocked on the source or a lock."""
        self._closed = True
        tasks = tuple(self._refreshes.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
