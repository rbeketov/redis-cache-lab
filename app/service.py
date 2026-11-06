"""Cache-aside and stale-while-revalidate for a single-process catalogue.

Redis holds the data; local locks only coordinate loads and writes. A dirty
product cannot be read from Redis until invalidation or a fresh SET succeeds.
"""
import asyncio
import logging
import math
import time
from collections.abc import Callable
from weakref import WeakValueDictionary

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.models import Product, ProductPatch
from app.source import CatalogSource

logger = logging.getLogger(__name__)


class _Entry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    product: Product
    fresh_until: float = Field(strict=True, allow_inf_nan=False)
    expires_at: float = Field(strict=True, allow_inf_nan=False)


class ProductService:
    def __init__(
        self, source: CatalogSource, cache: Redis, *, prefix: str,
        fresh_ttl: float = 5, stale_ttl: float = 10,
        mode: str = "cache_aside", clock: Callable[[], float] = time.time,
    ):
        if mode not in {"cache_aside", "swr"}:
            raise ValueError("Unknown cache mode")
        if (not math.isfinite(fresh_ttl) or fresh_ttl <= 0
                or not math.isfinite(stale_ttl) or stale_ttl < 0
                or not math.isfinite(fresh_ttl + stale_ttl)):
            raise ValueError("TTLs must be finite, fresh > 0 and stale >= 0")
        self.source = source
        self.cache = cache
        self.prefix = prefix
        self.fresh_ttl = fresh_ttl
        self.stale_ttl = stale_ttl
        self.mode = mode
        self.clock = clock
        # Context managers/waiters keep active locks alive. Idle IDs need no lock.
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

    async def _read_entry(self, product_id: int) -> _Entry | None:
        if product_id in self._dirty:
            return None
        try:
            raw = await self.cache.get(self._key(product_id))
        except RedisError:
            logger.warning("Redis GET failed for product %s; using source", product_id)
            return None
        # A PATCH may have invalidated trust while GET was awaiting the network.
        if raw is None or product_id in self._dirty:
            return None
        try:
            entry = _Entry.model_validate_json(raw)
        except ValidationError:
            return None
        if (entry.product.id != product_id
                or entry.fresh_until > entry.expires_at
                or self.clock() >= entry.expires_at):
            return None
        return entry

    def _cached_product(self, product_id: int, entry: _Entry | None) -> Product | None:
        if entry is None:
            return None
        now = self.clock()
        if now >= entry.expires_at:
            return None
        if now < entry.fresh_until:
            return entry.product
        if self.mode == "swr":
            self._schedule_refresh(product_id)
            return entry.product
        return None

    async def _invalidate(self, product_id: int) -> None:
        # Set before any await: cancellation during DEL must also preserve distrust.
        self._dirty.add(product_id)
        try:
            await self.cache.delete(self._key(product_id))
        except RedisError:
            logger.warning("Redis DEL failed for product %s; key remains dirty", product_id)
        else:
            self._dirty.discard(product_id)

    async def _load(self, product_id: int) -> Product:
        # Caller holds this product's lock through the read and the cache write.
        # Errors from the authoritative source deliberately propagate.
        product = await self.source.get(product_id)
        now = self.clock()
        lifetime = self.fresh_ttl + (self.stale_ttl if self.mode == "swr" else 0)
        entry = _Entry(product=product, fresh_until=now + self.fresh_ttl,
                       expires_at=now + lifetime)
        try:
            await self.cache.set(
                self._key(product_id), entry.model_dump_json(),
                px=max(1, math.ceil(lifetime * 1000)),
            )
        except RedisError:
            logger.warning("Redis SET failed for product %s; returning source", product_id)
        else:
            # Replacing an untrusted entry with a current snapshot also repairs it.
            self._dirty.discard(product_id)
        return product

    async def get(self, product_id: int) -> Product:
        if self._closed:
            raise RuntimeError("ProductService is closed")
        # Fresh/stale hits never queue behind a background refresh.
        hit = self._cached_product(product_id, await self._read_entry(product_id))
        if hit is not None:
            return hit
        async with self._lock(product_id):
            if product_id in self._dirty:
                await self._invalidate(product_id)
            # Another reader may have populated Redis while we waited for the lock.
            hit = self._cached_product(product_id, await self._read_entry(product_id))
            if hit is not None:
                return hit
            return await self._load(product_id)

    async def update(self, product_id: int, patch: ProductPatch) -> Product:
        if self._closed:
            raise RuntimeError("ProductService is closed")
        # Serializing the entire fill/refresh with PATCH prevents stale-fill races.
        async with self._lock(product_id):
            product = await self.source.update(product_id, patch)
            await self._invalidate(product_id)
            return product

    def _schedule_refresh(self, product_id: int) -> None:
        if self._closed:
            return
        current = self._refreshes.get(product_id)
        if current is not None and not current.done():
            return
        task = asyncio.create_task(
            self._refresh(product_id), name=f"cache-refresh-{product_id}",
        )
        self._refreshes[product_id] = task
        task.add_done_callback(lambda done: self._forget_refresh(product_id, done))

    def _forget_refresh(self, product_id: int, task: asyncio.Task[None]) -> None:
        # An older task's callback must not remove its already scheduled successor.
        if self._refreshes.get(product_id) is task:
            del self._refreshes[product_id]

    async def _refresh(self, product_id: int) -> None:
        try:
            async with self._lock(product_id):
                if self._closed:
                    return
                if product_id in self._dirty:
                    await self._invalidate(product_id)
                entry = await self._read_entry(product_id)
                if entry is not None and self.clock() < entry.fresh_until:
                    return
                await self._load(product_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            # A background task has no request to receive its exception. Log it;
            # do not rewrite the old entry or extend its original hard deadline.
            logger.exception("Background refresh failed for product %s", product_id)

    async def wait_idle(self) -> None:
        """Wait for the refreshes already scheduled when this call begins."""
        tasks = tuple(self._refreshes.values())
        if tasks:
            await asyncio.gather(
                *(asyncio.shield(task) for task in tasks), return_exceptions=True,
            )

    async def aclose(self) -> None:
        """Stop refreshes, including those blocked in the source; caller owns Redis."""
        self._closed = True
        tasks = tuple(self._refreshes.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for product_id, task in tuple(self._refreshes.items()):
            self._forget_refresh(product_id, task)
