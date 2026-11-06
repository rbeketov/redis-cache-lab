import asyncio
import logging
import math
import time
from collections.abc import Callable

from pydantic import BaseModel, FiniteFloat, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.models import Product, ProductPatch
from app.source import CatalogSource


logger = logging.getLogger(__name__)


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
        self._dirty: set[int] = set()
        self._locks: dict[int, asyncio.Lock] = {}
        self._refreshes: dict[int, asyncio.Task[None]] = {}
        self._closed = False

    async def get(self, product_id: int) -> Product:
        entry = await self._read(product_id)
        if entry is not None:
            now = self.clock()
            if now < entry.fresh_until:
                return entry.product
            if self.mode == "swr" and now < entry.expires_at:
                if not self._closed and product_id not in self._refreshes:
                    self._refreshes[product_id] = asyncio.create_task(
                        self._refresh(product_id), name=f"refresh-product-{product_id}",
                    )
                return entry.product
        async with self._lock(product_id):
            entry = await self._read(product_id)
            if entry is not None and self.clock() < entry.fresh_until:
                return entry.product
            return await self._load(product_id)

    async def update(self, product_id: int, patch: ProductPatch) -> Product:
        async with self._lock(product_id):
            product = await self.source.update(product_id, patch)
            self._dirty.add(product_id)
            await self._invalidate(product_id)
            return product

    async def wait_idle(self) -> None:
        tasks = list(self._refreshes.values())
        if tasks:
            await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))

    async def aclose(self) -> None:
        self._closed = True
        tasks = list(self._refreshes.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._refreshes.clear()

    def _key(self, product_id: int) -> str:
        return f"{self.prefix}:product:{product_id}"

    def _lock(self, product_id: int) -> asyncio.Lock:
        if product_id not in self._locks:
            self._locks[product_id] = asyncio.Lock()
        return self._locks[product_id]

    async def _read(self, product_id: int) -> "_CacheEntry | None":
        if product_id in self._dirty:
            return None
        try:
            raw = await self.cache.get(self._key(product_id))
        except RedisError:
            logger.warning("Redis GET failed for product %s", product_id, exc_info=True)
            return None
        if raw is None or product_id in self._dirty:
            return None
        try:
            entry = _CacheEntry.model_validate_json(raw, strict=True)
        except ValidationError:
            return None
        if entry.product.id != product_id or entry.expires_at < entry.fresh_until:
            return None
        return entry

    async def _load(self, product_id: int) -> Product:
        if product_id in self._dirty:
            await self._invalidate(product_id)
        product = await self.source.get(product_id)
        if product_id not in self._dirty:
            ttl = self.fresh_ttl + (self.stale_ttl if self.mode == "swr" else 0)
            now = self.clock()
            entry = _CacheEntry(
                product=product, fresh_until=now + self.fresh_ttl,
                expires_at=now + ttl,
            )
            try:
                await self.cache.set(
                    self._key(product_id), entry.model_dump_json(),
                    px=math.ceil(ttl * 1000),
                )
            except RedisError:
                logger.warning("Redis SET failed for product %s", product_id, exc_info=True)
        return product

    async def _invalidate(self, product_id: int) -> None:
        try:
            await self.cache.delete(self._key(product_id))
        except RedisError:
            logger.warning("Redis DEL failed for product %s", product_id, exc_info=True)
        else:
            self._dirty.discard(product_id)

    async def _refresh(self, product_id: int) -> None:
        try:
            async with self._lock(product_id):
                entry = await self._read(product_id)
                if entry is None or self.clock() >= entry.fresh_until:
                    await self._load(product_id)
        except Exception:
            logger.exception("Background refresh failed for product %s", product_id)
        finally:
            self._refreshes.pop(product_id, None)


class _CacheEntry(BaseModel):
    product: Product
    fresh_until: FiniteFloat
    expires_at: FiniteFloat
