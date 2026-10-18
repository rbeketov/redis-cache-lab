from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from app.models import Product, ProductPatch
from app.service import ProductService
from app.settings import Settings
from app.source import CatalogSource


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        cache = Redis.from_url(
            config.redis_url, decode_responses=True,
            socket_connect_timeout=0.2, socket_timeout=0.2,
            retry=Retry(NoBackoff(), 0),
        )
        source = CatalogSource(config.source_delay)
        # A fresh namespace prevents old cache entries surviving the in-memory source.
        prefix = f"cache-lab:{uuid4().hex}"
        service = ProductService(
            source, cache, prefix=prefix, fresh_ttl=config.fresh_ttl,
            stale_ttl=config.stale_ttl, mode=config.mode,
        )
        app.state.source = source
        app.state.service = service
        app.state.prefix = prefix
        try:
            yield
        finally:
            await service.aclose()
            await cache.aclose()

    app = FastAPI(title="Redis cache lab", lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"status": "ok", "mode": config.mode}

    @app.get("/products/{product_id}", response_model=Product)
    async def get_product(product_id: int):
        try:
            return await app.state.service.get(product_id)
        except KeyError:
            raise HTTPException(404, "Product not found") from None

    @app.patch("/products/{product_id}", response_model=Product)
    async def patch_product(product_id: int, patch: ProductPatch):
        try:
            return await app.state.service.update(product_id, patch)
        except KeyError:
            raise HTTPException(404, "Product not found") from None

    @app.get("/debug/stats")
    async def stats():
        return {"source_reads": app.state.source.reads,
                "source_writes": app.state.source.writes,
                "cache_prefix": app.state.prefix}

    return app


app = create_app()
