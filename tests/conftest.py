import asyncio
import os
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from app.service import ProductService
from app.source import CatalogSource


class ManualClock:
    def __init__(self):
        self.now = time.time()

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class ControlledSource(CatalogSource):
    def __init__(self):
        super().__init__(delay=0.02)
        self.block_id = None
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.fail = False

    async def get(self, product_id):
        snapshot = await super().get(product_id)
        if self.block_id == product_id:
            self.started.set()
            await self.release.wait()
        if self.fail:
            raise RuntimeError("Injected source failure")
        return snapshot


class FaultyRedis:
    """Real Redis underneath; deterministic network errors on selected commands."""
    def __init__(self, client):
        self.client = client
        self.fail = set()

    def __getattr__(self, name):
        return getattr(self.client, name)

    async def get(self, *args, **kwargs):
        if "get" in self.fail:
            raise ConnectionError("Injected Redis GET failure")
        return await self.client.get(*args, **kwargs)

    async def set(self, *args, **kwargs):
        if "set" in self.fail:
            raise ConnectionError("Injected Redis SET failure")
        return await self.client.set(*args, **kwargs)

    async def delete(self, *args, **kwargs):
        if "delete" in self.fail:
            raise ConnectionError("Injected Redis DEL failure")
        return await self.client.delete(*args, **kwargs)


@pytest.fixture
async def factory():
    client = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"),
                            decode_responses=True, socket_timeout=0.3)
    await client.ping()
    labs = []

    def make(*, mode="cache_aside", fresh_ttl=5, stale_ttl=10):
        source = ControlledSource()
        clock = ManualClock()
        cache = FaultyRedis(client)
        prefix = f"cache-lab:test:{uuid4().hex}"
        service = ProductService(source, cache, prefix=prefix, mode=mode,
                                 fresh_ttl=fresh_ttl, stale_ttl=stale_ttl, clock=clock)
        lab = SimpleNamespace(service=service, source=source, clock=clock,
                              cache=cache, redis=client, prefix=prefix)
        labs.append(lab)
        return lab

    try:
        yield make
    finally:
        for lab in labs:
            lab.source.release.set()
            await lab.service.aclose()
            keys = [key async for key in client.scan_iter(match=f"{lab.prefix}:*")]
            if keys:
                await client.delete(*keys)
        await client.aclose()
