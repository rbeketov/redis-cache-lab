import asyncio

import pytest

from app.models import ProductPatch


async def test_fresh_swr_hit_does_not_schedule_refresh(factory):
    lab = factory(mode="swr", fresh_ttl=5, stale_ttl=10)
    original = await lab.service.get(1)
    await lab.source.update(1, ProductPatch(stock=6))
    lab.clock.advance(4)
    results = await asyncio.gather(*(lab.service.get(1) for _ in range(20)))
    await asyncio.wait_for(lab.service.wait_idle(), 1)
    assert all(product == original for product in results)
    assert lab.source.reads == 1, "Fresh hits must not start background loads"


async def test_failed_invalidation_preserves_other_product_cache(factory):
    lab = factory()
    await lab.service.get(1)
    other = await lab.service.get(2)
    lab.cache.fail = {"delete"}
    await lab.service.update(1, ProductPatch(price_kopecks=61000))
    assert await lab.service.get(2) == other
    assert lab.source.reads == 2, "A dirty key must not disable another key's cache"
    lab.cache.fail.clear()
    assert (await lab.service.get(1)).price_kopecks == 61000
    reads = lab.source.reads
    assert await lab.service.get(2) == other
    assert lab.source.reads == reads


async def test_fractional_fresh_boundary_starts_swr_refresh(factory):
    lab = factory(mode="swr", fresh_ttl=0.5, stale_ttl=5)
    original = await lab.service.get(1)
    ttl = await lab.redis.pttl(f"{lab.prefix}:product:1")
    assert 0 < ttl <= 5500
    await lab.source.update(1, ProductPatch(stock=6))
    lab.clock.advance(0.5)
    lab.source.block_id = 1
    try:
        result = await asyncio.wait_for(lab.service.get(1), 1)
        assert result == original
        await asyncio.wait_for(lab.source.started.wait(), 1)
        assert lab.source.reads == 2
    finally:
        lab.source.release.set()
    await asyncio.wait_for(lab.service.wait_idle(), 1)
    assert (await lab.service.get(1)).stock == 6


@pytest.mark.parametrize("mode,age", [("cache_aside", 0.5), ("swr", 5.5)])
async def test_exact_hard_expiry_waits_for_source(factory, mode, age):
    lab = factory(mode=mode, fresh_ttl=0.5, stale_ttl=5)
    await lab.service.get(1)
    await lab.source.update(1, ProductPatch(stock=6))
    lab.clock.advance(age)
    lab.source.block_id = 1
    request = asyncio.create_task(lab.service.get(1))
    try:
        await asyncio.wait_for(lab.source.started.wait(), 1)
        assert not request.done(), "An entry expires at equality, not afterwards"
    finally:
        lab.source.release.set()
        await asyncio.wait_for(request, 1)
    assert request.result().stock == 6
