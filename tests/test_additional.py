"""Additional public-behavior checks for expiry, recovery and write races."""

import asyncio

import pytest

from app.models import ProductPatch


pytestmark = pytest.mark.expert


async def test_cache_aside_reloads_at_exact_fractional_expiry(factory):
    lab = factory(fresh_ttl=1.25)
    await lab.service.get(1)
    ttl = await lab.redis.pttl(f"{lab.prefix}:product:1")
    assert 0 < ttl <= 1250
    await lab.source.update(1, ProductPatch(stock=3))

    lab.clock.advance(1.25)
    assert (await lab.service.get(1)).stock == 3
    assert lab.source.reads == 2, "Equality is expiry, not a fresh hit"
    await lab.service.get(1)
    assert lab.source.reads == 2


async def test_fresh_swr_value_does_not_start_background_refresh(factory):
    lab = factory(mode="swr", fresh_ttl=2, stale_ttl=5)
    original = await lab.service.get(1)
    await lab.source.update(1, ProductPatch(stock=4))
    lab.clock.advance(1)

    assert await lab.service.get(1) == original
    await asyncio.wait_for(lab.service.wait_idle(), 2)
    assert lab.source.reads == 1, "Fresh values must not trigger a refresh"


async def test_swr_exact_fresh_boundary_starts_nonblocking_refresh(factory):
    lab = factory(mode="swr", fresh_ttl=1.25, stale_ttl=2.5)
    original = await lab.service.get(1)
    await lab.source.update(1, ProductPatch(stock=6))
    lab.clock.advance(1.25)
    lab.source.block_id = 1

    try:
        assert await asyncio.wait_for(lab.service.get(1), 1) == original
        await asyncio.wait_for(lab.source.started.wait(), 1)
        assert lab.source.reads == 2
    finally:
        lab.source.release.set()

    await asyncio.wait_for(lab.service.wait_idle(), 2)
    assert (await lab.service.get(1)).stock == 6
    assert lab.source.reads == 2


async def test_swr_exact_hard_boundary_waits_for_source(factory):
    lab = factory(mode="swr", fresh_ttl=1.25, stale_ttl=2.5)
    await lab.service.get(1)
    await lab.source.update(1, ProductPatch(stock=7))
    lab.clock.advance(3.75)
    lab.source.block_id = 1
    request = asyncio.create_task(lab.service.get(1))

    try:
        await asyncio.wait_for(lab.source.started.wait(), 1)
        assert not request.done(), "Equality with hard expiry forbids stale data"
    finally:
        lab.source.release.set()
        if not request.done():
            await asyncio.wait_for(request, 2)

    assert (await request).stock == 7


async def test_failed_invalidation_preserves_other_products_cache(factory):
    lab = factory()
    await lab.service.get(1)
    other = await lab.service.get(2)
    lab.cache.fail = {"delete"}

    updated = await lab.service.update(1, ProductPatch(price_kopecks=0, stock=0))
    assert (await lab.service.get(1)) == updated
    before = lab.source.reads
    assert await lab.service.get(2) == other
    assert lab.source.reads == before, "A dirty product must not disable another key"

    lab.cache.fail.clear()
    assert (await lab.service.get(1)) == updated
    before = lab.source.reads
    assert await lab.service.get(2) == other
    assert lab.source.reads == before


async def test_cancelled_patch_during_delete_never_resurrects_old_value(
    factory, monkeypatch,
):
    lab = factory()
    await lab.service.get(1)
    entered = asyncio.Event()
    release = asyncio.Event()
    original_delete = lab.cache.delete

    async def delayed_delete(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original_delete(*args, **kwargs)

    monkeypatch.setattr(lab.cache, "delete", delayed_delete)
    request = asyncio.create_task(
        lab.service.update(1, ProductPatch(stock=9)))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
    finally:
        release.set()
        monkeypatch.setattr(lab.cache, "delete", original_delete)
        if not request.done():
            request.cancel()
        await asyncio.gather(request, return_exceptions=True)

    result = await asyncio.wait_for(lab.service.get(1), 2)
    assert result.stock == 9
    assert result.revision == 2
    assert (await lab.service.get(1)) == result


async def test_failed_background_refresh_can_recover_on_next_stale_read(
    factory, caplog,
):
    lab = factory(mode="swr", fresh_ttl=1, stale_ttl=5)
    original = await lab.service.get(1)
    lab.clock.advance(1)
    lab.source.fail = True

    assert await lab.service.get(1) == original
    await asyncio.wait_for(lab.service.wait_idle(), 2)
    assert caplog.records
    assert lab.source.reads == 2

    lab.source.fail = False
    await lab.source.update(1, ProductPatch(stock=6))
    assert await lab.service.get(1) == original
    await asyncio.wait_for(lab.service.wait_idle(), 2)
    assert (await lab.service.get(1)).stock == 6
    assert lab.source.reads == 3, "A failed task must not prevent a later refresh"


@pytest.mark.parametrize("mode", ["cache_aside", "swr"])
async def test_cold_load_racing_with_patch_cannot_leave_an_old_cache(factory, mode):
    lab = factory(mode=mode)
    lab.source.block_id = 1
    reader = asyncio.create_task(lab.service.get(1))
    writer = None

    try:
        await asyncio.wait_for(lab.source.started.wait(), 1)
        writer = asyncio.create_task(
            lab.service.update(1, ProductPatch(stock=0)))
        await asyncio.sleep(0)
        lab.source.release.set()
        updated = await asyncio.wait_for(writer, 2)
        await asyncio.wait_for(reader, 2)
        # The overlapping read may observe the old snapshot; later reads may not.
        assert await lab.service.get(1) == updated
        assert updated.stock == 0
        assert updated.revision == 2
    finally:
        lab.source.release.set()
        tasks = [task for task in (reader, writer) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
