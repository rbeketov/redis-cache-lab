"""Additional boundary, recovery and cancellation scenarios."""
import asyncio
import json

import pytest

from app.models import ProductPatch


async def test_cancelled_invalidation_keeps_old_entry_untrusted(factory, monkeypatch):
    lab = factory()
    await lab.service.get(1)
    key = f"{lab.prefix}:product:1"
    original = await lab.redis.get(key)
    started = asyncio.Event()
    delete = lab.cache.delete

    async def blocked_delete(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(lab.cache, "delete", blocked_delete)
    write = asyncio.create_task(lab.service.update(1, ProductPatch(price_kopecks=0)))
    try:
        await asyncio.wait_for(started.wait(), 1)
    finally:
        write.cancel()
        await asyncio.gather(write, return_exceptions=True)
    assert lab.source.products[1].price_kopecks == 0
    assert await lab.redis.get(key) == original
    monkeypatch.setattr(lab.cache, "delete", delete)
    assert (await asyncio.wait_for(lab.service.get(1), 1)).price_kopecks == 0
    assert (await lab.service.get(1)).revision == 2
    assert lab.source.reads == 2


async def test_failed_delete_preserves_other_key_and_recovers_after_failed_set(factory):
    lab = factory()
    await lab.service.get(1)
    await lab.service.get(2)
    lab.cache.fail = {"delete"}
    await lab.service.update(1, ProductPatch(stock=0))
    assert (await lab.service.get(1)).stock == 0
    assert (await lab.service.get(2)).revision == 1
    assert lab.source.reads == 3
    # DEL recovers before SET: removing the old value must still be safe.
    lab.cache.fail = {"set"}
    assert (await lab.service.get(1)).stock == 0
    assert await lab.redis.get(f"{lab.prefix}:product:1") is None
    lab.cache.fail.clear()
    assert (await lab.service.get(1)).stock == 0
    reads = lab.source.reads
    await lab.service.get(1)
    await lab.service.get(2)
    assert lab.source.reads == reads


async def test_foreground_fill_cannot_overwrite_a_completed_patch(factory):
    lab = factory()
    lab.source.block_id = 1
    read = asyncio.create_task(lab.service.get(1))
    write = None
    try:
        await asyncio.wait_for(lab.source.started.wait(), 1)
        write = asyncio.create_task(lab.service.update(1, ProductPatch(stock=7)))
        await asyncio.sleep(0)
    finally:
        lab.source.release.set()
        await asyncio.gather(read, *([write] if write else []))
    assert (await asyncio.wait_for(lab.service.get(1), 1)).stock == 7


@pytest.mark.parametrize("stale_ttl", [0, 0.5])
async def test_fractional_ttl_and_exact_expiry_boundary(factory, stale_ttl):
    lab = factory(mode="swr", fresh_ttl=0.25, stale_ttl=stale_ttl)
    await lab.service.get(1)
    ttl = await lab.redis.pttl(f"{lab.prefix}:product:1")
    assert 0 < ttl <= (0.25 + stale_ttl) * 1000
    await lab.source.update(1, ProductPatch(stock=2))
    lab.clock.advance(0.25)
    if stale_ttl:
        assert (await lab.service.get(1)).stock == 20
        await lab.service.wait_idle()
    assert (await lab.service.get(1)).stock == 2
    await lab.source.update(1, ProductPatch(stock=1))
    # Exactly at hard expiry, SWR must await the source, not return stock=2.
    lab.clock.advance(0.25 + stale_ttl)
    assert (await lab.service.get(1)).stock == 1


async def test_fresh_swr_reads_do_not_schedule_background_work(factory):
    lab = factory(mode="swr")
    first = await lab.service.get(1)
    lab.source.fail = True
    assert all(p == first for p in await asyncio.gather(
        *(lab.service.get(1) for _ in range(20))))
    await lab.service.wait_idle()
    assert lab.source.reads == 1


@pytest.mark.parametrize("corruption", ["wrong_id", "nan", "reversed_deadlines"])
async def test_structurally_valid_but_unusable_entry_is_replaced(factory, corruption):
    lab = factory()
    await lab.service.get(1)
    key = f"{lab.prefix}:product:1"
    entry = json.loads(await lab.redis.get(key))
    if corruption == "wrong_id":
        entry["product"]["id"] = 2
    elif corruption == "nan":
        entry["expires_at"] = float("nan")
    else:
        entry["expires_at"] = entry["fresh_until"] - 1
    await lab.redis.set(key, json.dumps(entry), ex=5)
    assert (await lab.service.get(1)).id == 1
    await lab.service.get(1)
    assert lab.source.reads == 2
