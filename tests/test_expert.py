import asyncio

import pytest

from app.models import ProductPatch

pytestmark = pytest.mark.expert


async def test_e1_stale_returns_before_a_single_background_refresh(factory):
    lab = factory(mode="swr", fresh_ttl=1, stale_ttl=5)
    original = await lab.service.get(1)
    await lab.source.update(1, ProductPatch(stock=4))
    lab.clock.advance(1.1)
    lab.source.block_id = 1
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(lab.service.get(1) for _ in range(20))), 1)
        assert all(p.revision == original.revision for p in results)
        await asyncio.wait_for(lab.source.started.wait(), 1)
        assert lab.source.reads == 2, "Exactly one refresh should be in flight"
        second_wave = await asyncio.wait_for(
            asyncio.gather(*(lab.service.get(1) for _ in range(5))), 1)
        assert all(p.revision == original.revision for p in second_wave)
        assert lab.source.reads == 2, "Readers must not wait behind the refresh"
    finally:
        lab.source.release.set()
    await asyncio.wait_for(lab.service.wait_idle(), 2)
    assert (await lab.service.get(1)).stock == 4
    assert lab.source.reads == 2


async def test_e2_hard_expiry_waits_for_fresh_data(factory):
    lab = factory(mode="swr", fresh_ttl=1, stale_ttl=2)
    await lab.service.get(1)
    await lab.source.update(1, ProductPatch(stock=8))
    lab.clock.advance(3.1)
    lab.source.block_id = 1
    request = asyncio.create_task(lab.service.get(1))
    try:
        await asyncio.wait_for(lab.source.started.wait(), 1)
        assert not request.done(), "Hard-expired data must not be served"
    finally:
        lab.source.release.set()
    assert (await request).stock == 8


async def test_e3_update_cannot_be_overwritten_by_old_refresh(factory):
    lab = factory(mode="swr", fresh_ttl=1, stale_ttl=5)
    await lab.service.get(1)
    lab.clock.advance(1.1)
    lab.source.block_id = 1
    write = None
    try:
        assert (await asyncio.wait_for(lab.service.get(1), 1)).revision == 1
        await asyncio.wait_for(lab.source.started.wait(), 1)
        # A refresh already captured revision 1 and is still in flight.
        write = asyncio.create_task(lab.service.update(1, ProductPatch(stock=9)))
        await asyncio.sleep(0)
    finally:
        lab.source.release.set()
    assert (await asyncio.wait_for(write, 2)).revision == 2
    await asyncio.wait_for(lab.service.wait_idle(), 2)
    assert (await lab.service.get(1)).stock == 9


async def test_e4_background_failure_preserves_bounded_stale_value(factory, caplog):
    lab = factory(mode="swr", fresh_ttl=1, stale_ttl=2)
    await lab.service.get(1)
    lab.clock.advance(1.1)
    lab.source.fail = True
    assert (await lab.service.get(1)).revision == 1
    await asyncio.wait_for(lab.service.wait_idle(), 2)
    assert caplog.records, "Background failures must be observable"
    lab.clock.advance(2.1)
    with pytest.raises(RuntimeError, match="source failure"):
        await lab.service.get(1)


async def test_e5_shutdown_cleans_up_refresh_tasks(factory):
    lab = factory(mode="swr", fresh_ttl=1, stale_ttl=5)
    await lab.service.get(1)
    lab.clock.advance(1.1)
    lab.source.block_id = 1
    try:
        await asyncio.wait_for(lab.service.get(1), 1)
        await asyncio.wait_for(lab.source.started.wait(), 1)
        await asyncio.wait_for(lab.service.aclose(), 1)
        await asyncio.wait_for(lab.service.wait_idle(), 1)
    finally:
        lab.source.release.set()
