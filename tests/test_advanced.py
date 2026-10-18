import asyncio

import pytest

pytestmark = pytest.mark.advanced


async def test_p1_twenty_concurrent_misses_load_once(factory):
    lab = factory()
    results = await asyncio.gather(*(lab.service.get(1) for _ in range(20)))
    assert all(product.id == 1 for product in results)
    assert lab.source.reads == 1, "Same-key misses should share one load"


async def test_p2_other_keys_are_not_blocked(factory):
    lab = factory()
    lab.source.block_id = 1
    blocked = asyncio.create_task(lab.service.get(1))
    try:
        await asyncio.wait_for(lab.source.started.wait(), 2)
        other = await asyncio.wait_for(lab.service.get(2), 1)
        assert other.id == 2
    finally:
        lab.source.release.set()
        await blocked


async def test_p3_cancelled_request_does_not_leave_a_lock(factory):
    lab = factory()
    lab.source.block_id = 1
    request = asyncio.create_task(lab.service.get(1))
    try:
        await asyncio.wait_for(lab.source.started.wait(), 2)
    finally:
        request.cancel()
        await asyncio.gather(request, return_exceptions=True)
        lab.source.release.set()
    assert (await asyncio.wait_for(lab.service.get(1), 1)).id == 1
