import pytest

from app.models import ProductPatch

pytestmark = pytest.mark.basic


async def test_b1_repeated_read_uses_redis_with_finite_ttl(factory):
    lab = factory()
    first = await lab.service.get(1)
    second = await lab.service.get(1)
    assert first == second
    assert lab.source.reads == 1, "A fresh hit must avoid a source read"
    ttl = await lab.redis.pttl(f"{lab.prefix}:product:1")
    assert 0 < ttl <= 5000, "Entry must exist in Redis with a finite TTL"


async def test_b2_expired_value_is_reloaded(factory):
    lab = factory(fresh_ttl=1)
    await lab.service.get(1)
    await lab.source.update(1, ProductPatch(stock=3))
    lab.clock.advance(1.01)
    result = await lab.service.get(1)
    assert result.stock == 3
    assert lab.source.reads == 2
    await lab.service.get(1)
    assert lab.source.reads == 2, "Reloaded value should be cached again"


async def test_b3_update_invalidates_only_its_product(factory):
    lab = factory()
    await lab.service.get(1)
    await lab.service.get(2)
    result = await lab.service.update(1, ProductPatch(price_kopecks=55000, stock=0))
    assert result.revision == 2
    assert (await lab.service.get(1)).price_kopecks == 55000
    assert (await lab.service.get(1)).stock == 0
    assert (await lab.service.get(2)).revision == 1
    assert lab.source.reads == 3, "Other products must remain cached"


@pytest.mark.parametrize("command", ["get", "set", "delete"])
async def test_b4_redis_errors_do_not_break_source_operations(factory, command):
    lab = factory()
    if command == "delete":
        await lab.service.get(1)
        lab.cache.fail = {command}
        updated = await lab.service.update(1, ProductPatch(stock=5))
        assert updated.stock == 5
        assert (await lab.service.get(1)).stock == 5
    else:
        lab.cache.fail = {command}
        assert (await lab.service.get(1)).id == 1


async def test_b5_recovery_does_not_resurrect_old_price(factory):
    lab = factory()
    await lab.service.get(1)
    lab.cache.fail = {"get", "set", "delete"}
    await lab.service.update(1, ProductPatch(price_kopecks=60000))
    lab.cache.fail.clear()
    assert (await lab.service.get(1)).price_kopecks == 60000


@pytest.mark.parametrize("bad", ["not-json", "null", "{}", "[]"])
async def test_b6_corrupt_entry_behaves_like_a_miss(factory, bad):
    lab = factory()
    await lab.redis.set(f"{lab.prefix}:product:1", bad, ex=5)
    assert (await lab.service.get(1)).id == 1
    await lab.service.get(1)
    assert lab.source.reads == 1, "Corrupt entry must be replaced by valid data"


async def test_b7_source_errors_are_not_hidden(factory):
    lab = factory()
    lab.source.fail = True
    with pytest.raises(RuntimeError, match="source failure"):
        await lab.service.get(1)
